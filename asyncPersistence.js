'use strict'

const regEscape = require('escape-string-regexp')
const Packet = require('aedes-persistence').Packet
const BroadcastPersistence = require('aedes-persistence/broadcastPersistence.js')
const { MongoClient } = require('mongodb')
const { Qlobber } = require('qlobber')
const QlobberSub = require('qlobber/aedes/qlobber-sub')
const QLOBBER_OPTIONS = {
  separator: '/',
  wildcard_one: '+',
  wildcard_some: '#',
  match_empty_levels: true
}

// Batching limits for retained message pattern queries
//
// MongoDB refuses to compile a regex whose pattern exceeds 16384 bytes:
//   "Regular expression is invalid: pattern string is longer than the limit
//    set by the application" (error 51091)
//
// - MAX_REGEX_SOURCE_LENGTH: budget for the pattern MongoDB actually compiles,
//   which is `RegExp.source` — NOT the raw subscription filter. V8 escapes every
//   `/` as `\/` in `source`, and MQTT filters are slash-delimited, so a deep
//   topic tree inflates ~10% on top of regEscape's own expansions (`-` becomes
//   `\x2d`, four bytes for one). Budgeting against raw lengths silently
//   overshoots the 16384 limit; always measure with {@link sourceLength}.
//
// - MAX_PATTERNS_PER_BATCH: caps `$in` size and regex branch count so a client
//   subscribing to thousands of filters issues bounded queries.
const MAX_PATTERNS_PER_BATCH = 200
const MAX_REGEX_SOURCE_LENGTH = 15000

class AsyncMongoPersistence {
  // private class members start with #
  #trie
  #destroyed
  #broker
  #opts
  #db
  #mongoDBclient
  #cl
  #broadcast
  #retainedBulkQueue
  #executing

  constructor (opts = {}) {
    this.#trie = new QlobberSub(QLOBBER_OPTIONS) // used to match packets
    opts.ttl = opts.ttl || {}

    if (typeof opts.ttl.packets === 'number') {
      const ttl = opts.ttl.packets
      opts.ttl.packets = {
        retained: ttl,
        will: ttl,
        outgoing: ttl,
        incoming: ttl
      }
    }

    this.#opts = opts
    this.#db = null
    this.#cl = null
    this.#destroyed = false
    this.#retainedBulkQueue = [] // used for storing retained packets with ordered bulks
    this.#executing = false // used as lock while a bulk is executing
  }

  #addTTLIndexes (indexes) {
    const addTTLIndex = (collection, key, expireAfterSeconds) => {
      if (expireAfterSeconds >= 0) {
        indexes.push({ collection, key, name: 'ttl', expireAfterSeconds })
      }
    }

    if (this.#opts.ttl.subscriptions >= 0) {
      addTTLIndex(
        'subscriptions',
        this.#opts.ttlAfterDisconnected ? 'disconnected' : 'added',
        this.#opts.ttl.subscriptions
      )
    }

    if (this.#opts.ttl.packets) {
      addTTLIndex('retained', 'added', this.#opts.ttl.packets.retained)
      addTTLIndex('will', 'packet.added', this.#opts.ttl.packets.will)
      addTTLIndex('outgoing', 'packet.added', this.#opts.ttl.packets.outgoing)
      addTTLIndex('incoming', 'packet.added', this.#opts.ttl.packets.incoming)
    }
  }

  // access #broker, only for testing
  get broker () {
    return this.#broker
  }

  // access #db, only for testing
  get _db () {
    return this.#db
  }

  // access #mongoDBclient, only for testing
  get _mongoDBclient () {
    return this.#mongoDBclient
  }

  // setup is called by aedes-persistence/callbackPersistence.js
  async setup (broker) {
    this.#broker = broker

    // database already connected
    if (this.#db) {
      return
    }

    // database already provided in the options
    if (this.#opts.db) {
      this.#db = this.#opts.db
    } else {
      // connect to the database
      const conn = this.#opts.url || 'mongodb://127.0.0.1/aedes'
      const options = this.#opts.mongoOptions

      const mongoDBclient = new MongoClient(conn, options)
      this.#mongoDBclient = mongoDBclient
      const urlParsed = URL.parse(this.#opts.url)
      // skip the first / of the pathname if it exists
      const pathname = urlParsed.pathname ? urlParsed.pathname.substring(1) : undefined
      const databaseName = this.#opts.database || pathname
      this.#db = mongoDBclient.db(databaseName)
    }

    const collectionPrefix = `${this.#opts.collectionPrefix || ''}`

    const db = this.#db
    const subscriptions = db.collection(`${collectionPrefix}subscriptions`)
    const retained = db.collection(`${collectionPrefix}retained`)
    const will = db.collection(`${collectionPrefix}will`)
    const outgoing = db.collection(`${collectionPrefix}outgoing`)
    const incoming = db.collection(`${collectionPrefix}incoming`)
    this.#cl = {
      subscriptions,
      retained,
      will,
      outgoing,
      incoming
    }

    // drop existing TTL indexes (if exist)
    if (this.#opts.dropExistingIndexes) {
      const collections = await db.collections()
      for (const collection of collections) {
        const exists = await collection.indexExists('ttl')
        if (exists) {
          await collection.dropIndex('ttl')
        }
      }
    }

    // create indexes
    const createIndex = async (idx) => {
      const indexOpts = { name: idx.name }
      if (typeof idx.expireAfterSeconds === 'number') {
        indexOpts.expireAfterSeconds = idx.expireAfterSeconds
      }
      if (idx.unique) {
        indexOpts.unique = true
      }
      await this.#cl[idx.collection].createIndex(idx.key, indexOpts)
    }

    const indexes = [
      {
        collection: 'outgoing',
        key: { clientId: 1, 'packet.brokerId': 1, 'packet.brokerCounter': 1 },
        name: 'query_clientId_brokerId'
      },
      {
        collection: 'outgoing',
        key: { clientId: 1, 'packet.messageId': 1 },
        name: 'query_clientId_messageId'
      },
      {
        collection: 'incoming',
        key: { clientId: 1, 'packet.brokerId': 1, 'packet.brokerCounter': 1 },
        name: 'query_clientId_brokerId'
      },
      {
        collection: 'incoming',
        key: { clientId: 1, 'packet.messageId': 1 },
        name: 'query_clientId_messageId'
      },
      {
        // storeRetained / createRetainedStreamCombi key off `topic`. Without
        // this index every operation falls back to COLLSCAN; under load
        // storeRetained's ordered bulkWrite serializes the whole publish
        // pipeline behind it, stalling broker.publish callbacks by tens of
        // seconds on deployments with more than a few thousand retained
        // topics (observed: ~60s uniform lag with 15k retained + 200 msg/s).
        // Unique matches the {topic}-filtered upsert semantics.
        collection: 'retained',
        key: { topic: 1 },
        name: 'query_topic',
        unique: true
      }
    ]

    // Add TTL indexes
    this.#addTTLIndexes(indexes)
    // create all indexes in parallel
    await Promise.all(indexes.map(createIndex))

    if (this.#opts.ttlAfterDisconnected) {
      // To avoid stale subscriptions that might be left behind by broker shutting
      // down while clients were connected, set all to disconnected on startup.
      await this.#cl.subscriptions.updateMany({ disconnected: { $exists: false } }, { $currentDate: { disconnected: true } })

      // Handlers for setting and clearing the disconnected timestamp on subscriptions
      this.#broker.on('clientReady', (client) => {
        this.#cl.subscriptions.updateMany({ clientId: client.id }, { $unset: { disconnected: true } })
      })
      this.#broker.on('clientDisconnect', (client) => {
        this.#cl.subscriptions.updateMany({ clientId: client.id }, { $currentDate: { disconnected: true } })
      })
    }

    // add subscriptions to Trie
    for await (const subscription of subscriptions.find({
      qos: { $gte: 0 }
    })) {
      this.#trie.add(subscription.topic, subscription)
    }
    // subscribe to the broker for subscription updates
    this.#broadcast = new BroadcastPersistence(broker, this.#trie)
    await this.#broadcast.brokerSubscribe()
    // setup is done
  }

  async processRetainedBulk () {
    if (!this.#executing && !this.#destroyed && this.#retainedBulkQueue.length > 0) {
      this.#executing = true
      const operations = []
      const onEnd = []

      while (this.#retainedBulkQueue.length) {
        const { operation, resolve } = this.#retainedBulkQueue.shift()
        operations.push(operation)
        onEnd.push(resolve)
      }
      // execute operations and ignore the error
      await this.#cl.retained.bulkWrite(operations).catch(() => { })
      // resolve all promises
      while (onEnd.length) onEnd.shift().call()
      // check if we have new packets in queue
      this.#executing = false
      // do not await as we run this in background and ignore errors
      this.processRetainedBulk()
    }
    if (this.#destroyed) {
      // cleanup dangling promises
      while (this.#retainedBulkQueue.length) {
        const { resolve } = this.#retainedBulkQueue.shift()
        resolve() // resolve all promises
      }
    }
  }

  async storeRetained (packet) {
    const { promise, resolve } = promiseWithResolvers()
    const queue = this.#retainedBulkQueue
    const filter = { topic: packet.topic }

    if (packet.payload.length > 0) {
      queue.push({
        operation: {
          updateOne: {
            filter,
            update: { $set: decoratePacket(packet, this.#opts.ttl.packets) },
            upsert: true
          }
        },
        resolve
      })
    } else {
      queue.push({
        operation: {
          deleteOne: {
            filter
          }
        },
        resolve
      })
    }
    this.processRetainedBulk()
    return promise
  }

  createRetainedStream (pattern) {
    return this.createRetainedStreamCombi([pattern])
  }

  async * createRetainedStreamCombi (patterns) {
    // Early return for empty patterns to avoid a filter that would match every
    // document in the collection
    if (patterns.length === 0) {
      return
    }

    const matcher = new Qlobber(QLOBBER_OPTIONS)

    for (let i = 0; i < patterns.length; i++) {
      matcher.add(patterns[i], true)
    }

    const filters = buildRetainedFilters(patterns)
    // A topic can match several filters; only dedupe when that's possible
    const seenTopics = filters.length > 1 ? new Set() : null

    for (const filter of filters) {
      for await (const packet of this.#queryRetained(filter, matcher)) {
        if (seenTopics) {
          if (seenTopics.has(packet.topic)) {
            continue
          }
          seenTopics.add(packet.topic)
        }
        yield packet
      }
    }
  }

  async * #queryRetained (filter, matcher) {
    const exclude = { _id: 0 }

    for await (const result of this.#cl.retained.find(filter).project(exclude)) {
      const packet = asPacket(result)
      if (matcher.match(packet.topic).length > 0) {
        yield packet
      }
    }
  }

  async addSubscriptions (client, subs) {
    const subscriptions = []
    const operations = subs.map(sub => {
      const subscription = { ...sub, clientId: client.id }
      subscriptions.push(subscription)
      return {
        updateOne: {
          filter: {
            clientId: client.id,
            topic: sub.topic
          },
          update: {
            $set: decorateSubscription(subscription, this.#opts)
          },
          upsert: true
        }
      }
    })

    await this.#cl.subscriptions.bulkWrite(operations)
    // inform the broker
    await this.#broadcast.addedSubscriptions(client, subs)
  }

  async removeSubscriptions (client, subs) {
    const operations = subs.map(topic => ({
      deleteOne: {
        filter: {
          clientId: client.id,
          topic
        }
      }
    }))
    await this.#cl.subscriptions.bulkWrite(operations)
    // inform the broker
    await this.#broadcast.removedSubscriptions(client, subs)
  }

  async subscriptionsByClient (client) {
    const filter = { clientId: client.id }
    const exclude = { clientId: false, _id: false } // exclude these fields
    const subs = await this.#cl.subscriptions.find(filter).project(exclude).toArray()
    return subs
  }

  async countOffline () {
    const subsCount = this.#trie.subscriptionsCount
    const result = await this.#cl.subscriptions.aggregate([
      {
        $group: {
          _id: '$clientId'
        }
      }, {
        $count: 'clientsCount'
      }]).toArray()
    const clientsCount = result[0]?.clientsCount || 0
    return { subsCount, clientsCount }
  }

  async destroy () {
    if (this.#destroyed) {
      throw new Error('destroyed called twice!')
    }
    this.#destroyed = true
    // stop listening to subscription updates
    await this.#broadcast.brokerUnsubscribe()

    if (this.#opts.db) {
      return
    }
    await this.#mongoDBclient.close()
  }

  async subscriptionsByTopic (topic) {
    return this.#trie.match(topic)
  }

  async cleanSubscriptions (client) {
    const subs = await this.subscriptionsByClient(client)
    if (subs.length > 0) {
      const remSubs = subs.map(sub => sub.topic)
      await this.removeSubscriptions(client, remSubs)
    }
  }

  async outgoingEnqueue (sub, packet) {
    return await this.outgoingEnqueueCombi([sub], packet)
  }

  async outgoingEnqueueCombi (subs, packet) {
    if (subs?.length === 0) {
      return packet
    }

    const newPacket = new Packet(packet)
    const decoratedPacket = decoratePacket(newPacket, this.#opts.ttl.packets)
    const packets = subs.map(sub => ({
      clientId: sub.clientId,
      packet: decoratedPacket
    }))

    await this.#cl.outgoing.insertMany(packets)
  }

  async * outgoingStream (client) {
    for await (const result of this.#cl.outgoing.find({ clientId: client.id })) {
      yield asPacket(result)
    }
  }

  async outgoingUpdate (client, packet) {
    if (packet.brokerId) {
      await updateWithMessageId(this.#cl, client, packet)
    } else {
      await updatePacket(this.#cl, client, packet)
    }
  }

  async outgoingClearMessageId (client, packet) {
    const result = await this.#cl.outgoing.findOneAndDelete({
      clientId: client.id,
      'packet.messageId': packet.messageId
    })
    return result ? asPacket(result) : null
  }

  async incomingStorePacket (client, packet) {
    const newPacket = new Packet(packet)
    newPacket.messageId = packet.messageId

    await this.#cl.incoming.insertOne({
      clientId: client.id,
      packet: decoratePacket(newPacket, this.#opts.ttl.packets)
    })
  }

  async incomingGetPacket (client, packet) {
    const result = await this.#cl.incoming.findOne({
      clientId: client.id,
      'packet.messageId': packet.messageId
    })

    if (!result) {
      throw new Error(`packet not found for: ${client}`)
    }

    return asPacket(result)
  }

  async incomingDelPacket (client, packet) {
    await this.#cl.incoming.deleteOne({
      clientId: client.id,
      'packet.messageId': packet.messageId
    })
  }

  async putWill (client, packet) {
    packet.clientId = client.id
    packet.brokerId = this.#broker.id
    await this.#cl.will.insertOne({
      clientId: client.id,
      packet: decoratePacket(packet, this.#opts.ttl.packets)
    })
  }

  async getWill (client) {
    const result = await this.#cl.will.findOne({
      clientId: client.id
    })
    if (!result) {
      return null // packet not found
    }
    return asPacket(result)
  }

  async delWill (client) {
    const result = await this.#cl.will.findOneAndDelete({
      clientId: client.id
    })
    if (!result) {
      return null // packet not found
    }
    return asPacket(result)
  }

  async * streamWill (brokers) {
    const filter = {}

    if (brokers) {
      filter['packet.brokerId'] = { $nin: Object.keys(brokers) }
    }
    for await (const will of this.#cl.will.find(filter)) {
      yield asPacket(will)
    }
  }

  async * getClientList (topic) {
    const filter = {}
    if (topic) {
      filter.topic = topic
    }
    for await (const sub of this.#cl.subscriptions.find(filter)) {
      yield sub.clientId
    }
  }
}

// The pattern MongoDB compiles is `RegExp.source`, which escapes characters the
// raw filter does not (notably `/` as `\/`). Measure what the server receives.
function sourceLength (pattern) {
  return new RegExp(pattern).source.length
}

// Literal prefix a topic must start with to have any chance of matching this
// filter. `#` also matches the parent level (`a/b` matches `a/b/#`), so trailing
// slashes are dropped. Returns '' when the filter can match any topic.
function literalPrefix (filter) {
  const wildcard = filter.search(/[#+]/)
  if (wildcard === -1) {
    return filter
  }
  const prefix = filter.slice(0, wildcard)
  return filter[wildcard] === '#' ? prefix.replace(/\/+$/, '') : prefix
}

// Anchored branch for one prefix, guaranteed to fit the budget on its own:
// escaping can quadruple a character (`-` becomes `\x2d`), so an oversized
// prefix is truncated. The regex only pre-filters — `matcher.match` decides the
// real matches — so a shorter prefix stays correct, it just yields more
// candidates to filter out.
function prefixBranch (prefix) {
  const maxRaw = Math.floor(MAX_REGEX_SOURCE_LENGTH / 4)
  return `^${regEscape(prefix.length > maxRaw ? prefix.slice(0, maxRaw) : prefix)}`
}

// Query filters covering `patterns`, each small enough for MongoDB to compile.
// Filters without a wildcard go through `$in`, wildcard filters become
// `^`-anchored prefixes: both keep the `topic` index scan bounded, where an
// unanchored alternation has to walk the whole index.
function buildRetainedFilters (patterns) {
  const exact = []
  const prefixes = []

  for (const pattern of patterns) {
    const prefix = literalPrefix(pattern)
    if (prefix === '') {
      return [{}] // matches every topic, narrowing it further buys nothing
    }
    if (prefix === pattern) {
      exact.push(pattern)
    } else {
      prefixes.push(prefix)
    }
  }

  const filters = []

  for (let i = 0; i < exact.length; i += MAX_PATTERNS_PER_BATCH) {
    filters.push({ topic: { $in: exact.slice(i, i + MAX_PATTERNS_PER_BATCH) } })
  }

  let branches = []
  let length = 0
  for (const prefix of prefixes) {
    const branch = prefixBranch(prefix)
    const cost = sourceLength(branch) + 1 // + the `|` that joins it to the batch
    if (branches.length >= MAX_PATTERNS_PER_BATCH ||
        (length + cost > MAX_REGEX_SOURCE_LENGTH && branches.length > 0)) {
      filters.push({ topic: new RegExp(branches.join('|')) })
      branches = []
      length = 0
    }
    branches.push(branch)
    length += cost
  }
  if (branches.length > 0) {
    filters.push({ topic: new RegExp(branches.join('|')) })
  }

  return filters
}

function decoratePacket (packet, setTTL) {
  if (setTTL) {
    packet.added = new Date()
  }
  return packet
}

function decorateSubscription (sub, opts) {
  if (opts.ttl.subscriptions) {
    sub.added = new Date()
  }
  return sub
}

function asPacket (obj) {
  const packet = obj?.packet || obj
  if (!packet) {
    throw new Error('Invalid packet')
  }
  if (Buffer.isBuffer(packet?.payload?.buffer)) {
    packet.payload = packet.payload.buffer
  }
  return packet
}

async function updateWithMessageId (db, client, packet) {
  await db.outgoing.updateOne({
    clientId: client.id,
    'packet.brokerCounter': packet.brokerCounter,
    'packet.brokerId': packet.brokerId
  }, {
    $set: {
      'packet.messageId': packet.messageId
    }
  })
}

async function updatePacket (db, client, packet) {
  await db.outgoing.updateOne({
    clientId: client.id,
    'packet.messageId': packet.messageId
  }, {
    $set: {
      clientId: client.id,
      packet
    }
  })
}

function promiseWithResolvers () {
  // this can be replaced by Promise.withResolvers()in NodeJS >= 22
  let res
  let rej
  const promise = new Promise((resolve, reject) => {
    res = resolve
    rej = reject
  })
  return { promise, resolve: res, reject: rej }
}

module.exports = AsyncMongoPersistence
