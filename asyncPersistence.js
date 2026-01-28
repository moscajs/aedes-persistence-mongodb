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
// MongoDB has a BSON document size limit of 16MB, but regex patterns can hit
// practical compilation/execution limits around 32KB depending on the driver.
// These conservative values prevent "regular expression is too large" errors
// while still allowing efficient batch processing of large pattern sets.
//
// Research notes:
// - MongoDB regex size is limited by BSON document size and regex compilation
// - Practical regex limit in MongoDB is typically around 32KB
// - After escaping, MQTT wildcards (#, +) are replaced, reducing final regex size
// - Joining patterns with '|' adds minimal overhead ((n-1) characters)
//
// Current values are conservative to ensure compatibility across different
// MongoDB versions and deployment configurations. They can be increased if needed:
// - MAX_PATTERNS_PER_BATCH could be 100-200 for most use cases
// - MAX_TOTAL_PATTERN_LENGTH could be 15000-20000 (still well under 32KB limit)
const MAX_PATTERNS_PER_BATCH = 50
const MAX_TOTAL_PATTERN_LENGTH = 5000

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
    const matcher = new Qlobber(QLOBBER_OPTIONS)

    for (let i = 0; i < patterns.length; i++) {
      matcher.add(patterns[i], true)
    }

    // Calculate total pattern length
    const totalLength = patterns.reduce((sum, p) => sum + p.length, 0)

    // Determine if we need to batch
    const needsBatching =
      patterns.length > MAX_PATTERNS_PER_BATCH ||
      totalLength > MAX_TOTAL_PATTERN_LENGTH

    if (needsBatching) {
      // Process patterns in batches to avoid creating regex that's too large
      // Use dynamic batching based on cumulative length
      const seenTopics = new Set() // Track yielded packets to avoid duplicates
      const batches = []
      let currentBatch = []
      let currentLength = 0

      for (const pattern of patterns) {
        const patternLength = pattern.length

        // Edge case: if a single pattern exceeds MAX_TOTAL_PATTERN_LENGTH,
        // it will be placed in its own batch. This is intentional behavior
        // to ensure the pattern is still processed (MongoDB will handle it
        // or fail with a clear error). Very long patterns (>32KB after escaping)
        // may still cause MongoDB "regular expression is too large" errors.

        // Start a new batch if adding this pattern would exceed limits
        if (currentBatch.length >= MAX_PATTERNS_PER_BATCH ||
            (currentLength + patternLength > MAX_TOTAL_PATTERN_LENGTH && currentBatch.length > 0)) {
          batches.push(currentBatch)
          currentBatch = []
          currentLength = 0
        }
        currentBatch.push(pattern)
        currentLength += patternLength
      }
      // Add the last batch if not empty
      if (currentBatch.length > 0) {
        batches.push(currentBatch)
      }

      for (const batch of batches) {
        for await (const packet of this.#queryRetainedByPatterns(batch, matcher)) {
          // Avoid duplicates across batches
          if (!seenTopics.has(packet.topic)) {
            seenTopics.add(packet.topic)
            yield packet
          }
        }
      }
    } else {
      // Original logic for small pattern sets
      for await (const packet of this.#queryRetainedByPatterns(patterns, matcher)) {
        yield packet
      }
    }
  }

  async * #queryRetainedByPatterns (patterns, matcher) {
    // Early return for empty patterns to avoid creating an empty regex
    // that would match all documents in the collection
    if (patterns.length === 0) {
      return
    }

    const regexes = patterns.map(pattern =>
      regEscape(pattern).replace(/(\/*#|\\\+).*$/, '')
    )

    const topic = new RegExp(regexes.join('|'))
    const filter = { topic }
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
