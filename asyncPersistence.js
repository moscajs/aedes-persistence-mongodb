'use strict'

const regEscape = require('escape-string-regexp')
const CachedPersistence = require('aedes-cached-persistence')
const Packet = CachedPersistence.Packet
const { MongoClient } = require('mongodb')
const { Qlobber } = require('qlobber')
const qlobberOpts = {
  separator: '/',
  wildcard_one: '+',
  wildcard_some: '#',
  match_empty_levels: true
}

class AsyncMongoPersistence {
  constructor (opts = {}) {
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

    this._opts = opts
    this._db = null
    this._cl = null
    this.retainedBulkQueue = [] // used for storing retained packets with ordered bulks
    this.executing = false // used as lock while a bulk is executing
  }

  // setup is called by "set broker" in aedes-abstract-persistence
  async setup () {
    // database already connected
    if (this._db) {
      return
    }

    // database already provided in the options
    if (this._opts.db) {
      this._db = this._opts.db
    } else {
      // connect to the database
      const conn = this._opts.url || 'mongodb://127.0.0.1/aedes'
      const options = this._opts.mongoOptions

      const mongoDBclient = new MongoClient(conn, options)
      this._mongoDBclient = mongoDBclient
      const urlParsed = URL.parse(this._opts.url)
      // skip the first / of the pathname if it exists
      const pathname = urlParsed.pathname ? urlParsed.pathname.substring(1) : undefined
      const databaseName = this._opts.database || pathname
      this._db = mongoDBclient.db(databaseName)
    }
    const db = this._db
    const subscriptions = db.collection('subscriptions')
    const retained = db.collection('retained')
    const will = db.collection('will')
    const outgoing = db.collection('outgoing')
    const incoming = db.collection('incoming')
    this._cl = {
      subscriptions,
      retained,
      will,
      outgoing,
      incoming
    }

    // drop existing TTL indexes (if exist)
    if (this._opts.dropExistingIndexes) {
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
      await this._cl[idx.collection].createIndex(idx.key, indexOpts)
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

    if (this._opts.ttl.subscriptions >= 0) {
      indexes.push({
        collection: 'subscriptions',
        key: this._opts.ttlAfterDisconnected ? 'disconnected' : 'added',
        name: 'ttl',
        expireAfterSeconds: this._opts.ttl.subscriptions
      })
    }

    if (this._opts.ttl.packets) {
      if (this._opts.ttl.packets.retained >= 0) {
        indexes.push({
          collection: 'retained',
          key: 'added',
          name: 'ttl',
          expireAfterSeconds: this._opts.ttl.packets.retained
        })
      }

      if (this._opts.ttl.packets.will >= 0) {
        indexes.push({
          collection: 'will',
          key: 'packet.added',
          name: 'ttl',
          expireAfterSeconds: this._opts.ttl.packets.will
        })
      }

      if (this._opts.ttl.packets.outgoing >= 0) {
        indexes.push({
          collection: 'outgoing',
          key: 'packet.added',
          name: 'ttl',
          expireAfterSeconds: this._opts.ttl.packets.outgoing
        })
      }

      if (this._opts.ttl.packets.incoming >= 0) {
        indexes.push({
          collection: 'incoming',
          key: 'packet.added',
          name: 'ttl',
          expireAfterSeconds: this._opts.ttl.packets.incoming
        })
      }
    }
    // create all indexes in parallel
    await Promise.all(indexes.map(createIndex))

    if (this._opts.ttlAfterDisconnected) {
      // To avoid stale subscriptions that might be left behind by broker shutting
      // down while clients were connected, set all to disconnected on startup.
      await this._cl.subscriptions.updateMany({ disconnected: { $exists: false } }, { $currentDate: { disconnected: true } })

      // Handlers for setting and clearing the disconnected timestamp on subscriptions
      this.broker.on('clientReady', (client) => {
        this._cl.subscriptions.updateMany({ clientId: client.id }, { $unset: { disconnected: true } })
      })
      this.broker.on('clientDisconnect', (client) => {
        this._cl.subscriptions.updateMany({ clientId: client.id }, { $currentDate: { disconnected: true } })
      })
    }

    // add subscriptions to Trie
    for await (const subscription of subscriptions.find({
      qos: { $gte: 0 }
    })) {
      this._trie.add(subscription.topic, subscription)
    }
    // setup is done
  }

  async storeRetained (packet) {
    await this.setup()
    const { promise, resolve } = promiseWithResolvers()
    const queue = this.retainedBulkQueue
    const filter = { topic: packet.topic }
    const setTTL = this._opts.ttl.packets

    if (packet.payload.length > 0) {
      queue.push({
        operation: {
          updateOne: {
            filter,
            update: { $set: decoratePacket(packet, setTTL) },
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
    processRetainedBulk(this)
    return promise
  }

  createRetainedStream (pattern) {
    return this.createRetainedStreamCombi([pattern])
  }

  async * createRetainedStreamCombi (patterns) {
    const regexes = []
    const matcher = new Qlobber(qlobberOpts)

    for (let i = 0; i < patterns.length; i++) {
      matcher.add(patterns[i], true)
      regexes.push(regEscape(patterns[i]).replace(/(\/*#|\\\+).*$/, ''))
    }

    const topic = new RegExp(regexes.join('|'))
    const filter = { topic }
    const exclude = { _id: 0 } // exclude the _id field
    for await (const result of this._cl.retained.find(filter).project(exclude)) {
      const packet = asPacket(result)
      if (matcher.match(packet.topic).length > 0) {
        yield packet
      }
    }
  }

  async addSubscriptions (client, subs) {
    const operations = []
    for (const sub of subs) {
      const subscription = Object.assign({}, sub)
      subscription.clientId = client.id
      operations.push({
        updateOne: {
          filter: {
            clientId: client.id,
            topic: sub.topic
          },
          update: {
            $set: decorateSubscription(subscription, this._opts)
          },
          upsert: true
        }
      })
    }

    await this._cl.subscriptions.bulkWrite(operations)
  }

  async removeSubscriptions (client, subs) {
    const operations = []

    for (const topic of subs) {
      operations.push({
        deleteOne: {
          filter: {
            clientId: client.id,
            topic
          }
        }
      })
    }
    await this._cl.subscriptions.bulkWrite(operations)
  }

  async subscriptionsByClient (client) {
    const filter = { clientId: client.id }
    const exclude = { clientId: false, _id: false } // exclude these fields
    const subs = await this._cl.subscriptions.find(filter).project(exclude).toArray()
    return subs
  }

  async countOffline () {
    const subsCount = this._trie.subscriptionsCount
    const result = await this._cl.subscriptions.aggregate([
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
    if (this._opts.db) {
      return
    }
    await this._mongoDBclient.close()
  }

  async outgoingEnqueue (sub, packet) {
    return await this.outgoingEnqueueCombi([sub], packet)
  }

  async outgoingEnqueueCombi (subs, packet) {
    if (subs?.length === 0) {
      return packet
    }

    const packets = []
    const newPacket = new Packet(packet)
    const setTTL = this._opts.ttl.packets

    for (const sub of subs) {
      packets.push({
        clientId: sub.clientId,
        packet: decoratePacket(newPacket, setTTL)
      })
    }

    await this._cl.outgoing.insertMany(packets)
  }

  async * outgoingStream (client) {
    for await (const result of this._cl.outgoing.find({ clientId: client.id })) {
      yield asPacket(result)
    }
  }

  async outgoingUpdate (client, packet) {
    if (packet.brokerId) {
      await updateWithMessageId(this._cl, client, packet)
    } else {
      await updatePacket(this._cl, client, packet)
    }
  }

  async outgoingClearMessageId (client, packet) {
    const outgoing = this._cl.outgoing

    const result = await outgoing.findOneAndDelete({
      clientId: client.id,
      'packet.messageId': packet.messageId
    })
    if (!result) {
      return null // packet not found
    }
    return asPacket(result)
  }

  async incomingStorePacket (client, packet) {
    const newPacket = new Packet(packet)
    newPacket.messageId = packet.messageId
    const setTTL = this._opts.ttl.packets

    await this._cl.incoming.insertOne({
      clientId: client.id,
      packet: decoratePacket(newPacket, setTTL)
    })
  }

  async incomingGetPacket (client, packet) {
    const result = await this._cl.incoming.findOne({
      clientId: client.id,
      'packet.messageId': packet.messageId
    })

    if (!result) {
      throw new Error(`packet not found for: ${client}`)
    }

    return asPacket(result)
  }

  async incomingDelPacket (client, packet) {
    await this._cl.incoming.deleteOne({
      clientId: client.id,
      'packet.messageId': packet.messageId
    })
  }

  async putWill (client, packet) {
    const setTTL = this._opts.ttl.packets
    packet.clientId = client.id
    packet.brokerId = this.broker.id
    await this._cl.will.insertOne({
      clientId: client.id,
      packet: decoratePacket(packet, setTTL)
    })
  }

  async getWill (client) {
    const result = await this._cl.will.findOne({
      clientId: client.id
    })
    if (!result) {
      return null // packet not found
    }
    return asPacket(result)
  }

  async delWill (client) {
    const result = await this._cl.will.findOneAndDelete({
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
    for await (const will of this._cl.will.find(filter)) {
      yield asPacket(will)
    }
  }

  async * getClientList (topic) {
    const filter = {}
    if (topic) {
      filter.topic = topic
    }
    for await (const sub of this._cl.subscriptions.find(filter)) {
      yield sub.clientId
    }
  }
}

async function processRetainedBulk (ctx) {
  if (!ctx.executing && !ctx._destroyed && ctx.retainedBulkQueue.length > 0) {
    ctx.executing = true
    const operations = []
    const onEnd = []

    while (ctx.retainedBulkQueue.length) {
      const { operation, resolve } = ctx.retainedBulkQueue.shift()
      operations.push(operation)
      onEnd.push(resolve)
    }
    // execute operations and ignore the error
    await ctx._cl.retained.bulkWrite(operations).catch(() => {})
    // resolve all promises
    while (onEnd.length) onEnd.shift().call()
    // check if we have new packets in queue
    ctx.executing = false
    // do not await as we run this in background and ignore errors
    processRetainedBulk(ctx)
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
