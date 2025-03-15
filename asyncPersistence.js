const escape = require('escape-string-regexp')
const CachedPersistence = require('aedes-cached-persistence')
const Packet = CachedPersistence.Packet
const { MongoClient, Binary } = require('mongodb')
const { Qlobber } = require('qlobber')
const qlobberOpts = {
  separator: '/',
  wildcard_one: '+',
  wildcard_some: '#',
  match_empty_levels: true
}

function toStream (op) {
  return op.stream ? op.stream() : op
}

class AsyncMongoPersistence {
  constructor (opts = {}) {
    opts.ttl = opts.ttl || {}

    if (typeof opts.ttl.packets === 'number') {
      opts.ttl.packets = {
        retained: opts.ttl.packets,
        will: opts.ttl.packets,
        outgoing: opts.ttl.packets,
        incoming: opts.ttl.packets
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
    if (this.ready) {
      // setup is already done
      return
    }
    // database already connected
    if (this._opts.db) {
      return
    }

    const conn = this._opts.url || 'mongodb://127.0.0.1/aedes'
    const options = this._opts.mongoOptions

    const mongoDBclient = new MongoClient(conn, options)
    this._mongoDBclient = mongoDBclient
    const urlParsed = URL.parse(this._opts.url)
    // skip the first / of the pathname if it exists
    const pathname = urlParsed.pathname ? urlParsed.pathname.substring(1) : undefined
    const databaseName = this._opts.database || pathname
    const db = mongoDBclient.db(databaseName)
    this._db = db
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
      for await (const collection of db.collections()) {
        const exists = collection.indexExists('ttl')
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
    const { promise, resolve, reject } = promiseWithResolve()
    const queue = this.retainedBulkQueue
    const filter = { topic: packet.topic }

    if (packet.payload.length > 0) {
      queue.push({
        operation: {
          updateOne: {
            filter,
            update: { $set: decoratePacket(packet, this._opts) },
            upsert: true
          }
        },
        resolve,
        reject
      })
    } else {
      queue.push({
        operation: {
          deleteOne: {
            filter
          }
        },
        resolve,
        reject
      })
    }
    processRetainedBulk(this)
    return promise
  }

  createRetainedStream (pattern) {
    return this.createRetainedStreamCombi([pattern])
  }

  createRetainedStreamCombi (patterns) {
    let regex = []

    const instance = through.obj(filterPattern)
    instance.matcher = new Qlobber(qlobberOpts)

    for (let i = 0; i < patterns.length; i++) {
      instance.matcher.add(patterns[i], true)
      regex.push(escape(patterns[i]).replace(/(\/*#|\\\+).*$/, ''))
    }

    regex = regex.join('|')

    return pump(
      toStream(this._cl.retained.find({
        topic: new RegExp(regex)
      })),
      instance
    )
  }

  addSubscriptions (client, subs, cb) {
    if (!this.ready) {
      this.once('ready', this.addSubscriptions.bind(this, client, subs, cb))
      return
    }

    let published = 0
    let errored = false
    const bulk = this._cl.subscriptions.initializeOrderedBulkOp()
    subs
      .forEach((sub) => {
        const subscription = Object.assign({}, sub)
        subscription.clientId = client.id
        bulk.find({
          clientId: client.id,
          topic: sub.topic
        }).upsert().updateOne({
          $set: decorateSubscription(subscription, this._opts)
        })
      })

    bulk.execute(finish)
    this._addedSubscriptions(client, subs, finish)

    function finish (err) {
      errored = err
      published++
      if (published === 2) {
        cb(errored, client)
      }
    }
  }

  removeSubscriptions (client, subs, cb) {
    if (!this.ready) {
      this.once('ready', this.removeSubscriptions.bind(this, client, subs, cb))
      return
    }

    let published = 0
    let errored = false
    const bulk = this._cl.subscriptions.initializeOrderedBulkOp()
    subs
      .forEach((topic) => {
        bulk.find({
          clientId: client.id,
          topic
        }).deleteOne()
      })

    bulk.execute(finish)
    this._removedSubscriptions(client, subs.map(toSub), finish)

    function finish (err) {
      if (err && !errored) {
        errored = true
        cb(err, client)
        return
      }
      published++
      if (published === 2 && !errored) {
        cb(null, client)
      }
    }
  }

  subscriptionsByClient (client, cb) {
    if (!this.ready) {
      this.once('ready', this.subscriptionsByClient.bind(this, client, cb))
      return
    }

    this._cl.subscriptions.find({ clientId: client.id }).toArray((err, subs) => {
      if (err) {
        cb(err)
        return
      }

      const toReturn = subs.map((sub) => {
        // remove clientId and _id from sub
        const { clientId, _id, ...resub } = sub
        return resub
      })

      cb(null, toReturn.length > 0 ? toReturn : null, client)
    })
  }

  countOffline (cb) {
    let clientsCount = 0
    toStream(this._cl.subscriptions.aggregate([{
      $group: {
        _id: '$clientId'
      }
    }])).on('data', () => {
      clientsCount++
    }).on('end', () => {
      cb(null, this._trie.subscriptionsCount, clientsCount)
    }).on('error', cb)
  }

  destroy (cb) {
    if (!this.ready) {
      this.once('ready', this.destroy.bind(this, cb))
      return
    }

    if (this._destroyed) {
      throw new Error('destroyed called twice!')
    }

    this._destroyed = true

    cb = cb || noop

    if (this._opts.db) {
      cb()
    } else {
      this._client.close(() => {
        // swallow err in case of close
        cb()
      })
    }
  }

  outgoingEnqueue (sub, packet, cb) {
    this.outgoingEnqueueCombi([sub], packet, cb)
  }

  outgoingEnqueueCombi (subs, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.outgoingEnqueueCombi.bind(this, subs, packet, cb))
      return
    }

    if (!subs || subs.length === 0) {
      return cb(null, packet)
    }

    const newp = new Packet(packet)
    const opts = this._opts

    function createPacket (sub) {
      return {
        clientId: sub.clientId,
        packet: decoratePacket(newp, opts)
      }
    }

    this._cl.outgoing.insertMany(subs.map(createPacket), (err) => {
      cb(err, packet)
    })
  }

  async * outgoingStream (client) {
    for await (const result of this._cl.outgoing.find({ clientId: client.id })) {
      yield asPacket(result)
    }
  }

  outgoingUpdate (client, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.outgoingUpdate.bind(this, client, packet, cb))
      return
    }
    if (packet.brokerId) {
      updateWithMessageId(this._cl, client, packet, cb)
    } else {
      updatePacket(this._cl, client, packet, cb)
    }
  }

  outgoingClearMessageId (client, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.outgoingClearMessageId.bind(this, client, packet, cb))
      return
    }

    const outgoing = this._cl.outgoing

    outgoing.findOne({
      clientId: client.id,
      'packet.messageId': packet.messageId
    }, (err, p) => {
      if (err) {
        return cb(err)
      }

      if (!p) {
        return cb(null)
      }

      outgoing.deleteOne({
        clientId: client.id,
        'packet.messageId': packet.messageId
      }, (err) => {
        if (p.packet.payload instanceof Binary) {
          p.packet.payload = p.packet.payload.buffer
        }
        cb(err, p.packet)
      })
    })
  }

  incomingStorePacket (client, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.incomingStorePacket.bind(this, client, packet, cb))
      return
    }

    const newp = new Packet(packet)
    newp.messageId = packet.messageId

    this._cl.incoming.insertOne({
      clientId: client.id,
      packet: decoratePacket(newp, this._opts)
    }, cb)
  }

  incomingGetPacket (client, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.incomingGetPacket.bind(this, client, packet, cb))
      return
    }

    this._cl.incoming.findOne({
      clientId: client.id,
      'packet.messageId': packet.messageId
    }, (err, result) => {
      if (err) {
        cb(err)
        return
      }

      if (!result) {
        cb(new Error('packet not found'), null, client)
        return
      }

      const packet = result.packet

      if (packet && packet.payload) {
        packet.payload = packet.payload.buffer
      }

      cb(null, packet, client)
    })
  }

  incomingDelPacket (client, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.incomingDelPacket.bind(this, client, packet, cb))
      return
    }

    this._cl.incoming.deleteOne({
      clientId: client.id,
      'packet.messageId': packet.messageId
    }, cb)
  }

  putWill (client, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.putWill.bind(this, client, packet, cb))
      return
    }

    packet.clientId = client.id
    packet.brokerId = this.broker.id
    this._cl.will.insertOne({
      clientId: client.id,
      packet: decoratePacket(packet, this._opts)
    }, (err) => {
      cb(err, client)
    })
  }

  getWill (client, cb) {
    this._cl.will.findOne({
      clientId: client.id
    }, (err, result) => {
      if (err) {
        cb(err)
        return
      }

      if (!result) {
        cb(null, null, client)
        return
      }

      const packet = result.packet

      if (packet && packet.payload) {
        packet.payload = packet.payload.buffer
      }

      cb(null, packet, client)
    })
  }

  delWill (client, cb) {
    const will = this._cl.will
    this.getWill(client, (err, packet) => {
      if (err || !packet) {
        cb(err, null, client)
        return
      }
      will.deleteOne({
        clientId: client.id
      }, (err) => {
        cb(err, packet, client)
      })
    })
  }

  async * streamWill (brokers) {
    const query = {}

    if (brokers) {
      query['packet.brokerId'] = { $nin: Object.keys(brokers) }
    }
    for await (const will of this._cl.will.find(query)) {
      yield asPacket(will)
    }
  }

  async * getClientList (topic) {
    const query = {}

    if (topic) {
      query.topic = topic
    }

    for await (const sub of this._cl.subscriptions.find(query)) {
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

    await ctx._cl.retained.bulkWrite(operations)
    // resolve all promises
    while (onEnd.length) onEnd.shift().call()
    // check if we have new packets in queue
    ctx.executing = false
    processRetainedBulk(ctx)
  }
}

function decoratePacket (packet, opts) {
  if (opts.ttl.packets) {
    packet.added = new Date()
  }
  return packet
}

function filterPattern (chunk, enc, cb) {
  if (this.matcher.match(chunk.topic).length > 0) {
    chunk.payload = chunk.payload.buffer
    // this is converting chunk to slow properties
    // https://github.com/sindresorhus/to-fast-properties
    // might make this faster
    delete chunk._id
    this.push(chunk)
  }
  cb()
}

function decorateSubscription (sub, opts) {
  if (opts.ttl.subscriptions) {
    sub.added = new Date()
  }
  return sub
}

function toSub (topic) {
  return {
    topic
  }
}

function asPacket (obj) {
  const packet = obj.packet

  if (packet.payload) {
    const buffer = packet.payload.buffer
    if (buffer && Buffer.isBuffer(buffer)) {
      packet.payload = packet.payload.buffer
    }
  }
  return packet
}

function updateWithMessageId (db, client, packet, cb) {
  db.outgoing.updateOne({
    clientId: client.id,
    'packet.brokerCounter': packet.brokerCounter,
    'packet.brokerId': packet.brokerId
  }, {
    $set: {
      'packet.messageId': packet.messageId
    }
  }, (err) => {
    cb(err, client, packet)
  })
}

function updatePacket (db, client, packet, cb) {
  db.outgoing.updateOne({
    clientId: client.id,
    'packet.messageId': packet.messageId
  }, {
    $set: {
      clientId: client.id,
      packet
    }
  }, (err) => {
    cb(err, client, packet)
  })
}

function promiseWithResolve () {
  // this can be replaced by Promise.withResolvers()in NodeJS >= 22
  let res, rej
  const promise = new Promise((resolve, reject) => {
    res = resolve
    rej = reject
  })
  return { promise, resolve: res, reject: rej }
}

function noop () { }

module.exports = (opts) => new AsyncMongoPersistence(opts)
