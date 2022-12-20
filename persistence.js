'use strict'

const urlModule = require('native-url')
const escape = require('escape-string-regexp')
const CachedPersistence = require('aedes-cached-persistence')
const Packet = CachedPersistence.Packet
const mongodb = require('mongodb')
const pump = require('pump')
const through = require('through2')
const parallel = require('fastparallel')()
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

class MongoPersistence extends CachedPersistence {
  constructor (opts = {}) {
    super(opts)

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
    this.packetsQueue = [] // used for storing retained packets with ordered bulks
    this.executing = false // used as lock while a bulk is executing
  }

  _connect (cb) {
    if (this._opts.db) {
      cb(null, this._opts.db)
      return
    }

    const conn = this._opts.url || 'mongodb://127.0.0.1/aedes'

    const options = { useNewUrlParser: true, useUnifiedTopology: true }

    if (this._opts.mongoOptions) {
      Object.assign(options, this._opts.mongoOptions)
    }

    mongodb.MongoClient.connect(conn, options, cb)
  }

  _setup () {
    if (this.ready) {
      return
    }

    this._connect((err, client) => {
      if (err) {
        this.emit('error', err)
        return
      }

      this._client = client

      let db
      if (this._opts.db) {
        db = this._opts.db
      } else {
        const urlParsed = urlModule.parse(this._opts.url)
        const databaseName = this._opts.database || (urlParsed.pathname ? urlParsed.pathname.substr(1) : undefined)
        db = this._db = client.db(databaseName)
      }

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

      const initCollections = () => {
        const finishInit = () => {
          toStream(subscriptions.find({
            qos: { $gte: 0 }
          })).on('data', (chunk) => {
            this._trie.add(chunk.topic, chunk)
          }).on('end', () => {
            this.emit('ready')
          }).on('error', (err) => {
            this.emit('error', err)
          })
        }

        function createIndex (opts, cb) {
          const indexOpts = { name: opts.name }
          if (typeof opts.expireAfterSeconds === 'number') {
            indexOpts.expireAfterSeconds = opts.expireAfterSeconds
          }

          return this._cl[opts.collection].createIndex(opts.key, indexOpts, cb)
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

        parallel(this, createIndex, indexes, finishInit)
      }

      // drop existing indexes (if exists)
      if (this._opts.dropExistingIndexes) {
        this._dropIndexes(db, initCollections)
      } else {
        initCollections()
      }

      if (this._opts.ttlAfterDisconnected) {
        // To avoid stale subscriptions that might be left behind by broker shutting
        // down while clients were connected, set all to disconnected on startup.
        this._cl.subscriptions.updateMany({ disconnected: { $exists: false } }, { $currentDate: { disconnected: true } })

        // Handlers for setting and clearing the disconnected timestamp on subscriptions
        this.broker.on('clientReady', (client) => {
          this._cl.subscriptions.updateMany({ clientId: client.id }, { $unset: { disconnected: true } })
        })
        this.broker.on('clientDisconnect', (client) => {
          this._cl.subscriptions.updateMany({ clientId: client.id }, { $currentDate: { disconnected: true } })
        })
      }
    })
  }

  _dropIndexes (db, cb) {
    db.collections((err, collections) => {
      if (err) { throw err }

      let done = 0

      function finish (err) {
        if (err) { throw err }

        done++
        if (done >= collections.length && typeof cb === 'function') {
          cb()
        }
      }

      if (collections.length === 0) {
        finish()
      } else {
        for (let i = 0; i < collections.length; i++) {
          collections[i].indexExists('ttl', (err, exists) => {
            if (err) { throw err }

            if (exists) {
              collections[i].dropIndex('ttl', finish)
            } else {
              finish()
            }
          })
        }
      }
    })
  }

  storeRetained (packet, cb) {
    if (!this.ready) {
      this.once('ready', this.storeRetained.bind(this, packet, cb))
      return
    }

    this.packetsQueue.push({ packet, cb })
    this._executeBulk()
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

  outgoingStream (client) {
    return pump(
      toStream(this._cl.outgoing.find({ clientId: client.id })),
      through.obj(asPacket))
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
        if (p.packet.payload instanceof mongodb.Binary) {
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

  streamWill (brokers) {
    const query = {}

    if (brokers) {
      query['packet.brokerId'] = { $nin: Object.keys(brokers) }
    }
    return pump(toStream(this._cl.will.find(query)), through.obj(asPacket))
  }

  getClientList (topic) {
    const query = {}

    if (topic) {
      query.topic = topic
    }

    return pump(toStream(this._cl.subscriptions.find(query)), through.obj(function asPacket (obj, enc, cb) {
      this.push(obj.clientId)
      cb()
    }))
  }

  _executeBulk () {
    if (!this.executing && !this._destroyed && this.packetsQueue.length > 0) {
      this.executing = true
      const bulk = this._cl.retained.initializeOrderedBulkOp()
      const onEnd = []

      while (this.packetsQueue.length) {
        const p = this.packetsQueue.shift()
        onEnd.push(p.cb)
        const criteria = { topic: p.packet.topic }

        if (p.packet.payload.length > 0) {
          bulk.find(criteria).upsert().updateOne({
            $set: decoratePacket(p.packet, this._opts)
          })
        } else {
          bulk.find(criteria).deleteOne()
        }
      }

      bulk.execute(() => {
        while (onEnd.length) onEnd.shift().call()
        this.executing = false
        this._executeBulk()
      })
    }
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

function asPacket (obj, enc, cb) {
  const packet = obj.packet

  if (packet.payload) {
    const buffer = packet.payload.buffer
    if (buffer && Buffer.isBuffer(buffer)) {
      packet.payload = packet.payload.buffer
    }
  }
  cb(null, packet)
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

function noop () { }

module.exports = (opts) => new MongoPersistence(opts)
