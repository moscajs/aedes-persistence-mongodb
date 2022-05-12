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

    CachedPersistence.call(this, opts)
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

    const that = this

    this._connect(function (err, client) {
      if (err) {
        that.emit('error', err)
        return
      }

      that._client = client

      let db
      if (that._opts.db) {
        db = that._opts.db
      } else {
        const urlParsed = urlModule.parse(that._opts.url)
        const databaseName = that._opts.database || (urlParsed.pathname ? urlParsed.pathname.substr(1) : undefined)
        db = that._db = client.db(databaseName)
      }

      const subscriptions = db.collection('subscriptions')
      const retained = db.collection('retained')
      const will = db.collection('will')
      const outgoing = db.collection('outgoing')
      const incoming = db.collection('incoming')

      that._cl = {
        subscriptions,
        retained,
        will,
        outgoing,
        incoming
      }

      function initCollections () {
        function finishInit () {
          toStream(subscriptions.find({
            qos: { $gte: 0 }
          })).on('data', function (chunk) {
            that._trie.add(chunk.topic, chunk)
          }).on('end', function () {
            that.emit('ready')
          }).on('error', function (err) {
            that.emit('error', err)
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

        if (that._opts.ttl.subscriptions >= 0) {
          indexes.push({
            collection: 'subscriptions',
            key: 'added',
            name: 'ttl',
            expireAfterSeconds: that._opts.ttl.subscriptions
          })
        }

        if (that._opts.ttl.packets) {
          if (that._opts.ttl.packets.retained >= 0) {
            indexes.push({
              collection: 'retained',
              key: 'added',
              name: 'ttl',
              expireAfterSeconds: that._opts.ttl.packets.retained
            })
          }

          if (that._opts.ttl.packets.will >= 0) {
            indexes.push({
              collection: 'will',
              key: 'packet.added',
              name: 'ttl',
              expireAfterSeconds: that._opts.ttl.packets.will
            })
          }

          if (that._opts.ttl.packets.outgoing >= 0) {
            indexes.push({
              collection: 'outgoing',
              key: 'packet.added',
              name: 'ttl',
              expireAfterSeconds: that._opts.ttl.packets.outgoing
            })
          }

          if (that._opts.ttl.packets.incoming >= 0) {
            indexes.push({
              collection: 'incoming',
              key: 'packet.added',
              name: 'ttl',
              expireAfterSeconds: that._opts.ttl.packets.incoming
            })
          }
        }

        parallel(that, createIndex, indexes, finishInit)
      }

      // drop existing indexes (if exists)
      if (that._opts.dropExistingIndexes) {
        that._dropIndexes(db, initCollections)
      } else {
        initCollections()
      }
    })
  }

  _dropIndexes (db, cb) {
    db.collections(function (err, collections) {
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
          collections[i].indexExists('ttl', function (err, exists) {
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
    executeBulk(this)
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

    const that = this
    let published = 0
    let errored = false
    const bulk = this._cl.subscriptions.initializeOrderedBulkOp()
    subs
      .forEach(function (sub) {
        const subscription = Object.assign({}, sub)
        subscription.clientId = client.id
        bulk.find({
          clientId: client.id,
          topic: sub.topic
        }).upsert().updateOne({
          $set: decorateSubscription(subscription, that._opts)
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
      .forEach(function (topic) {
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

    this._cl.subscriptions.find({ clientId: client.id }).toArray(function (err, subs) {
      if (err) {
        cb(err)
        return
      }

      const toReturn = subs.map(function (sub) {
        // remove clientId from sub
        const { clientId, _id, ...resub } = sub
        return resub
      })

      cb(null, toReturn.length > 0 ? toReturn : null, client)
    })
  }

  countOffline (cb) {
    let clientsCount = 0
    const that = this
    toStream(this._cl.subscriptions.aggregate([{
      $group: {
        _id: '$clientId'
      }
    }])).on('data', function () {
      clientsCount++
    }).on('end', function () {
      cb(null, that._trie.subscriptionsCount, clientsCount)
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
      this._client.close(function () {
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

    this._cl.outgoing.insertMany(subs.map(createPacket), function (err) {
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
    }, function (err, p) {
      if (err) {
        return cb(err)
      }

      if (!p) {
        return cb(null)
      }

      outgoing.deleteOne({
        clientId: client.id,
        'packet.messageId': packet.messageId
      }, function (err) {
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
    }, function (err, result) {
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
    }, function (err) {
      cb(err, client)
    })
  }

  getWill (client, cb) {
    this._cl.will.findOne({
      clientId: client.id
    }, function (err, result) {
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
    this.getWill(client, function (err, packet) {
      if (err || !packet) {
        cb(err, null, client)
        return
      }
      will.deleteOne({
        clientId: client.id
      }, function (err) {
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
}

function decoratePacket (packet, opts) {
  if (opts.ttl.packets) {
    packet.added = new Date()
  }
  return packet
}

function executeBulk (that) {
  if (!that.executing && that.packetsQueue.length > 0) {
    that.executing = true
    const bulk = that._cl.retained.initializeOrderedBulkOp()
    const onEnd = []

    while (that.packetsQueue.length) {
      const p = that.packetsQueue.shift()
      onEnd.push(p.cb)
      const criteria = { topic: p.packet.topic }

      if (p.packet.payload.length > 0) {
        bulk.find(criteria).upsert().updateOne({
          $set: decoratePacket(p.packet, that._opts)
        })
      } else {
        bulk.find(criteria).deleteOne()
      }
    }

    bulk.execute(function () {
      while (onEnd.length) onEnd.shift().call()
      that.executing = false
      executeBulk(that)
    })
  }
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
  }, function (err) {
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
  }, function (err) {
    cb(err, client, packet)
  })
}

function noop () { }

module.exports = (opts) => new MongoPersistence(opts)
