'use strict'

const util = require('util')
const urlModule = require('native-url')
const escape = require('escape-string-regexp')
const CachedPersistence = require('aedes-cached-persistence')
const Packet = CachedPersistence.Packet
const mongodb = require('mongodb')
const pump = require('pump')
const through = require('through2')
const parallel = require('fastparallel')()
const Qlobber = require('qlobber').Qlobber
const qlobberOpts = {
  separator: '/',
  wildcard_one: '+',
  wildcard_some: '#',
  match_empty_levels: true
}

function MongoPersistence (opts) {
  if (!(this instanceof MongoPersistence)) {
    return new MongoPersistence(opts)
  }

  opts = opts || {}
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

util.inherits(MongoPersistence, CachedPersistence)

MongoPersistence.prototype._connect = function (cb) {
  if (this._opts.db) {
    cb(null, this._opts.db)
    return
  }

  const conn = this._opts.url || 'mongodb://127.0.0.1/aedes'

  mongodb.MongoClient.connect(conn, { useNewUrlParser: true, useUnifiedTopology: true }, cb)
}

MongoPersistence.prototype._setup = function () {
  if (this.ready) {
    return
  }

  var that = this

  this._connect(function (err, client) {
    if (err) {
      that.emit('error', err)
      return
    }

    that._client = client

    var db
    if (that._opts.db) {
      db = that._opts.db
    } else {
      var urlParsed = urlModule.parse(that._opts.url)
      var databaseName = that._opts.database || (urlParsed.pathname ? urlParsed.pathname.substr(1) : undefined)
      db = that._db = client.db(databaseName)
    }

    var subscriptions = db.collection('subscriptions')
    var retained = db.collection('retained')
    var will = db.collection('will')
    var outgoing = db.collection('outgoing')
    var incoming = db.collection('incoming')

    that._cl = {
      subscriptions: subscriptions,
      retained: retained,
      will: will,
      outgoing: outgoing,
      incoming: incoming
    }

    function initCollections () {
      function finishInit () {
        subscriptions.find({
          qos: { $gte: 0 }
        }).on('data', function (chunk) {
          that._trie.add(chunk.topic, chunk)
        }).on('end', function () {
          that.emit('ready')
        }).on('error', function (err) {
          that.emit('error', err)
        })
      }

      function createTtlIndex (opts, cb) {
        this._cl[opts.collection].createIndex(opts.key, { expireAfterSeconds: opts.expireAfterSeconds, name: 'ttl' }, cb)
      }

      const indexes = []

      if (that._opts.ttl.subscriptions >= 0) {
        indexes.push({
          collection: 'subscriptions',
          key: 'added',
          expireAfterSeconds: that._opts.ttl.subscriptions
        })
      }

      if (that._opts.ttl.packets) {
        if (that._opts.ttl.packets.retained >= 0) {
          indexes.push({
            collection: 'retained',
            key: 'added',
            expireAfterSeconds: that._opts.ttl.packets.retained
          })
        }

        if (that._opts.ttl.packets.will >= 0) {
          indexes.push({
            collection: 'will',
            key: 'packet.added',
            expireAfterSeconds: that._opts.ttl.packets.will
          })
        }

        if (that._opts.ttl.packets.outgoing >= 0) {
          indexes.push({
            collection: 'outgoing',
            key: 'packet.added',
            expireAfterSeconds: that._opts.ttl.packets.outgoing
          })
        }

        if (that._opts.ttl.packets.incoming >= 0) {
          indexes.push({
            collection: 'incoming',
            key: 'packet.added',
            expireAfterSeconds: that._opts.ttl.packets.incoming
          })
        }
      }

      parallel(that, createTtlIndex, indexes, finishInit)
    }

    // drop existing indexes (if exists)
    if (that._opts.dropExistingIndexes) {
      that._dropIndexes(db, initCollections)
    } else {
      initCollections()
    }
  })
}

MongoPersistence.prototype._dropIndexes = function (db, cb) {
  db.collections(function (err, collections) {
    if (err) throw err

    var done = 0

    function finish (err) {
      if (err) throw err

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
          if (err) throw err

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

function decoratePacket (packet, opts) {
  if (opts.ttl.packets) {
    packet.added = new Date()
  }
  return packet
}

MongoPersistence.prototype.storeRetained = function (packet, cb) {
  if (!this.ready) {
    this.once('ready', this.storeRetained.bind(this, packet, cb))
    return
  }

  this.packetsQueue.push({ packet, cb })
  executeBulk(this)
}

function executeBulk (that) {
  if (!that.executing && that.packetsQueue.length > 0) {
    that.executing = true
    var bulk = that._cl.retained.initializeOrderedBulkOp()
    var onEnd = []

    while (that.packetsQueue.length) {
      var p = that.packetsQueue.shift()
      onEnd.push(p.cb)
      var criteria = { topic: p.packet.topic }

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

MongoPersistence.prototype.createRetainedStream = function (pattern) {
  return this.createRetainedStreamCombi([pattern])
}

MongoPersistence.prototype.createRetainedStreamCombi = function (patterns) {
  var regex = []

  var instance = through.obj(filterPattern)
  instance.matcher = new Qlobber(qlobberOpts)

  for (let i = 0; i < patterns.length; i++) {
    instance.matcher.add(patterns[i], true)
    regex.push(escape(patterns[i]).replace(/(\/*#|\\\+).*$/, ''))
  }

  regex = regex.join('|')

  return pump(
    this._cl.retained.find({
      topic: new RegExp(regex)
    }),
    instance
  )
}

function decorateSubscription (sub, opts) {
  if (opts.ttl.subscriptions) {
    sub.added = new Date()
  }
  return sub
}

MongoPersistence.prototype.addSubscriptions = function (client, subs, cb) {
  if (!this.ready) {
    this.once('ready', this.addSubscriptions.bind(this, client, subs, cb))
    return
  }

  var that = this
  var published = 0
  var errored = false
  var bulk = this._cl.subscriptions.initializeOrderedBulkOp()
  subs
    .forEach(function (sub) {
      var subscription = {
        clientId: client.id,
        topic: sub.topic,
        qos: sub.qos
      }
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

function toSub (topic) {
  return {
    topic: topic
  }
}

MongoPersistence.prototype.removeSubscriptions = function (client, subs, cb) {
  if (!this.ready) {
    this.once('ready', this.removeSubscriptions.bind(this, client, subs, cb))
    return
  }

  var published = 0
  var errored = false
  var bulk = this._cl.subscriptions.initializeOrderedBulkOp()
  subs
    .forEach(function (topic) {
      bulk.find({
        clientId: client.id,
        topic: topic
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

MongoPersistence.prototype.subscriptionsByClient = function (client, cb) {
  if (!this.ready) {
    this.once('ready', this.subscriptionsByClient.bind(this, client, cb))
    return
  }

  this._cl.subscriptions.find({ clientId: client.id }).toArray(function (err, subs) {
    if (err) {
      cb(err)
      return
    }

    var toReturn = subs.map(function (sub) {
      return {
        topic: sub.topic,
        qos: sub.qos
      }
    })

    cb(null, toReturn.length > 0 ? toReturn : null, client)
  })
}

MongoPersistence.prototype.countOffline = function (cb) {
  var clientsCount = 0
  var that = this
  this._cl.subscriptions.aggregate([{
    $group: {
      _id: '$clientId'
    }
  }]).on('data', function () {
    clientsCount++
  }).on('end', function () {
    cb(null, that._trie.subscriptionsCount, clientsCount)
  }).on('error', cb)
}

MongoPersistence.prototype.destroy = function (cb) {
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

MongoPersistence.prototype.outgoingEnqueue = function (sub, packet, cb) {
  this.outgoingEnqueueCombi([sub], packet, cb)
}

MongoPersistence.prototype.outgoingEnqueueCombi = function (subs, packet, cb) {
  if (!this.ready) {
    this.once('ready', this.outgoingEnqueueCombi.bind(this, subs, packet, cb))
    return
  }

  if (!subs || subs.length === 0) {
    return cb(null, packet)
  }

  var newp = new Packet(packet)
  var opts = this._opts

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

function asPacket (obj, enc, cb) {
  var packet = obj.packet

  if (packet.payload) {
    var buffer = packet.payload.buffer
    if (buffer && Buffer.isBuffer(buffer)) {
      packet.payload = packet.payload.buffer
    }
  }
  cb(null, packet)
}

MongoPersistence.prototype.outgoingStream = function (client) {
  return pump(
    this._cl.outgoing.find({ clientId: client.id }),
    through.obj(asPacket))
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
      packet: packet
    }
  }, function (err) {
    cb(err, client, packet)
  })
}

MongoPersistence.prototype.outgoingUpdate = function (client, packet, cb) {
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

MongoPersistence.prototype.outgoingClearMessageId = function (client, packet, cb) {
  if (!this.ready) {
    this.once('ready', this.outgoingClearMessageId.bind(this, client, packet, cb))
    return
  }

  var outgoing = this._cl.outgoing

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

MongoPersistence.prototype.incomingStorePacket = function (client, packet, cb) {
  if (!this.ready) {
    this.once('ready', this.incomingStorePacket.bind(this, client, packet, cb))
    return
  }

  var newp = new Packet(packet)
  newp.messageId = packet.messageId

  this._cl.incoming.insertOne({
    clientId: client.id,
    packet: decoratePacket(newp, this._opts)
  }, cb)
}

MongoPersistence.prototype.incomingGetPacket = function (client, packet, cb) {
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

    var packet = result.packet

    if (packet && packet.payload) {
      packet.payload = packet.payload.buffer
    }

    cb(null, packet, client)
  })
}

MongoPersistence.prototype.incomingDelPacket = function (client, packet, cb) {
  if (!this.ready) {
    this.once('ready', this.incomingDelPacket.bind(this, client, packet, cb))
    return
  }

  this._cl.incoming.deleteOne({
    clientId: client.id,
    'packet.messageId': packet.messageId
  }, cb)
}

MongoPersistence.prototype.putWill = function (client, packet, cb) {
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

MongoPersistence.prototype.getWill = function (client, cb) {
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

    var packet = result.packet

    if (packet && packet.payload) {
      packet.payload = packet.payload.buffer
    }

    cb(null, packet, client)
  })
}

MongoPersistence.prototype.delWill = function (client, cb) {
  var will = this._cl.will
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

MongoPersistence.prototype.streamWill = function (brokers) {
  var query = {}

  if (brokers) {
    query['packet.brokerId'] = { $nin: Object.keys(brokers) }
  }
  return pump(this._cl.will.find(query), through.obj(asPacket))
}

MongoPersistence.prototype.getClientList = function (topic) {
  var query = {}

  if (topic) {
    query.topic = topic
  }

  return pump(this._cl.subscriptions.find(query), through.obj(function asPacket (obj, enc, cb) {
    this.push(obj.clientId)
    cb()
  }))
}

function noop () { }

module.exports = MongoPersistence
