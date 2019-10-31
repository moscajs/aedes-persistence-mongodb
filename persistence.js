'use strict'

var util = require('util')
var urlModule = require('url')
var escape = require('escape-string-regexp')
var CachedPersistence = require('aedes-cached-persistence')
var Packet = CachedPersistence.Packet
var mongodb = require('mongodb')
var pump = require('pump')
var through = require('through2')
var Qlobber = require('qlobber').Qlobber
var qlobberOpts = {
  separator: '/',
  wildcard_one: '+',
  wildcard_some: '#'
}

function MongoPersistence (opts) {
  if (!(this instanceof MongoPersistence)) {
    return new MongoPersistence(opts)
  }

  opts = opts || {}
  opts.ttl = opts.ttl || {}

  this._opts = opts
  this._db = null
  this._cl = null

  CachedPersistence.call(this, opts)
}

util.inherits(MongoPersistence, CachedPersistence)

MongoPersistence.prototype._connect = function (cb) {
  if (this._opts.db) {
    cb(null, this._opts.db)
    return
  }

  var conn = this._opts.url || 'mongodb://127.0.0.1/aedes'

  // TODO add options
  mongodb.MongoClient.connect(conn, { useNewUrlParser: true, useUnifiedTopology: true }, cb)
}

MongoPersistence.prototype._setup = function () {
  if (this.ready) {
    return
  }

  var that = this

  this._connect(function (err, client) {
    if (err) {
      this.emit('error', err)
      return
    }

    that._client = client

    var db
    if (that._opts.db) {
      db = that._opts.db
    } else {
      var urlParsed = urlModule.parse(that._opts.url)
      var databaseName = that._opts.database || (urlParsed.pathname ? urlParsed.pathname.substr(1) : undefined)
      databaseName = databaseName.substr(databaseName.lastIndexOf('/') + 1)
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

    if (that._opts.ttl.subscriptions) {
      subscriptions.createIndex({ 'added': 1 }, { expireAfterSeconds: that._opts.ttl.subscriptions })
    }

    if (that._opts.ttl.packets) {
      retained.createIndex({ 'added': 1 }, { expireAfterSeconds: that._opts.ttl.packets })
      will.createIndex({ 'added': 1 }, { expireAfterSeconds: that._opts.ttl.packets })
      outgoing.createIndex({ 'added': 1 }, { expireAfterSeconds: that._opts.ttl.packets })
      incoming.createIndex({ 'added': 1 }, { expireAfterSeconds: that._opts.ttl.packets })
    }

    subscriptions.find({
      qos: { $gte: 0 }
    }).on('data', function (chunk) {
      that._trie.add(chunk.topic, chunk)
    }).on('end', function () {
      that.emit('ready')
    }).on('error', function (err) {
      that.emit('error', err)
    })
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
  var criteria = { topic: packet.topic }
  if (packet.payload.length > 0) {
    this._cl.retained.updateOne(criteria, { $set: decoratePacket(packet, this._opts) }, { upsert: true }, cb)
  } else {
    this._cl.retained.deleteOne(criteria, cb)
  }
}

function filterStream (pattern) {
  var instance = through.obj(filterPattern)
  instance.matcher = new Qlobber(qlobberOpts)
  instance.matcher.add(pattern, true)
  return instance
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
  var actual = escape(pattern).replace(/(#|\\\+).*$/, '')
  return pump(
    this._cl.retained.find({
      topic: new RegExp(actual)
    }),
    filterStream(pattern)
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
      }).upsert().updateOne(
        decorateSubscription(subscription, that._opts)
      )
    })

  this._addedSubscriptions(client, subs, function (err) {
    finish(err)
    bulk.execute(finish)
  })

  function finish (err) {
    if (err && !errored) {
      errored = true
      cb(err, client)
      return
    } else if (errored) {
      return
    }
    published++
    if (published === 2) {
      cb(null, client)
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
      }).removeOne()
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
  if (!this.ready) {
    this.once('ready', this.outgoingEnqueue.bind(this, sub, packet, cb))
    return
  }

  var newp = new Packet(packet)

  this._cl.outgoing.insertOne({
    clientId: sub.clientId,
    packet: decoratePacket(newp, this._opts)
  }, cb)
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
    query['topic'] = topic
  }

  return pump(this._cl.subscriptions.find(query), through.obj(function asPacket (obj, enc, cb) {
    this.push(obj.clientId)
    cb()
  }))
}

function noop () {}

module.exports = MongoPersistence
