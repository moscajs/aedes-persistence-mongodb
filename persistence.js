'use strict'

var util = require('util')
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

  var conn = this._opts.url || 'mongodb://127.0.0.1/aedes?autoReconnect=true'

  mongodb.MongoClient.connect(conn, cb)
}

MongoPersistence.prototype._setup = function () {
  if (this.ready) {
    return
  }

  var that = this

  this._connect(function (err, db) {
    if (err) {
      this.emit('error', err)
      return
    }

    that._db = db

    var subscriptions = db.collection('subscriptions')

    that._cl = {
      subscriptions: subscriptions,
      retained: db.collection('retained'),
      will: db.collection('will'),
      outgoing: db.collection('outgoing'),
      incoming: db.collection('incoming')
    }

    subscriptions.find({
      qos: { $gt: 0 }
    }).on('data', function (chunk) {
      that._trie.add(chunk.topic, chunk)
    }).on('end', function () {
      that.emit('ready')
    }).on('error', function (err) {
      that.emit('error', err)
    })
  })
}

MongoPersistence.prototype.storeRetained = function (packet, cb) {
  if (!this.ready) {
    this.once('ready', this.storeRetained.bind(this, packet, cb))
    return
  }
  var criteria = { topic: packet.topic }
  if (packet.payload.length > 0) {
    this._cl.retained.update(criteria, packet, { upsert: true }, cb)
  } else {
    this._cl.retained.remove(criteria, cb)
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
  return pump(
    this._cl.retained.find({
      topic: new RegExp(pattern.replace(/(#|\+).*$/, ''))
    }),
    filterStream(pattern)
  )
}

MongoPersistence.prototype.addSubscriptions = function (client, subs, cb) {
  if (!this.ready) {
    this.once('ready', this.addSubscriptions.bind(this, client, subs, cb))
    return
  }

  var published = 0
  var errored = false
  var bulk = this._cl.subscriptions.initializeOrderedBulkOp()
  subs
    .forEach(function (sub) {
      bulk.find({
        clientId: client.id,
        topic: sub.topic
      }).upsert().updateOne({
        clientId: client.id,
        topic: sub.topic,
        qos: sub.qos
      })
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
    this._db.close(function () {
      // swallow err in case of close
      cb()
    })
    this._db.unref()
  }
}

MongoPersistence.prototype.outgoingEnqueue = function (sub, packet, cb) {
  if (!this.ready) {
    this.once('ready', this.outgoingEnqueue.bind(this, sub, packet, cb))
    return
  }

  this._cl.outgoing.insert({
    clientId: sub.clientId,
    packet: new Packet(packet)
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
  db.outgoing.update({
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
  db.outgoing.update({
    clientId: client.id,
    'packet.messageId': packet.messageId
  }, {
    clientId: client.id,
    packet: packet
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

    outgoing.remove({
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

  this._cl.incoming.insert({
    clientId: client.id,
    packet: newp
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

  this._cl.incoming.remove({
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
  this._cl.will.insert({
    clientId: client.id,
    packet: packet
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
    will.remove({
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
