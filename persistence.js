'use strict'

var util = require('util')
var CachedPersistence = require('aedes-cached-persistence')
var Packet = CachedPersistence.Packet
var mongojs = require('mongojs')
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

  this._db = mongojs(opts.url, [
    'retained',
    'subscriptions',
    'outgoing',
    'incoming',
    'will'
  ])

  CachedPersistence.call(this, opts)
}

util.inherits(MongoPersistence, CachedPersistence)

MongoPersistence.prototype._setup = function () {
  this.emit('ready')
}

MongoPersistence.prototype.storeRetained = function (packet, cb) {
  var criteria = { topic: packet.topic }
  if (packet.payload.length > 0) {
    this._db.retained.update(criteria, packet, { upsert: true }, cb)
  } else {
    this._db.retained.remove(criteria, cb)
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
    this._db.retained.find({
      topic: new RegExp(pattern.replace(/(#|\+).*$/, ''))
    }),
    filterStream(pattern)
  )
}

MongoPersistence.prototype.addSubscriptions = function (client, subs, cb) {
  var published = 0
  var count = 0
  var errored = false
  var that = this
  var bulk = this._db.subscriptions.initializeOrderedBulkOp()
  subs
    .forEach(function (sub) {
      if (sub.qos > 0) {
        count++
        that._waitFor(client, sub.topic, finish)
      }
      bulk.find({
        clientId: client.id,
        topic: sub.topic
      }).upsert().updateOne({
        clientId: client.id,
        topic: sub.topic,
        qos: sub.qos
      })
    })

  bulk.execute(finish)

  this._addedSubscriptions(client, subs, finish)

  function finish (err) {
    if (err && !errored) {
      errored = true
      cb(err, client)
      return
    } else if (errored) {
      return
    }
    published++
    console.log(published, count)
    if (published === count + 2) {
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
  var published = 0
  var count = 0
  var errored = false
  var that = this
  var bulk = this._db.subscriptions.initializeOrderedBulkOp()
  subs
    .forEach(function (topic) {
      count++
      that._waitFor(client, topic, finish)
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
    if (published === count + 2 && !errored) {
      cb(null, client)
    }
  }
}

MongoPersistence.prototype.subscriptionsByClient = function (client, cb) {
  this._db.subscriptions.find({ clientId: client.id }).toArray(function (err, subs) {
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
  setImmediate(cb, null, 0, 0)
  // var subsCount = -1
  // var clientsCount = -1
  // this._db.subscriptions.aggregate([{
  //   $group: {
  //     _id: '$clientId',
  //     count: {
  //       $sum: 1
  //     }
  //   }
  // }, {
  //   $group: {
  //     count: {
  //       $sum: 1
  //     }
  //   }
  // }]).toArray(function (err, count) {
  //   if (err) { return cb(err) }
  //   console.log(count)
  //   subsCount = parseInt(count)
  //   if (clientsCount >= 0)
  //     cb(null, subsCount, clientsCount)
  // })
  // pipeline.get(offlineClientsCountKey, function (err, count) {
  //   if (err) { return cb(err) }
  //   clientsCount = parseInt(count)
  //   if (subsCount >= 0)
  //     cb(null, subsCount, clientsCount)
  // })
}

MongoPersistence.prototype.destroy = function (cb) {
  cb = cb || noop
  if (!this._db) {
    cb()
    return
  }

  this._db.close(cb)
}

MongoPersistence.prototype.outgoingEnqueue = function (sub, packet, cb) {
  this._db.outgoing.insert({
    clientId: sub.clientId,
    packet: new Packet(packet)
  }, cb)
}

function asPacket (obj, enc, cb) {
  var packet = obj.packet
  if (packet.payload) {
    packet.payload = packet.payload.buffer
  }
  cb(null, packet)
}

MongoPersistence.prototype.outgoingStream = function (client) {
  return pump(
    this._db.outgoing.find({ clientId: client.id }),
    through.obj(asPacket))
}

function updateWithMessageId (db, client, packet, cb) {
  db.outgoing.update({
    clientId: client.id,
    "packet.brokerCounter": packet.brokerCounter,
    "packet.brokerId": packet.brokerId
  }, {
    $set: {
      "packet.messageId": packet.messageId
    }
  }, function (err) {
    cb(err, client, packet)
  })
}

function updatePacket (db, client, packet, cb) {
  db.outgoing.update({
    clientId: client.id,
    "packet.messageId": packet.messageId
  }, {
    clientId: client.id,
    packet: packet
  }, function (err) {
    cb(err, client, packet)
  })
}

MongoPersistence.prototype.outgoingUpdate = function (client, packet, cb) {
  var that = this
  if (packet.brokerId) {
    updateWithMessageId(this._db, client, packet, cb)
  } else {
    updatePacket(this._db, client, packet, cb)
  }
}

MongoPersistence.prototype.outgoingClearMessageId = function (client, packet, cb) {
  this._db.outgoing.remove({
    clientId: client.id,
    "packet.messageId": packet.messageId
  }, function (err) {
    cb(err, client)
  })
}

MongoPersistence.prototype.incomingStorePacket = function (client, packet, cb) {
  var newp = new Packet(packet)
  newp.messageId = packet.messageId

  this._db.incoming.insert({
    clientId: client.id,
    packet: newp
  }, cb)
}

MongoPersistence.prototype.incomingGetPacket = function (client, packet, cb) {
  this._db.incoming.findOne({
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
  this._db.incoming.remove({
    clientId: client.id,
    'packet.messageId': packet.messageId
  }, cb)
}

MongoPersistence.prototype.putWill = function (client, packet, cb) {
  packet.clientId = client.id
  packet.brokerId = this.broker.id
  this._db.will.insert({
    clientId: client.id,
    packet: packet
  }, function (err) {
    cb(err, client)
  })
}

MongoPersistence.prototype.getWill = function (client, cb) {
  this._db.will.findOne({
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
  var db = this._db
  this.getWill(client, function (err, packet) {
    if (err || !packet) {
      cb(err, null, client)
      return
    }
    db.will.remove({
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
  return pump(this._db.will.find(query), through.obj(asPacket))
}

function noop () {}

module.exports = MongoPersistence
