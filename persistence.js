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
    'subscriptions'
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
}

MongoPersistence.prototype.destroy = function (cb) {
  cb = cb || noop
  if (!this._db) {
    cb()
    return
  }

  this._db.close(cb)
}

function noop () {}

module.exports = MongoPersistence
