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
    'retained'
  ])

  CachedPersistence.call(this, opts)
}

util.inherits(MongoPersistence, CachedPersistence)

MongoPersistence.prototype._setup = function () {
  this.emit('ready')
}

MongoPersistence.prototype.storeRetained = function (packet, cb) {
  this._db.retained.insert(packet, cb)
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

function RetainedPacket (original) {
  this.cmd = original.cmd
  this.id = original.cmd
}

module.exports = MongoPersistence
