'use strict'

const test = require('node:test')
const persistence = require('../')
const { MongoClient } = require('mongodb')
const abs = require('aedes-cached-persistence/abstract')
const mqemitterMongo = require('mqemitter-mongodb')
const dbname = 'aedes-test'
const mongourl = `mongodb://127.0.0.1/${dbname}`

async function cleanDB (collections) {
  await Promise.all(collections.map((c) => c.deleteMany({})))
}

function makeBuildEmitter (dbopts) {
  return function buildEmitter () {
    const emitter = mqemitterMongo(dbopts)
    return emitter
  }
}

function makePersistence (dbopts, collections) {
  return async function build () {
    await cleanDB(collections)
    const instance = persistence(dbopts)
    // make intance.destroy close the broker as well
    const oldDestroy = instance.destroy.bind(instance)
    instance.destroy = (cb) => {
      oldDestroy(() => {
        instance.broker.mq.close(cb)
      })
    }
    return instance
  }
}
// Testing starts here.
async function doTest () {
  const mongoClient = new MongoClient(mongourl, { w: 1 })
  const db = mongoClient.db(dbname)

  const collections = [
    db.collection('subscriptions'),
    db.collection('retained'),
    db.collection('will'),
    db.collection('outgoing'),
    db.collection('incoming')
  ]

  const defaultDBopts = {
    url: mongourl
  }
  // run all tests from aedes-abstract-persistence
  abs({
    test,
    buildEmitter: makeBuildEmitter(defaultDBopts),
    persistence: makePersistence(defaultDBopts, collections),
    waitForReady: true,
  })

  test.after(() => {
    mongoClient.close()
  })
}
doTest()
