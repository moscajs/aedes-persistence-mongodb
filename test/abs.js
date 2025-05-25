'use strict'

const test = require('node:test')
const persistence = require('../')
const { MongoClient } = require('mongodb')
const abs = require('aedes-persistence/abstract')
const mqemitterMongo = require('mqemitter-mongodb')
const dbname = 'aedes-test'
const mongourl = `mongodb://127.0.0.1/${dbname}`

async function cleanDB () {
  const mongoClient = new MongoClient(mongourl, { w: 1 })
  const db = mongoClient.db(dbname)
  await db.admin().ping()

  const collections = [
    db.collection('subscriptions'),
    db.collection('retained'),
    db.collection('will'),
    db.collection('outgoing'),
    db.collection('incoming')
  ]

  await Promise.all(collections.map((c) => c.deleteMany({})))
  await mongoClient.close()
}

function makeBuildEmitter (dbopts) {
  return function buildEmitter () {
    const emitter = mqemitterMongo(dbopts)
    return emitter
  }
}

function makePersistence (dbopts) {
  return async function build () {
    await cleanDB()
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
  const defaultDBopts = {
    url: mongourl
  }
  // run all tests from aedes-abstract-persistence
  abs({
    test,
    buildEmitter: makeBuildEmitter(defaultDBopts),
    persistence: makePersistence(defaultDBopts),
    waitForReady: true,
  })
}
doTest()
