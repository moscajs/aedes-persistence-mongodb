'use strict'

const test = require('node:test')
const { EventEmitter, once } = require('node:events')
const persistence = require('../')
const { MongoClient } = require('mongodb')
const mqemitterMongo = require('mqemitter-mongodb')
const { PromisifiedPersistence } = require('aedes-persistence/promisified.js')
const dbname = 'aedes-test'
const mongourl = `mongodb://127.0.0.1/${dbname}`

function sleep (msec) {
  return new Promise(resolve => setTimeout(resolve, msec))
}

// helpers

async function closeEmitter (emitter) {
  return new Promise((resolve) => {
    emitter.close(resolve)
  })
}

async function getEmitter (dbopts, emitter) {
  if (emitter) return emitter
  const mqEmitter = mqemitterMongo(dbopts)
  await once(mqEmitter.status, 'stream')
  return mqEmitter
}

async function cleanDB () {
  const mongoClient = new MongoClient(mongourl, { w: 1 })
  const oldDB = mongoClient.db(dbname)
  await oldDB.dropDatabase()
  await mongoClient.close()
}

async function createDB () {
  await cleanDB()
  const mongoClient = new MongoClient(mongourl, { w: 1 })
  const db = mongoClient.db(dbname)
  await db.admin().command({ setParameter: 1, ttlMonitorSleepSecs: 2 })
  return { mongoClient, db }
}

function getClient (p) {
  return p.instance.instance.asyncPersistence._mongoDBclient
}

function getDB (p) {
  return p.instance.instance.asyncPersistence._db
}

function toBroker (id, emitter) {
  const eventEmitter = new EventEmitter()
  return {
    id,
    publish: emitter.emit.bind(emitter),
    subscribe: emitter.on.bind(emitter),
    unsubscribe: emitter.removeListener.bind(emitter),
    on: eventEmitter.on.bind(eventEmitter),
    emit: eventEmitter.emit.bind(eventEmitter)
  }
}

async function setUpPersistence (t, id, dbopts, emitter) {
  const mqEmitter = await getEmitter(dbopts, emitter)
  const instance = persistence(dbopts)
  instance.broker = toBroker(id, mqEmitter)
  if (!instance.ready) {
    await once(instance, 'ready')
  }
  t.diagnostic(`instance ${id} created`)
  const p = new PromisifiedPersistence(instance)
  return { instance: p, emitter: mqEmitter, id }
}

async function cleanUpPersistence (t, { instance, emitter, id }) {
  await instance.destroy()
  await closeEmitter(emitter)
  t.diagnostic(`instance ${id} destroyed`)
}
// end of helpers

// Testing starts here.
async function doTest () {
  const defaultDBopts = {
    url: mongourl
  }

  test('Can connect to mongoDB', async (t) => {
    t.plan(1)
    const { mongoClient } = await createDB()
    t.assert.ok(mongoClient, 'Can connect to MongoDB')
    await mongoClient.close()
  })

  test('multiple persistences', async (t) => {
    t.plan(1)
    await cleanDB()

    const p1 = await setUpPersistence(t, '1', defaultDBopts)
    const p2 = await setUpPersistence(t, '2', defaultDBopts)

    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 1
    }, {
      topic: 'hello/#',
      qos: 1
    }, {
      topic: 'matteo',
      qos: 1
    }]

    await p1.instance.addSubscriptions(client, subs)
    await sleep(100)
    const resubs = await p2.instance.subscriptionsByTopic('hello')
    t.assert.deepEqual(resubs, [{
      clientId: client.id,
      topic: 'hello/#',
      qos: 1,
      rh: undefined,
      rap: undefined,
      nl: undefined
    }, {
      clientId: client.id,
      topic: 'hello',
      qos: 1,
      rh: undefined,
      rap: undefined,
      nl: undefined
    }])
    await cleanUpPersistence(t, p1)
    await cleanUpPersistence(t, p2)
  })

  test('multiple persistences with passed db object and url', async (t) => {
    t.plan(1)
    const { mongoClient, db } = await createDB()
    const dboptsWithDbObjectAndUrl = {
      url: mongourl,
      db
    }
    const p1 = await setUpPersistence(t, '1', dboptsWithDbObjectAndUrl)
    const p2 = await setUpPersistence(t, '2', dboptsWithDbObjectAndUrl)
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 1
    }, {
      topic: 'hello/#',
      qos: 1
    }, {
      topic: 'matteo',
      qos: 1
    }]
    await p1.instance.addSubscriptions(client, subs)
    await sleep(200)
    const resubs = await p2.instance.subscriptionsByTopic('hello')
    t.assert.deepEqual(resubs, [{
      clientId: client.id,
      topic: 'hello/#',
      qos: 1,
      rh: undefined,
      rap: undefined,
      nl: undefined
    }, {
      clientId: client.id,
      topic: 'hello',
      qos: 1,
      rh: undefined,
      rap: undefined,
      nl: undefined
    }], 'subscriptions done by p1 can be found by p2')
    await cleanUpPersistence(t, p1)
    await cleanUpPersistence(t, p2)
    await mongoClient.close()
  })

  test('multiple persistences with passed only db object', async (t) => {
    t.plan(1)
    const { mongoClient, db } = await createDB()

    const dboptsWithOnlyDbObject = {
      db
    }
    const p1 = await setUpPersistence(t, '1', dboptsWithOnlyDbObject)
    const p2 = await setUpPersistence(t, '2', dboptsWithOnlyDbObject)
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 1
    }, {
      topic: 'hello/#',
      qos: 1
    }, {
      topic: 'matteo',
      qos: 1
    }]
    await p1.instance.addSubscriptions(client, subs)
    await sleep(100)
    const resubs = await p2.instance.subscriptionsByTopic('hello')
    t.assert.deepEqual(resubs, [{
      clientId: client.id,
      topic: 'hello/#',
      qos: 1,
      rh: undefined,
      rap: undefined,
      nl: undefined
    }, {
      clientId: client.id,
      topic: 'hello',
      qos: 1,
      rh: undefined,
      rap: undefined,
      nl: undefined
    }], 'subscriptions done by p1 can be found by p2')
    await cleanUpPersistence(t, p1)
    await cleanUpPersistence(t, p2)
    await mongoClient.close()
  })

  test('qos 0 subs restoration', async (t) => {
    t.plan(1)
    await cleanDB()
    const p1 = await setUpPersistence(t, '1', defaultDBopts)
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 0
    }]
    await p1.instance.addSubscriptions(client, subs)
    await cleanUpPersistence(t, p1)
    const p2 = await setUpPersistence(t, '2', defaultDBopts, p1.emitter)
    const resubs = await p2.instance.subscriptionsByTopic('hello')
    t.assert.deepEqual(resubs, [{
      clientId: 'abcde',
      topic: 'hello',
      qos: 0,
      rh: undefined,
      rap: undefined,
      nl: undefined
    }])
    await cleanUpPersistence(t, p2)
  })

  test('look up for expire after seconds index', async (t) => {
    t.plan(5)
    await cleanDB()
    const dbopts = structuredClone(defaultDBopts)
    dbopts.ttl = {
      packets: 1,
      subscriptions: 1
    }
    dbopts.ttlAfterDisconnected = true

    const p1 = await setUpPersistence(t, '1', dbopts)
    const db = getDB(p1)
    const indexes1 = await db.collection('subscriptions').indexInformation({ full: true })
    const index1 = indexes1.find(index => index.name === 'ttl')
    t.assert.deepEqual(index1.key, { disconnected: 1 }, 'must return the index key')

    const indexes2 = await db.collection('incoming').indexInformation({ full: true })
    const index2 = indexes2.find(index => index.name === 'ttl')
    t.assert.deepEqual(index2.key, { 'packet.added': 1 }, 'must return the index key')

    const indexes3 = await db.collection('outgoing').indexInformation({ full: true })
    const index3 = indexes3.find(index => index.name === 'ttl')
    t.assert.deepEqual(index3.key, { 'packet.added': 1 }, 'must return the index key')

    const indexes4 = await db.collection('will').indexInformation({ full: true })
    const index4 = indexes4.find(index => index.name === 'ttl')
    t.assert.deepEqual(index4.key, { 'packet.added': 1 }, 'must return the index key')

    const indexes5 = await db.collection('subscriptions').indexInformation({ full: true })
    const index5 = indexes5.find(index => index.name === 'ttl')
    t.assert.deepEqual(index5.key, { disconnected: 1 }, 'must return the index key')

    await cleanUpPersistence(t, p1)
  })

  test('look up for query indexes', async (t) => {
    t.plan(4)
    await cleanDB()

    const dbopts = structuredClone(defaultDBopts)
    dbopts.ttl = {
      packets: 1,
      subscriptions: 1
    }

    const expectedMsgIdx = { clientId: 1, 'packet.messageId': 1 }
    const expectedBrokerIdx = { clientId: 1, 'packet.brokerId': 1, 'packet.brokerCounter': 1 }

    const p1 = await setUpPersistence(t, '1', dbopts)
    const db = getDB(p1)
    const indexes1 = await db.collection('incoming').indexInformation({ full: true })
    const messageIdIndex1 = indexes1.find(index => index.name === 'query_clientId_messageId')
    const brokerIdIndex1 = indexes1.find(index => index.name === 'query_clientId_brokerId')
    t.assert.deepEqual(expectedMsgIdx, messageIdIndex1.key, 'must return the index key')
    t.assert.deepEqual(expectedBrokerIdx, brokerIdIndex1.key, 'must return the index key')

    const indexes2 = await db.collection('outgoing').indexInformation({ full: true })
    const messageIdIndex2 = indexes2.find(index => index.name === 'query_clientId_messageId')
    const brokerIdIndex2 = indexes2.find(index => index.name === 'query_clientId_brokerId')
    t.assert.deepEqual(expectedMsgIdx, messageIdIndex2.key, 'must return the index key')
    t.assert.deepEqual(expectedBrokerIdx, brokerIdIndex2.key, 'must return the index key')

    await cleanUpPersistence(t, p1)
  })

  test('look up for packet with added property', async (t) => {
    t.plan(1)
    await cleanDB()

    const dbopts = structuredClone(defaultDBopts)
    dbopts.ttl = {
      packets: 1,
      subscriptions: 1
    }

    const p1 = await setUpPersistence(t, '1', dbopts)

    const date = new Date()
    const packet = {
      cmd: 'publish',
      id: p1.instance.broker.id,
      topic: 'hello/world',
      payload: Buffer.from('muahah'),
      qos: 0,
      retain: true,
      added: date
    }
    await p1.instance.storeRetained(packet)
    const db = getDB(p1)
    // exclude _id attribute from result
    const { _id, ...result } = await db.collection('retained').findOne({ topic: 'hello/world' })
    result.payload = result.payload.buffer
    t.assert.deepEqual(packet, result, 'must return the packet')

    await cleanUpPersistence(t, p1)
  })

  test('drop existing indexes', async (t) => {
    t.plan(10)
    async function checkIndexes (db, shouldExist) {
      const collections = await db.collections()
      if (collections.length === 0) {
        return
      }
      for (let i = 0; i < collections.length; i++) {
        const exists = await collections[i].indexExists('ttl')
        if (collections[i].namespace.indexOf('pubsub') < 0) { // pubsub is the collection created by mqemitter-mongodb
          const msg = `Index on ${collections[i].namespace} should${shouldExist ? '' : ' not'} exist`
          t.assert.equal(shouldExist, exists, msg)
        }
      }
    }

    await cleanDB()
    const dbopts = structuredClone(defaultDBopts)
    dbopts.ttl = {
      packets: 1,
      subscriptions: 1
    }
    const p1 = await setUpPersistence(t, '1', dbopts)
    const db1 = getDB(p1)

    await checkIndexes(db1, true)
    await cleanUpPersistence(t, p1)

    dbopts.ttl = undefined
    dbopts.dropExistingIndexes = true

    const p2 = await setUpPersistence(t, '2', dbopts)
    const db2 = getDB(p2)
    await checkIndexes(db2, false)
    await cleanUpPersistence(t, p2)
  })

  test('look up for expired packets', async (t) => {
    await cleanDB()
    const dbopts = structuredClone(defaultDBopts)
    dbopts.ttl = {
      packets: 1,
      subscriptions: 1
    }
    const p1 = await setUpPersistence(t, '1', dbopts)
    const date = new Date()
    const packet = {
      cmd: 'publish',
      id: p1.instance.broker.id,
      topic: 'hello/world',
      payload: Buffer.from('muahah'),
      qos: 0,
      retain: true,
      added: date
    }

    const client = { clientId: 'client1' }

    await p1.instance.storeRetained(packet)
    await p1.instance.incomingStorePacket(client, packet)
    await p1.instance.outgoingEnqueue(client, packet)
    await p1.instance.putWill(client, packet)

    // wait for delete
    await sleep(4000) // https://docs.mongodb.com/manual/core/index-ttl/#timing-of-the-delete-operation
    const db = getDB(p1)
    const result1 = await db.collection('retained').findOne({ topic: 'hello/world' })
    t.assert.equal(result1, null, 'retained must return empty packet')
    const result2 = await db.collection('incoming').findOne({ topic: 'hello/world' })
    t.assert.equal(result2, null, 'incoming must return empty packet')
    const result3 = await db.collection('outgoing').findOne({ topic: 'hello/world' })
    t.assert.equal(result3, null, 'outgoing must return empty packet')
    const result4 = await db.collection('will').findOne({ topic: 'hello/world' })
    t.assert.equal(result4, null, 'will must return empty packet')
    await cleanUpPersistence(t, p1)
  })

  test('should pass mongoOptions to mongodb driver', async (t) => {
    t.plan(1)

    const dboptsWithUrlMongoOptions = {
      url: mongourl,
      mongoOptions: {
        appName: 'aedes-persistence-mongodb' // must be a valid mongo option
      }
    }

    const p1 = await setUpPersistence(t, '1', dboptsWithUrlMongoOptions)
    const client = getClient(p1)
    const appName = client.options.appName
    t.assert.equal(appName, dboptsWithUrlMongoOptions.mongoOptions.appName, 'must pass options to mongodb')
    await cleanUpPersistence(t, p1)
  })

  test('subscription should expire after client disconnected', async (t) => {
    t.plan(5)

    await cleanDB()
    const dbopts = structuredClone(defaultDBopts)
    dbopts.ttl = {
      subscriptions: 1
    }
    dbopts.ttlAfterDisconnected = true
    const client = { id: 'client1' }
    const filter = { clientId: client.id, topic: 'hello' }
    const subs = [{ topic: 'hello', qos: 1 }]

    const p1 = await setUpPersistence(t, '1', dbopts)
    await p1.instance.addSubscriptions(client, subs)
    const db = getDB(p1)
    const result1 = db.collection('subscriptions').findOne(filter)
    t.assert.notEqual(result1, null, 'must return subscription')
    t.assert.equal(result1.disconnected, undefined, 'disconnected should not be set')
    p1.instance.broker.emit('clientDisconnect', client)
    await sleep(500)
    const result2 = await db.collection('subscriptions').findOne(filter)
    t.assert.notEqual(result2, null, 'must return subscription')
    t.assert.notEqual(result2.disconnected, undefined, 'disconnected should be set')
    await sleep(3000)
    const result3 = await db.collection('subscriptions').findOne(filter)
    t.assert.equal(result3, null, 'must not return subscription')
    await cleanUpPersistence(t, p1)
  })

  test('prevent executing bulk when instance is destroyed', async (t) => {
    t.plan(1)
    await cleanDB()
    const p1 = await setUpPersistence(t, '1', defaultDBopts)
    const instance = p1.instance
    await cleanUpPersistence(t, p1)

    const packet = {
      cmd: 'publish',
      id: instance.broker.id,
      topic: 'hello/world',
      payload: Buffer.from('muahah'),
      qos: 0,
      retain: true
    }
    await instance.storeRetained(packet)
    t.assert.ok(true, 'should not have thrown')
  })
}
doTest()
