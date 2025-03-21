const test = require('node:test')
const { EventEmitter } = require('node:events')
const persistence = require('./')
const { MongoClient } = require('mongodb')
const abs = require('aedes-cached-persistence/abstract')
const mqemitterMongo = require('mqemitter-mongodb')
const dbname = 'aedes-test'
const mongourl = `mongodb://127.0.0.1/${dbname}`

function sleep (msec) {
  return new Promise(resolve => setTimeout(resolve, msec))
}

function waitForEventOnce (emitter, eventName, errorEvent) {
  return new Promise((resolve, reject) => {
    emitter.once(eventName, resolve)
    if (errorEvent) {
      emitter.once(errorEvent, reject)
    }
  })
}

// promisified versions of the instance methods
// to avoid deep callbacks while testing
async function addSubscriptions (instance, client, subs) {
  return new Promise((resolve, reject) => {
    instance.addSubscriptions(client, subs, (err, reClient) => {
      if (err) {
        reject(err)
      } else {
        resolve(reClient)
      }
    })
  })
}

async function subscriptionsByTopic (instance, topic) {
  return new Promise((resolve, reject) => {
    instance.subscriptionsByTopic(topic, (err, resubs) => {
      if (err) {
        reject(err)
      } else {
        resolve(resubs)
      }
    })
  })
}

function storeRetained (instance, packet) {
  return new Promise((resolve, reject) => {
    instance.storeRetained(packet, err => {
      if (err) {
        reject(err)
      } else {
        resolve()
      }
    })
  })
}

async function incomingStorePacket (instance, client, packet) {
  return new Promise((resolve, reject) => {
    instance.incomingStorePacket(client, packet, err => {
      if (err) {
        reject(err)
      } else {
        resolve()
      }
    })
  })
}

async function putWill (instance, client, packet) {
  return new Promise((resolve, reject) => {
    instance.putWill(client, packet, (err, reClient) => {
      if (err) {
        reject(err)
      } else {
        resolve(reClient)
      }
    })
  })
}

async function outgoingEnqueue (instance, sub, packet) {
  return new Promise((resolve, reject) => {
    instance.outgoingEnqueue(sub, packet, err => {
      if (err) {
        reject(err)
      } else {
        resolve()
      }
    })
  })
}

// helpers

async function closeEmitter (emitter) {
  return new Promise((resolve) => {
    emitter.close(resolve)
  })
}

async function destroyInstance (instance) {
  return new Promise((resolve) => {
    instance.destroy(resolve)
  })
}

async function getEmitter (emitter, dbopts) {
  if (emitter) return emitter
  const myEmitter = emitter || mqemitterMongo(dbopts)
  await waitForEventOnce(myEmitter.status, 'stream')
  return myEmitter
}

// end of helpers

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

  async function cleanDB () {
    await Promise.all(collections.map((c) => c.deleteMany({})))
  }

  // set ttl task to run every 2 seconds
  await db.admin().command({ setParameter: 1, ttlMonitorSleepSecs: 2 })
  await cleanDB()
  await runTest(mongoClient, db)

  function makeBuildEmitter (dbopts) {
    return function buildEmitter () {
      const emitter = mqemitterMongo(dbopts)
      return emitter
    }
  }

  function makePersistence (dbopts) {
    return async function build (cb) {
      await cleanDB()
      const instance = persistence(dbopts)
      // make intance.destroy close the broker as well
      const oldDestroy = instance.destroy
      instance.destroy = (cb) => {
        oldDestroy(() => {
          instance.broker.mq.close(cb)
        })
      }
      return instance
    }
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
    const myEmitter = await getEmitter(emitter, dbopts)
    const instance = persistence(dbopts)
    instance.broker = toBroker(id, myEmitter)
    await waitForEventOnce(instance, 'ready')
    t.diagnostic(`instance ${id} created`)
    return { instance, emitter: myEmitter, id }
  }

  async function cleanUpPersistence (t, { instance, emitter, id }) {
    await destroyInstance(instance)
    await closeEmitter(emitter)
    t.diagnostic(`instance ${id} destroyed`)
  }

  function runTest (client, db) {
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

    // and the rest of the tests
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

      await addSubscriptions(p1.instance, client, subs)
      await sleep(100)
      const resubs = await subscriptionsByTopic(p2.instance, 'hello')
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
      await cleanDB()
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
      await addSubscriptions(p1.instance, client, subs)
      sleep(100)
      const resubs = subscriptionsByTopic(p2.instance, 'hello')
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

    test('multiple persistences with passed only db object', async (t) => {
      t.plan(1)
      await cleanDB()

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
      await addSubscriptions(p1.instance, client, subs)
      sleep(100)
      const resubs = subscriptionsByTopic(p2.instance, 'hello')
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

    test('qos 0 subs restoration', async (t) => {
      t.plan(1)
      await cleanDB()
      const p1 = await setUpPersistence(t, '1', defaultDBopts)
      const client = { id: 'abcde' }
      const subs = [{
        topic: 'hello',
        qos: 0
      }]
      await addSubscriptions(p1.instance, client, subs)
      await cleanUpPersistence(t, p1)
      const p2 = await setUpPersistence(t, '2', defaultDBopts, p1.emitter)
      const resubs = subscriptionsByTopic(p2.instance, 'hello')
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

      const indexes1 = await db.collection('subscriptions').indexInformation({ full: true })
      const index1 = indexes1.find(index => index.name === 'ttl')
      t.assert.deepEqual({ added: 1 }, index1.key, 'must return the index key')

      const indexes2 = await db.collection('incoming').indexInformation({ full: true })
      const index2 = indexes2.find(index => index.name === 'ttl')
      t.assert.deepEqual({ 'packet.added': 1 }, index2.key, 'must return the index key')

      const indexes3 = await db.collection('outgoing').indexInformation({ full: true })
      const index3 = indexes3.find(index => index.name === 'ttl')
      t.assert.deepEqual({ 'packet.added': 1 }, index3.key, 'must return the index key')

      const indexes4 = await db.collection('will').indexInformation({ full: true })
      const index4 = indexes4.find(index => index.name === 'ttl')
      t.assert.deepEqual({ 'packet.added': 1 }, index4.key, 'must return the index key')

      const indexes5 = await db.collection('subscriptions').indexInformation({ full: true })
      const index5 = indexes5.find(index => index.name === 'ttl')
      t.assert.deepEqual({ disconnected: 1 }, index5.key, 'must return the index key')

      cleanUpPersistence(t, p1)
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

      cleanUpPersistence(t, p1)
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
      await storeRetained(p1.instance, packet)
      // exclude _id attribute from result
      const result = await db.collection('retained').findOne({ topic: 'hello/world' }, { _id: false })
      result.payload = result.payload.buffer
      t.assert.deepEqual(packet, result, 'must return the packet')

      await cleanUpPersistence(t, p1)
    })

    test('drop existing indexes', async (t) => {
      t.plan(2)
      async function checkIndexes (shouldExist) {
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
      await checkIndexes(true)
      await cleanUpPersistence(t, p1)

      dbopts.ttl = undefined
      dbopts.dropExistingIndexes = true

      const p2 = await setUpPersistence(t, '2', dbopts)
      await checkIndexes(false)
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

      await storeRetained(p1.instance, packet)
      await incomingStorePacket(p1.instance, client, packet)
      await outgoingEnqueue(p1.instance, client, packet)
      await putWill(p1.instance, client, packet)

      // wait for delete
      sleep(4000) // https://docs.mongodb.com/manual/core/index-ttl/#timing-of-the-delete-operation
      const result1 = await db.collection('retained').findOne({ topic: 'hello/world' })
      t.assert.equal(null, result1, 'must return empty packet')
      const result2 = await db.collection('incoming').findOne({ topic: 'hello/world' })
      t.assert.equal(null, result2, 'must return empty packet')
      const result3 = db.collection('outgoing').findOne({ topic: 'hello/world' })
      t.assert.equal(null, result3, 'must return empty packet')
      const result4 = db.collection('will').findOne({ topic: 'hello/world' })
      t.assert.equal(null, result4, 'must return empty packet')
      await cleanUpPersistence(t, p1)
    })

    test('should pass mongoOptions to mongodb driver', async (t) => {
      t.plan(1)

      const dboptsWithUrlMongoOptions = {
        url: mongourl,
        mongoOptions: {
          raw: true // must be a valid mongo option
        }
      }

      const instance = persistence(dboptsWithUrlMongoOptions)
      t.assert.equal(dboptsWithUrlMongoOptions.mongoOptions.raw, client.s.options.raw, 'must pass options to mongodb')
      await destroyInstance(instance)
    })

    test('subscription should expire after client disconnected', async (t) => {
      t.plan(5)

      const dbopts = structuredClone(defaultDBopts)
      dbopts.ttl = {
        subscriptions: 1
      }
      dbopts.ttlAfterDisconnected = true
      const client = { id: 'client1' }
      const filter = { clientId: client.id, topic: 'hello' }

      const p1 = await setUpPersistence(t, '1', dbopts)
      await addSubscriptions(p1.instance, client)
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
      await cleanUpPersistence(p1)

      const packet = {
        cmd: 'publish',
        id: instance.broker.id,
        topic: 'hello/world',
        payload: Buffer.from('muahah'),
        qos: 0,
        retain: true
      }
      await storeRetained(instance, packet)
      t.assert.ok(true, 'should not have thrown')
    })
  }
}
doTest()
