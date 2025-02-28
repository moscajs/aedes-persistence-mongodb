const test = require('node:test')
const assert = require('node:assert/strict')
const { EventEmitter } = require('events')
const persistence = require('./')
const { MongoClient } = require('mongodb')
const abs = require('aedes-cached-persistence/abstract')
const mqemitterMongo = require('mqemitter-mongodb')
const dbname = 'aedes-test'
const mongourl = 'mongodb://127.0.0.1/' + dbname
let clean = null

function sleep (sec) {
  return new Promise(resolve => setTimeout(resolve, sec * 1000))
}

MongoClient.connect(mongourl, { useNewUrlParser: true, useUnifiedTopology: true, w: 1 }, (err, client) => {
  if (err) {
    throw err
  }

  const db = client.db(dbname)

  const collections = [
    db.collection('subscriptions'),
    db.collection('retained'),
    db.collection('will'),
    db.collection('outgoing'),
    db.collection('incoming')
  ]

  clean = async (cb) => {
    await Promise.all(collections.map((c) => c.deleteMany({})))
    cb()
  }

  // set ttl task to run every 2 seconds
  db.admin().command({ setParameter: 1, ttlMonitorSleepSecs: 2 }, (err) => {
    if (err) {
      throw err
    }

    clean((err) => {
      if (err) {
        throw err
      }

      runTest(client, db)
    })
  })
})

function makeBuildEmitter (dbopts) {
  return function buildEmitter () {
    const emitter = mqemitterMongo(dbopts)
    return emitter
  }
}

function makePersistence (dbopts) {
  return function build (cb) {
    clean((err) => {
      if (err) {
        return cb(err)
      }

      const instance = persistence(dbopts)

      const oldDestroy = instance.destroy

      instance.destroy = (cb) => {
        instance.destroy = oldDestroy
        instance.destroy(() => {
          instance.broker.mq.close(() => {
            cb()
          })
        })
      }

      cb(null, instance)
    })
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

function runTest (client, db) {
  const dbopts = {
    url: mongourl
  }
  // run all tests from aedes-abstract-persistence
  abs({
    test,
    buildEmitter: makeBuildEmitter(dbopts),
    persistence: makePersistence(dbopts),
    waitForReady: true,
  })

  test('multiple persistences', (t) => {
    clean((err) => {
      assert.ifError(err)

      const emitter = mqemitterMongo(dbopts)

      emitter.status.once('stream', () => {
        t.diagnostic('mqemitter 1 ready')

        const emitter2 = mqemitterMongo(dbopts)

        emitter2.status.once('stream', () => {
          t.diagnostic('mqemitter 2 ready')

          const instance = persistence(dbopts)
          instance.broker = toBroker('1', emitter)

          instance.on('ready', () => {
            t.diagnostic('instance ready')

            const instance2 = persistence(dbopts)
            instance2.broker = toBroker('2', emitter2)

            instance2.on('ready', () => {
              t.diagnostic('instance2 ready')

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

              instance.addSubscriptions(client, subs, (err) => {
                assert.ok(!err, 'no error')
                setTimeout(() => {
                  instance2.subscriptionsByTopic('hello', (err, resubs) => {
                    assert.ok(!err, 'no error')
                    assert.deepEqual(resubs, [{
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
                    instance.destroy(() => {
                      t.diagnostic('first dies')
                      emitter.close(() => {
                        t.diagnostic('first emitter dies')
                        instance2.destroy(() => {
                          t.diagnostic('second dies')
                          emitter2.close(t.diagnostic('second emitter dies'))
                        })
                      })
                    })
                  })
                }, 100)
              })
            })
          })
        })
      })
    })
  })

  const dboptsWithDbObjectAndUrl = {
    url: mongourl,
    db
  }

  test('multiple persistences with passed db object and url', (t) => {
    clean((err) => {
      assert.ifError(err)

      const emitter = mqemitterMongo(dboptsWithDbObjectAndUrl)

      emitter.status.once('stream', () => {
        t.diagnostic('mqemitter 1 ready')

        const emitter2 = mqemitterMongo(dboptsWithDbObjectAndUrl)

        emitter2.status.once('stream', () => {
          t.diagnostic('mqemitter 2 ready')

          const instance = persistence(dboptsWithDbObjectAndUrl)
          instance.broker = toBroker('1', emitter)

          instance.on('ready', () => {
            t.diagnostic('instance ready')

            const instance2 = persistence(dboptsWithDbObjectAndUrl)
            instance2.broker = toBroker('2', emitter2)

            instance2.on('ready', () => {
              t.diagnostic('instance2 ready')

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

              instance.addSubscriptions(client, subs, (err) => {
                assert.ok(!err, 'no error')
                setTimeout(() => {
                  instance2.subscriptionsByTopic('hello', (err, resubs) => {
                    assert.ok(!err, 'no error')
                    assert.deepEqual(resubs, [{
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
                    instance.destroy(() => {
                      t.diagnostic('first dies')
                      emitter.close(() => {
                        t.diagnostic('first emitter dies')
                        instance2.destroy(() => {
                          t.diagnostic('second dies')
                          emitter2.close(t.diagnostic('second emitter dies'))
                        })
                      })
                    })
                  })
                }, 100)
              })
            })
          })
        })
      })
    })
  })

  const dboptsWithOnlyDbObject = {
    db
  }

  test('multiple persistences with passed only db object', (t) => {
    clean((err) => {
      assert.ifError(err)

      const emitter = mqemitterMongo(dboptsWithOnlyDbObject)

      emitter.status.once('stream', () => {
        t.diagnostic('mqemitter 1 ready')

        const emitter2 = mqemitterMongo(dboptsWithOnlyDbObject)

        emitter2.status.once('stream', () => {
          t.diagnostic('mqemitter 2 ready')

          const instance = persistence(dboptsWithOnlyDbObject)
          instance.broker = toBroker('1', emitter)

          instance.on('ready', () => {
            t.diagnostic('instance ready')

            const instance2 = persistence(dboptsWithOnlyDbObject)
            instance2.broker = toBroker('2', emitter2)

            instance2.on('ready', () => {
              t.diagnostic('instance2 ready')

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

              instance.addSubscriptions(client, subs, (err) => {
                assert.ok(!err, 'no error')
                setTimeout(() => {
                  instance2.subscriptionsByTopic('hello', (err, resubs) => {
                    assert.ok(!err, 'no error')
                    assert.deepEqual(resubs, [{
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
                    instance.destroy(() => {
                      t.diagnostic('first dies')
                      emitter.close(() => {
                        t.diagnostic('first emitter dies')
                        instance2.destroy(() => {
                          t.diagnostic('second dies')
                          emitter2.close(t.diagnostic('second emitter dies'))
                        })
                      })
                    })
                  })
                }, 100)
              })
            })
          })
        })
      })
    })
  })

  test('qos 0 subs restoration', (t) => {
    clean((err) => {
      assert.ifError(err)

      const emitter = mqemitterMongo(dbopts)

      emitter.status.on('stream', () => {
        t.diagnostic('mqemitter 1 ready')
        const instance = persistence(dbopts)
        instance.broker = toBroker('1', emitter)

        instance.on('ready', () => {
          t.diagnostic('instance ready')
          const client = { id: 'abcde' }
          const subs = [{
            topic: 'hello',
            qos: 0
          }]

          instance.addSubscriptions(client, subs, (err, client) => {
            assert.ok(!err, 'no error')

            instance.destroy(t.diagnostic('first dies'))
            emitter.close(t.diagnostic('first emitter dies'))

            const instance2 = persistence(dbopts)
            instance2.broker = toBroker('1', emitter)

            instance2.on('ready', () => {
              t.diagnostic('instance ready')
              instance2.subscriptionsByTopic('hello', (err, resubs) => {
                assert.ok(!err, 'should not err')
                assert.deepEqual(resubs, [{
                  clientId: 'abcde',
                  topic: 'hello',
                  qos: 0,
                  rh: undefined,
                  rap: undefined,
                  nl: undefined
                }])
                instance2.destroy(t.diagnostic('second dies'))
              })
            })
          })
        })
      })
    })
  })

  test('look up for expire after seconds index', (t) => {
    clean((err) => {
      assert.ifError(err)

      dbopts.ttl = {
        packets: 1,
        subscriptions: 1
      }
      dbopts.ttlAfterDisconnected = true
      const emitter = mqemitterMongo(dbopts)

      emitter.status.on('stream', () => {
        t.diagnostic('mqemitter ready')
        const instance = persistence(dbopts)
        instance.broker = toBroker('1', emitter)

        instance.on('ready', () => {
          t.diagnostic('instance ready')

          db.collection('retained').indexInformation({ full: true }, (err, indexes) => {
            assert.ok(!err, 'no error')
            const index = indexes.find(index => index.name === 'ttl')
            assert.deepEqual({ added: 1 }, index.key, 'must return the index key')

            db.collection('incoming').indexInformation({ full: true }, (err, indexes) => {
              assert.ok(!err, 'no error')
              const index = indexes.find(index => index.name === 'ttl')
              assert.deepEqual({ 'packet.added': 1 }, index.key, 'must return the index key')

              db.collection('outgoing').indexInformation({ full: true }, (err, indexes) => {
                assert.ok(!err, 'no error')
                const index = indexes.find(index => index.name === 'ttl')
                assert.deepEqual({ 'packet.added': 1 }, index.key, 'must return the index key')

                db.collection('will').indexInformation({ full: true }, (err, indexes) => {
                  assert.ok(!err, 'no error')
                  const index = indexes.find(index => index.name === 'ttl')
                  assert.deepEqual({ 'packet.added': 1 }, index.key, 'must return the index key')

                  db.collection('subscriptions').indexInformation({ full: true }, (err, indexes) => {
                    assert.ok(!err, 'no error')
                    const index = indexes.find(index => index.name === 'ttl')
                    assert.deepEqual({ disconnected: 1 }, index.key, 'must return the index key')

                    instance.destroy(() => {
                      t.diagnostic('Instance dies')
                      emitter.close()
                    })
                  })
                })
              })
            })
          })
        })
      })
    })
  })

  test('look up for query indexes', (t) => {
    clean((err) => {
      assert.ifError(err)

      dbopts.ttl = {
        packets: 1,
        subscriptions: 1
      }
      const emitter = mqemitterMongo(dbopts)

      emitter.status.on('stream', () => {
        t.diagnostic('mqemitter ready')
        const instance = persistence(dbopts)
        instance.broker = toBroker('1', emitter)

        instance.on('ready', () => {
          t.diagnostic('instance ready')

          db.collection('incoming').indexInformation({ full: true }, (err, indexes) => {
            assert.ok(!err, 'no error')
            const messageIdIndex = indexes.find(index => index.name === 'query_clientId_messageId')
            const brokerIdIndex = indexes.find(index => index.name === 'query_clientId_brokerId')
            assert.deepEqual(
              { clientId: 1, 'packet.messageId': 1 },
              messageIdIndex.key, 'must return the index key'
            )
            assert.deepEqual(
              { clientId: 1, 'packet.brokerId': 1, 'packet.brokerCounter': 1 },
              brokerIdIndex.key, 'must return the index key'
            )

            db.collection('outgoing').indexInformation({ full: true }, (err, indexes) => {
              assert.ok(!err, 'no error')
              const messageIdIndex = indexes.find(index => index.name === 'query_clientId_messageId')
              const brokerIdIndex = indexes.find(index => index.name === 'query_clientId_brokerId')
              assert.deepEqual(
                { clientId: 1, 'packet.messageId': 1 },
                messageIdIndex.key, 'must return the index key'
              )
              assert.deepEqual(
                { clientId: 1, 'packet.brokerId': 1, 'packet.brokerCounter': 1 },
                brokerIdIndex.key, 'must return the index key'
              )

              instance.destroy(() => {
                t.diagnostic('Instance dies')
                emitter.close()
              })
            })
          })
        })
      })
    })
  })

  test('look up for packet with added property', (t) => {
    clean((err) => {
      assert.ifError(err)

      dbopts.ttl = {
        packets: 1,
        subscriptions: 1
      }
      const emitter = mqemitterMongo(dbopts)

      emitter.status.on('stream', () => {
        t.diagnostic('mqemitter ready')
        const instance = persistence(dbopts)
        instance.broker = toBroker('2', emitter)

        instance.on('ready', () => {
          t.diagnostic('instance ready')

          const date = new Date()
          const packet = {
            cmd: 'publish',
            id: instance.broker.id,
            topic: 'hello/world',
            payload: Buffer.from('muahah'),
            qos: 0,
            retain: true,
            added: date
          }

          instance.storeRetained(packet, (err) => {
            assert.ok(!err, 'no error')

            db.collection('retained').findOne({ topic: 'hello/world' }, (err, result) => {
              assert.ok(!err, 'no error')
              delete result._id
              result.payload = result.payload.buffer
              assert.deepEqual(packet, result, 'must return the packet')
              instance.destroy(() => {
                t.diagnostic('Instance dies')
                emitter.close()
              })
            })
          })
        })
      })
    })
  })

  test('drop existing indexes', (t) => {
    function checkIndexes (shouldExist, cb) {
      db.collections((err, collections) => {
        assert.ok(!err, 'no error')
        if (collections.length === 0) {
          cb()
        } else {
          let done = 0
          for (let i = 0; i < collections.length; i++) {
            collections[i].indexExists('ttl', (err, exists) => {
              assert.ok(!err, 'no error')
              if (collections[i].namespace.indexOf('pubsub') < 0) { // pubsub is the collection created by mqemitter-mongodb
                assert.equal(shouldExist, exists, 'Index on ' + collections[i].namespace + ' should' + (shouldExist ? '' : ' not') + ' exist')
              }
              if (++done >= collections.length) {
                cb()
              }
            })
          }
        }
      })
    }

    clean((err) => {
      assert.ok(!err, 'no error')

      dbopts.ttl = {
        packets: 1,
        subscriptions: 1
      }
      const emitter = mqemitterMongo(dbopts)

      emitter.status.on('stream', () => {
        t.diagnostic('mqemitter ready')

        const instance = persistence(dbopts)
        instance.broker = toBroker('1', emitter)

        instance.on('ready', () => {
          t.diagnostic('instance ready')

          checkIndexes(true, () => {
            delete dbopts.ttl
            dbopts.dropExistingIndexes = true

            instance.destroy(t.diagnostic('first instance dies'))
            emitter.close(t.diagnostic('first emitter dies'))

            const emitter2 = mqemitterMongo(dbopts)

            emitter2.status.on('stream', () => {
              t.diagnostic('mqemitter ready')

              const instance2 = persistence(dbopts)
              instance2.broker = toBroker('2', emitter2)

              instance2.on('ready', () => {
                t.diagnostic('instance ready')
                checkIndexes(false, () => {
                  instance2.destroy(t.diagnostic('second instance dies'))
                  emitter2.close()
                })
              })
            })
          })
        })
      })
    })
  })

  test('look up for expired packets', (t) => {
    clean((err) => {
      assert.ifError(err)

      dbopts.ttl = {
        packets: 1,
        subscriptions: 1
      }
      const emitter = mqemitterMongo(dbopts)

      emitter.status.on('stream', () => {
        t.diagnostic('mqemitter ready')
        const instance = persistence(dbopts)
        instance.broker = toBroker('1', emitter)

        instance.on('ready', () => {
          t.diagnostic('instance ready')

          const date = new Date()
          const packet = {
            cmd: 'publish',
            id: instance.broker.id,
            topic: 'hello/world',
            payload: Buffer.from('muahah'),
            qos: 0,
            retain: true,
            added: date
          }

          function checkExpired () {
            db.collection('retained').findOne({ topic: 'hello/world' }, (err, result) => {
              assert.ok(!err, 'no error')
              assert.equal(null, result, 'must return empty packet')

              db.collection('incoming').findOne({ topic: 'hello/world' }, (err, result) => {
                assert.ok(!err, 'no error')
                assert.equal(null, result, 'must return empty packet')

                db.collection('outgoing').findOne({ topic: 'hello/world' }, (err, result) => {
                  assert.ok(!err, 'no error')
                  assert.equal(null, result, 'must return empty packet')

                  db.collection('will').findOne({ topic: 'hello/world' }, (err, result) => {
                    assert.ok(!err, 'no error')
                    assert.equal(null, result, 'must return empty packet')

                    instance.destroy(t.diagnostic('instance dies'))
                    emitter.close(t.diagnostic('emitter dies'))
                  })
                })
              })
            })
          }

          instance.storeRetained(packet, (err) => {
            assert.ok(!err, 'no error')

            instance.incomingStorePacket({ clientId: 'client1' }, packet, (err) => {
              assert.ok(!err, 'no error')

              instance.outgoingEnqueue({ clientId: 'client1' }, packet, (err) => {
                assert.ok(!err, 'no error')

                instance.putWill({ clientId: 'client1' }, packet, function (err) {
                  assert.ok(!err, 'no error')

                  setTimeout(checkExpired.bind(this), 4000) // https://docs.mongodb.com/manual/core/index-ttl/#timing-of-the-delete-operation
                })
              })
            })
          })
        })
      })
    })
  })

  const dboptsWithUrlMongoOptions = {
    url: mongourl,
    mongoOptions: {
      raw: true // must be a valid mongo option
    }
  }

  test('should pass mongoOptions to mongodb driver', (t) => {
    const instance = persistence(dboptsWithUrlMongoOptions)
    instance._connect((err, client) => {
      assert.ifError(err)
      for (const opt in dboptsWithUrlMongoOptions.mongoOptions) {
        assert.equal(dboptsWithUrlMongoOptions.mongoOptions[opt], client.s.options[opt], 'must pass options to mongodb')
      }
      client.close(() => {
        t.diagnostic('Client closed')
      })
    })
  })

  test('subscription should expire after client disconnected', (t) => {
    dbopts.ttl = {
      subscriptions: 1
    }
    dbopts.ttlAfterDisconnected = true
    const instance = persistence(dbopts)
    const emitter = mqemitterMongo(dbopts)
    const client = { id: 'client1' }
    instance.broker = toBroker('1', emitter)
    instance.on('ready', () => {
      instance.addSubscriptions(client, [{
        topic: 'hello',
        qos: 1
      }], (err) => {
        assert.ifError(err)
        db.collection('subscriptions').findOne({ clientId: client.id, topic: 'hello' }, (err, result) => {
          assert.ifError(err)
          assert.notEqual(result, null, 'must return subscription')
          assert.equal(result.disconnected, undefined, 'disconnected should not be set')
          instance.broker.emit('clientDisconnect', client)
          setTimeout(() => {
            db.collection('subscriptions').findOne({ clientId: client.id, topic: 'hello' }, (err, result) => {
              assert.ifError(err)
              assert.notEqual(result, null, 'must return subscription')
              assert.notEqual(result.disconnected, undefined, 'disconnected should be set')
              setTimeout(() => {
                db.collection('subscriptions').findOne({ clientId: client.id, topic: 'hello' }, (err, result) => {
                  assert.ifError(err)
                  assert.equal(result, null, 'must not return subscription')
                  instance.destroy(t.diagnostic('instance dies'))
                  emitter.close()
                })
              }, 3000)
            })
          }, 500)
        })
      })
    })
  })

  test('prevent executing bulk when instance is destroyed', (t) => {
    clean((err) => {
      assert.ifError(err)

      const emitter = mqemitterMongo(dbopts)

      emitter.status.on('stream', () => {
        t.diagnostic('mqemitter ready')
        const instance = persistence(dbopts)
        instance.broker = toBroker('2', emitter)

        instance.on('ready', () => {
          t.diagnostic('instance ready')

          const packet = {
            cmd: 'publish',
            id: instance.broker.id,
            topic: 'hello/world',
            payload: Buffer.from('muahah'),
            qos: 0,
            retain: true
          }

          instance.packetsQueue.push({ packet, cb: () => { } })

          instance.destroy(() => {
            t.diagnostic('Instance dies')
            instance._executeBulk() // should not throw
            emitter.close()
          })
        })
      })
    })
  })
  client.close()
  sleep(5).then(() => process.exit(0))
}
