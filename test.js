'use strict'

const { test } = require('tape')
const { EventEmitter } = require('events')
const persistence = require('./')
const { MongoClient } = require('mongodb')
const abs = require('aedes-cached-persistence/abstract')
const mqemitterMongo = require('mqemitter-mongodb')
const dbname = 'aedes-test'
const mongourl = 'mongodb://127.0.0.1/' + dbname
let clean = null

MongoClient.connect(mongourl, { useNewUrlParser: true, useUnifiedTopology: true, w: 1 }, function (err, client) {
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
  db.admin().command({ setParameter: 1, ttlMonitorSleepSecs: 2 }, function (err) {
    if (err) {
      throw err
    }

    clean(function (err) {
      if (err) {
        throw err
      }

      runTest(client, db)
    })
  })
})

function runTest (client, db) {
  test.onFinish(function () {
    // close mongodb client connection
    client.close()
  })

  const dbopts = {
    url: mongourl
  }

  abs({
    test,
    waitForReady: true,
    buildEmitter: function () {
      const emitter = mqemitterMongo(dbopts)
      return emitter
    },
    persistence: function build (cb) {
      clean(function (err) {
        if (err) {
          return cb(err)
        }

        const instance = persistence(dbopts)

        const oldDestroy = instance.destroy

        instance.destroy = function (cb) {
          instance.destroy = oldDestroy
          instance.destroy(function () {
            instance.broker.mq.close(function () {
              cb()
            })
          })
        }

        cb(null, instance)
      })
    }
  })

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

  test('multiple persistences', function (t) {
    t.plan(12)

    clean(function (err) {
      t.error(err)

      const emitter = mqemitterMongo(dbopts)

      emitter.status.once('stream', function () {
        t.pass('mqemitter 1 ready')

        const emitter2 = mqemitterMongo(dbopts)

        emitter2.status.once('stream', function () {
          t.pass('mqemitter 2 ready')

          const instance = persistence(dbopts)
          instance.broker = toBroker('1', emitter)

          instance.on('ready', function () {
            t.pass('instance ready')

            const instance2 = persistence(dbopts)
            instance2.broker = toBroker('2', emitter2)

            instance2.on('ready', function () {
              t.pass('instance2 ready')

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

              instance.addSubscriptions(client, subs, function (err) {
                t.notOk(err, 'no error')
                setTimeout(function () {
                  instance2.subscriptionsByTopic('hello', function (err, resubs) {
                    t.notOk(err, 'no error')
                    t.deepEqual(resubs, [{
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
                    instance.destroy(function () {
                      t.pass('first dies')
                      emitter.close(function () {
                        t.pass('first emitter dies')
                        instance2.destroy(function () {
                          t.pass('seond dies')
                          emitter2.close(t.pass.bind(t, 'second emitter dies'))
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

  test('multiple persistences with passed db object and url', function (t) {
    t.plan(12)

    clean(function (err) {
      t.error(err)

      const emitter = mqemitterMongo(dboptsWithDbObjectAndUrl)

      emitter.status.once('stream', function () {
        t.pass('mqemitter 1 ready')

        const emitter2 = mqemitterMongo(dboptsWithDbObjectAndUrl)

        emitter2.status.once('stream', function () {
          t.pass('mqemitter 2 ready')

          const instance = persistence(dboptsWithDbObjectAndUrl)
          instance.broker = toBroker('1', emitter)

          instance.on('ready', function () {
            t.pass('instance ready')

            const instance2 = persistence(dboptsWithDbObjectAndUrl)
            instance2.broker = toBroker('2', emitter2)

            instance2.on('ready', function () {
              t.pass('instance2 ready')

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

              instance.addSubscriptions(client, subs, function (err) {
                t.notOk(err, 'no error')
                setTimeout(function () {
                  instance2.subscriptionsByTopic('hello', function (err, resubs) {
                    t.notOk(err, 'no error')
                    t.deepEqual(resubs, [{
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
                    instance.destroy(function () {
                      t.pass('first dies')
                      emitter.close(function () {
                        t.pass('first emitter dies')
                        instance2.destroy(function () {
                          t.pass('seond dies')
                          emitter2.close(t.pass.bind(t, 'second emitter dies'))
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

  test('multiple persistences with passed only db object', function (t) {
    t.plan(12)

    clean(function (err) {
      t.error(err)

      const emitter = mqemitterMongo(dboptsWithOnlyDbObject)

      emitter.status.once('stream', function () {
        t.pass('mqemitter 1 ready')

        const emitter2 = mqemitterMongo(dboptsWithOnlyDbObject)

        emitter2.status.once('stream', function () {
          t.pass('mqemitter 2 ready')

          const instance = persistence(dboptsWithOnlyDbObject)
          instance.broker = toBroker('1', emitter)

          instance.on('ready', function () {
            t.pass('instance ready')

            const instance2 = persistence(dboptsWithOnlyDbObject)
            instance2.broker = toBroker('2', emitter2)

            instance2.on('ready', function () {
              t.pass('instance2 ready')

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

              instance.addSubscriptions(client, subs, function (err) {
                t.notOk(err, 'no error')
                setTimeout(function () {
                  instance2.subscriptionsByTopic('hello', function (err, resubs) {
                    t.notOk(err, 'no error')
                    t.deepEqual(resubs, [{
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
                    instance.destroy(function () {
                      t.pass('first dies')
                      emitter.close(function () {
                        t.pass('first emitter dies')
                        instance2.destroy(function () {
                          t.pass('seond dies')
                          emitter2.close(t.pass.bind(t, 'second emitter dies'))
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

  test('qos 0 subs restoration', function (t) {
    t.plan(10)

    clean(function (err) {
      t.error(err)

      const emitter = mqemitterMongo(dbopts)

      emitter.status.on('stream', function () {
        t.pass('mqemitter 1 ready')
        const instance = persistence(dbopts)
        instance.broker = toBroker('1', emitter)

        instance.on('ready', function () {
          t.pass('instance ready')
          const client = { id: 'abcde' }
          const subs = [{
            topic: 'hello',
            qos: 0
          }]

          instance.addSubscriptions(client, subs, function (err, client) {
            t.notOk(err, 'no error')

            instance.destroy(t.pass.bind(t, 'first dies'))
            emitter.close(t.pass.bind(t, 'first emitter dies'))

            const instance2 = persistence(dbopts)
            instance2.broker = toBroker('1', emitter)

            instance2.on('ready', function () {
              t.pass('instance ready')
              instance2.subscriptionsByTopic('hello', function (err, resubs) {
                t.notOk(err, 'should not err')
                t.deepEqual(resubs, [{
                  clientId: 'abcde',
                  topic: 'hello',
                  qos: 0,
                  rh: undefined,
                  rap: undefined,
                  nl: undefined
                }])
                instance2.destroy(t.pass.bind(t, 'second dies'))
              })
            })
          })
        })
      })
    })
  })

  test('look up for expire after seconds index', function (t) {
    clean(function (err) {
      t.error(err)

      dbopts.ttl = {
        packets: 1,
        subscriptions: 1
      }
      dbopts.ttlAfterDisconnected = true
      const emitter = mqemitterMongo(dbopts)

      emitter.status.on('stream', function () {
        t.pass('mqemitter ready')
        const instance = persistence(dbopts)
        instance.broker = toBroker('1', emitter)

        instance.on('ready', function () {
          t.pass('instance ready')

          db.collection('retained').indexInformation({ full: true }, function (err, indexes) {
            t.notOk(err, 'no error')
            const index = indexes.find(index => index.name === 'ttl')
            t.deepEqual({ added: 1 }, index.key, 'must return the index key')

            db.collection('incoming').indexInformation({ full: true }, function (err, indexes) {
              t.notOk(err, 'no error')
              const index = indexes.find(index => index.name === 'ttl')
              t.deepEqual({ 'packet.added': 1 }, index.key, 'must return the index key')

              db.collection('outgoing').indexInformation({ full: true }, function (err, indexes) {
                t.notOk(err, 'no error')
                const index = indexes.find(index => index.name === 'ttl')
                t.deepEqual({ 'packet.added': 1 }, index.key, 'must return the index key')

                db.collection('will').indexInformation({ full: true }, function (err, indexes) {
                  t.notOk(err, 'no error')
                  const index = indexes.find(index => index.name === 'ttl')
                  t.deepEqual({ 'packet.added': 1 }, index.key, 'must return the index key')

                  db.collection('subscriptions').indexInformation({ full: true }, function (err, indexes) {
                    t.notOk(err, 'no error')
                    const index = indexes.find(index => index.name === 'ttl')
                    t.deepEqual({ disconnected: 1 }, index.key, 'must return the index key')

                    instance.destroy(function () {
                      t.pass('Instance dies')
                      emitter.close(t.end.bind(t))
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

  test('look up for query indexes', function (t) {
    clean(function (err) {
      t.error(err)

      dbopts.ttl = {
        packets: 1,
        subscriptions: 1
      }
      const emitter = mqemitterMongo(dbopts)

      emitter.status.on('stream', function () {
        t.pass('mqemitter ready')
        const instance = persistence(dbopts)
        instance.broker = toBroker('1', emitter)

        instance.on('ready', function () {
          t.pass('instance ready')

          db.collection('incoming').indexInformation({ full: true }, function (err, indexes) {
            t.notOk(err, 'no error')
            const messageIdIndex = indexes.find(index => index.name === 'query_clientId_messageId')
            const brokerIdIndex = indexes.find(index => index.name === 'query_clientId_brokerId')
            t.deepEqual(
              { clientId: 1, 'packet.messageId': 1 },
              messageIdIndex.key, 'must return the index key'
            )
            t.deepEqual(
              { clientId: 1, 'packet.brokerId': 1, 'packet.brokerCounter': 1 },
              brokerIdIndex.key, 'must return the index key'
            )

            db.collection('outgoing').indexInformation({ full: true }, function (err, indexes) {
              t.notOk(err, 'no error')
              const messageIdIndex = indexes.find(index => index.name === 'query_clientId_messageId')
              const brokerIdIndex = indexes.find(index => index.name === 'query_clientId_brokerId')
              t.deepEqual(
                { clientId: 1, 'packet.messageId': 1 },
                messageIdIndex.key, 'must return the index key'
              )
              t.deepEqual(
                { clientId: 1, 'packet.brokerId': 1, 'packet.brokerCounter': 1 },
                brokerIdIndex.key, 'must return the index key'
              )

              instance.destroy(function () {
                t.pass('Instance dies')
                emitter.close(t.end.bind(t))
              })
            })
          })
        })
      })
    })
  })

  test('look up for packet with added property', function (t) {
    clean(function (err) {
      t.error(err)

      dbopts.ttl = {
        packets: 1,
        subscriptions: 1
      }
      const emitter = mqemitterMongo(dbopts)

      emitter.status.on('stream', function () {
        t.pass('mqemitter ready')
        const instance = persistence(dbopts)
        instance.broker = toBroker('2', emitter)

        instance.on('ready', function () {
          t.pass('instance ready')

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

          instance.storeRetained(packet, function (err) {
            t.notOk(err, 'no error')

            db.collection('retained').findOne({ topic: 'hello/world' }, function (err, result) {
              t.notOk(err, 'no error')
              delete result._id
              result.payload = result.payload.buffer
              t.deepEqual(packet, result, 'must return the packet')
              instance.destroy(function () {
                t.pass('Instance dies')
                emitter.close(t.end.bind(t))
              })
            })
          })
        })
      })
    })
  })

  test('drop existing indexes', function (t) {
    function checkIndexes (shouldExist, cb) {
      db.collections(function (err, collections) {
        t.notOk(err, 'no error')
        if (collections.length === 0) {
          cb()
        } else {
          let done = 0
          for (let i = 0; i < collections.length; i++) {
            collections[i].indexExists('ttl', function (err, exists) {
              t.notOk(err, 'no error')
              if (collections[i].namespace.indexOf('pubsub') < 0) { // pubsub is the collection created by mqemitter-mongodb
                t.equal(shouldExist, exists, 'Index on ' + collections[i].namespace + ' should' + (shouldExist ? '' : ' not') + ' exist')
              }
              if (++done >= collections.length) {
                cb()
              }
            })
          }
        }
      })
    }

    clean(function (err) {
      t.notOk(err, 'no error')

      dbopts.ttl = {
        packets: 1,
        subscriptions: 1
      }
      const emitter = mqemitterMongo(dbopts)

      emitter.status.on('stream', function () {
        t.pass('mqemitter ready')

        const instance = persistence(dbopts)
        instance.broker = toBroker('1', emitter)

        instance.on('ready', function () {
          t.pass('instance ready')

          checkIndexes(true, function () {
            delete dbopts.ttl
            dbopts.dropExistingIndexes = true

            instance.destroy(t.pass.bind(t, 'first instance dies'))
            emitter.close(t.pass.bind(t, 'first emitter dies'))

            const emitter2 = mqemitterMongo(dbopts)

            emitter2.status.on('stream', function () {
              t.pass('mqemitter ready')

              const instance2 = persistence(dbopts)
              instance2.broker = toBroker('2', emitter2)

              instance2.on('ready', function () {
                t.pass('instance ready')
                checkIndexes(false, function () {
                  instance2.destroy(t.pass.bind(t, 'second instance dies'))
                  emitter2.close(t.end.bind(t))
                })
              })
            })
          })
        })
      })
    })
  })

  test('look up for expired packets', function (t) {
    t.plan(17)

    clean(function (err) {
      t.error(err)

      dbopts.ttl = {
        packets: 1,
        subscriptions: 1
      }
      const emitter = mqemitterMongo(dbopts)

      emitter.status.on('stream', function () {
        t.pass('mqemitter ready')
        const instance = persistence(dbopts)
        instance.broker = toBroker('1', emitter)

        instance.on('ready', function () {
          t.pass('instance ready')

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
            db.collection('retained').findOne({ topic: 'hello/world' }, function (err, result) {
              t.notOk(err, 'no error')
              t.equal(null, result, 'must return empty packet')

              db.collection('incoming').findOne({ topic: 'hello/world' }, function (err, result) {
                t.notOk(err, 'no error')
                t.equal(null, result, 'must return empty packet')

                db.collection('outgoing').findOne({ topic: 'hello/world' }, function (err, result) {
                  t.notOk(err, 'no error')
                  t.equal(null, result, 'must return empty packet')

                  db.collection('will').findOne({ topic: 'hello/world' }, function (err, result) {
                    t.notOk(err, 'no error')
                    t.equal(null, result, 'must return empty packet')

                    instance.destroy(t.pass.bind(t, 'instance dies'))
                    emitter.close(t.pass.bind(t, 'emitter dies'))
                  })
                })
              })
            })
          }

          instance.storeRetained(packet, function (err) {
            t.notOk(err, 'no error')

            instance.incomingStorePacket({ clientId: 'client1' }, packet, function (err) {
              t.notOk(err, 'no error')

              instance.outgoingEnqueue({ clientId: 'client1' }, packet, function (err) {
                t.notOk(err, 'no error')

                instance.putWill({ clientId: 'client1' }, packet, function (err) {
                  t.notOk(err, 'no error')

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

  test('should pass mongoOptions to mongodb driver', function (t) {
    const instance = persistence(dboptsWithUrlMongoOptions)
    instance._connect(function (err, client) {
      t.error(err)
      for (const opt in dboptsWithUrlMongoOptions.mongoOptions) {
        t.equal(dboptsWithUrlMongoOptions.mongoOptions[opt], client.s.options[opt], 'must pass options to mongodb')
      }
      client.close(function () {
        t.pass('Client closed')
        t.end()
      })
    })
  })

  test('subscription should expire after client disconnected', function (t) {
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
      }], function (err) {
        t.error(err)
        db.collection('subscriptions').findOne({ clientId: client.id, topic: 'hello' }, function (err, result) {
          t.error(err)
          t.notEqual(result, null, 'must return subscription')
          t.equal(result.disconnected, undefined, 'disconnected should not be set')
          instance.broker.emit('clientDisconnect', client)
          setTimeout(() => {
            db.collection('subscriptions').findOne({ clientId: client.id, topic: 'hello' }, function (err, result) {
              t.error(err)
              t.notEqual(result, null, 'must return subscription')
              t.notEqual(result.disconnected, undefined, 'disconnected should be set')
              setTimeout(() => {
                db.collection('subscriptions').findOne({ clientId: client.id, topic: 'hello' }, function (err, result) {
                  t.error(err)
                  t.equal(result, null, 'must not return subscription')
                  instance.destroy(t.pass.bind(t, 'instance dies'))
                  emitter.close(t.end.bind(t))
                })
              }, 3000)
            })
          }, 500)
        })
      })
    })
  })

  test('prevent executing bulk when instance is destroied', function (t) {
    clean(function (err) {
      t.error(err)

      const emitter = mqemitterMongo(dbopts)

      emitter.status.on('stream', function () {
        t.pass('mqemitter ready')
        const instance = persistence(dbopts)
        instance.broker = toBroker('2', emitter)

        instance.on('ready', function () {
          t.pass('instance ready')

          const packet = {
            cmd: 'publish',
            id: instance.broker.id,
            topic: 'hello/world',
            payload: Buffer.from('muahah'),
            qos: 0,
            retain: true
          }

          instance.packetsQueue.push({ packet, cb: () => { } })

          instance.destroy(function () {
            t.pass('Instance dies')
            instance._executeBulk() // should not throw
            emitter.close(t.end.bind(t))
          })
        })
      })
    })
  })
}
