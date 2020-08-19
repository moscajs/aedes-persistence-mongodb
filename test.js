'use strict'

var test = require('tape').test
var persistence = require('./')
var MongoClient = require('mongodb').MongoClient
var abs = require('aedes-cached-persistence/abstract')
var mqemitterMongo = require('mqemitter-mongodb')
var clean = require('mongo-clean')
var dbname = 'aedes-test'
var mongourl = 'mongodb://127.0.0.1/' + dbname
var cleanopts = {
  action: 'deleteMany'
}

MongoClient.connect(mongourl, { useNewUrlParser: true, useUnifiedTopology: true, w: 1 }, function (err, client) {
  if (err) {
    throw err
  }

  var db = client.db(dbname)

  // set ttl task to run every 2 seconds
  db.executeDbAdminCommand({ setParameter: 1, ttlMonitorSleepSecs: 2 }, function (err) {
    if (err) {
      throw err
    }

    clean(db, cleanopts, function (err, db) {
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

  var dbopts = {
    url: mongourl
  }

  abs({
    test: test,
    waitForReady: true,
    buildEmitter: function () {
      var emitter = mqemitterMongo(dbopts)
      return emitter
    },
    persistence: function build (cb) {
      clean(db, cleanopts, function (err) {
        if (err) {
          return cb(err)
        }

        var instance = persistence(dbopts)

        var oldDestroy = instance.destroy

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
    return {
      id: id,
      publish: emitter.emit.bind(emitter),
      subscribe: emitter.on.bind(emitter),
      unsubscribe: emitter.removeListener.bind(emitter)
    }
  }

  test('multiple persistences', function (t) {
    t.plan(12)

    clean(db, cleanopts, function (err) {
      t.error(err)

      var emitter = mqemitterMongo(dbopts)

      emitter.status.once('stream', function () {
        t.pass('mqemitter 1 ready')

        var emitter2 = mqemitterMongo(dbopts)

        emitter2.status.once('stream', function () {
          t.pass('mqemitter 2 ready')

          var instance = persistence(dbopts)
          instance.broker = toBroker('1', emitter)

          instance.on('ready', function () {
            t.pass('instance ready')

            var instance2 = persistence(dbopts)
            instance2.broker = toBroker('2', emitter2)

            instance2.on('ready', function () {
              t.pass('instance2 ready')

              var client = { id: 'abcde' }
              var subs = [{
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
                      qos: 1
                    }, {
                      clientId: client.id,
                      topic: 'hello',
                      qos: 1
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

  var dboptsWithDbObjectAndUrl = {
    url: mongourl,
    db: db
  }

  test('multiple persistences with passed db object and url', function (t) {
    t.plan(12)

    clean(db, cleanopts, function (err) {
      t.error(err)

      var emitter = mqemitterMongo(dboptsWithDbObjectAndUrl)

      emitter.status.once('stream', function () {
        t.pass('mqemitter 1 ready')

        var emitter2 = mqemitterMongo(dboptsWithDbObjectAndUrl)

        emitter2.status.once('stream', function () {
          t.pass('mqemitter 2 ready')

          var instance = persistence(dboptsWithDbObjectAndUrl)
          instance.broker = toBroker('1', emitter)

          instance.on('ready', function () {
            t.pass('instance ready')

            var instance2 = persistence(dboptsWithDbObjectAndUrl)
            instance2.broker = toBroker('2', emitter2)

            instance2.on('ready', function () {
              t.pass('instance2 ready')

              var client = { id: 'abcde' }
              var subs = [{
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
                      qos: 1
                    }, {
                      clientId: client.id,
                      topic: 'hello',
                      qos: 1
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

  var dboptsWithOnlyDbObject = {
    db: db
  }

  test('multiple persistences with passed only db object', function (t) {
    t.plan(12)

    clean(db, cleanopts, function (err) {
      t.error(err)

      var emitter = mqemitterMongo(dboptsWithOnlyDbObject)

      emitter.status.once('stream', function () {
        t.pass('mqemitter 1 ready')

        var emitter2 = mqemitterMongo(dboptsWithOnlyDbObject)

        emitter2.status.once('stream', function () {
          t.pass('mqemitter 2 ready')

          var instance = persistence(dboptsWithOnlyDbObject)
          instance.broker = toBroker('1', emitter)

          instance.on('ready', function () {
            t.pass('instance ready')

            var instance2 = persistence(dboptsWithOnlyDbObject)
            instance2.broker = toBroker('2', emitter2)

            instance2.on('ready', function () {
              t.pass('instance2 ready')

              var client = { id: 'abcde' }
              var subs = [{
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
                      qos: 1
                    }, {
                      clientId: client.id,
                      topic: 'hello',
                      qos: 1
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

    clean(db, cleanopts, function (err) {
      t.error(err)

      var emitter = mqemitterMongo(dbopts)

      emitter.status.on('stream', function () {
        t.pass('mqemitter 1 ready')
        var instance = persistence(dbopts)
        instance.broker = toBroker('1', emitter)

        instance.on('ready', function () {
          t.pass('instance ready')
          var client = { id: 'abcde' }
          var subs = [{
            topic: 'hello',
            qos: 0
          }]

          instance.addSubscriptions(client, subs, function (err, client) {
            t.notOk(err, 'no error')

            instance.destroy(t.pass.bind(t, 'first dies'))
            emitter.close(t.pass.bind(t, 'first emitter dies'))

            var instance2 = persistence(dbopts)
            instance2.broker = toBroker('1', emitter)

            instance2.on('ready', function () {
              t.pass('instance ready')
              instance2.subscriptionsByTopic('hello', function (err, resubs) {
                t.notOk(err, 'should not err')
                t.deepEqual(resubs, [{
                  clientId: 'abcde',
                  topic: 'hello',
                  qos: 0
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
    clean(db, cleanopts, function (err) {
      t.error(err)

      dbopts.ttl = {
        packets: 1,
        subscriptions: 1
      }
      var emitter = mqemitterMongo(dbopts)

      emitter.status.on('stream', function () {
        t.pass('mqemitter ready')
        var instance = persistence(dbopts)
        instance.broker = toBroker('1', emitter)

        instance.on('ready', function () {
          t.pass('instance ready')

          db.collection('retained').indexInformation({ full: true }, function (err, indexes) {
            t.notOk(err, 'no error')
            t.deepEqual({ added: 1 }, indexes[1].key, 'must return the index key')

            db.collection('incoming').indexInformation({ full: true }, function (err, indexes) {
              t.notOk(err, 'no error')
              t.deepEqual({ 'packet.added': 1 }, indexes[1].key, 'must return the index key')

              db.collection('outgoing').indexInformation({ full: true }, function (err, indexes) {
                t.notOk(err, 'no error')
                t.deepEqual({ 'packet.added': 1 }, indexes[1].key, 'must return the index key')

                db.collection('will').indexInformation({ full: true }, function (err, indexes) {
                  t.notOk(err, 'no error')
                  t.deepEqual({ 'packet.added': 1 }, indexes[1].key, 'must return the index key')

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

  test('look up for packet with added property', function (t) {
    clean(db, cleanopts, function (err) {
      t.error(err)

      dbopts.ttl = {
        packets: 1,
        subscriptions: 1
      }
      var emitter = mqemitterMongo(dbopts)

      emitter.status.on('stream', function () {
        t.pass('mqemitter ready')
        var instance = persistence(dbopts)
        instance.broker = toBroker('2', emitter)

        instance.on('ready', function () {
          t.pass('instance ready')

          var date = new Date()
          var packet = {
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
          var done = 0
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

    clean(db, cleanopts, function (err) {
      t.notOk(err, 'no error')

      dbopts.ttl = {
        packets: 1,
        subscriptions: 1
      }
      var emitter = mqemitterMongo(dbopts)

      emitter.status.on('stream', function () {
        t.pass('mqemitter ready')

        var instance = persistence(dbopts)
        instance.broker = toBroker('1', emitter)

        instance.on('ready', function () {
          t.pass('instance ready')

          checkIndexes(true, function () {
            delete dbopts.ttl
            dbopts.dropExistingIndexes = true

            instance.destroy(t.pass.bind(t, 'first instance dies'))
            emitter.close(t.pass.bind(t, 'first emitter dies'))

            var emitter2 = mqemitterMongo(dbopts)

            emitter2.status.on('stream', function () {
              t.pass('mqemitter ready')

              var instance2 = persistence(dbopts)
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

    clean(db, cleanopts, function (err) {
      t.error(err)

      dbopts.ttl = {
        packets: 1,
        subscriptions: 1
      }
      var emitter = mqemitterMongo(dbopts)

      emitter.status.on('stream', function () {
        t.pass('mqemitter ready')
        var instance = persistence(dbopts)
        instance.broker = toBroker('1', emitter)

        instance.on('ready', function () {
          t.pass('instance ready')

          var date = new Date()
          var packet = {
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
}
