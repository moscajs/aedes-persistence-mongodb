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
  action: 'remove'
}

MongoClient.connect(mongourl, { useNewUrlParser: true, w: 1 }, function (err, client) {
  if (err) {
    throw err
  }

  var db = client.db(dbname)

  clean(db, cleanopts, function (err, db) {
    if (err) {
      throw err
    }

    runTest(client, db)
  })
})

function runTest (client, db) {
  test.onFinish(function () {
    client.close()
  })

  var dbopts = {
    url: mongourl
  }

  abs({
    test: test,
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
    t.plan(6)

    clean(db, cleanopts, function (err) {
      t.error(err)

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

            instance.destroy(t.pass.bind(t))
            emitter.close(t.end.bind(t))
          })
        })
      })
    })
  })

  test('look up for packet with added property', function (t) {
    t.plan(7)

    clean(db, cleanopts, function (err) {
      t.error(err)

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

            db.collection('retained').findOne({ topic: 'hello/world' }, function (err, packet) {
              t.notOk(err, 'no error')
              t.deepEqual(date, packet.added, 'must return the packet')

              instance.destroy(t.pass.bind(t))
              emitter.close(t.end.bind(t))
            })
          })
        })
      })
    })
  })
}
