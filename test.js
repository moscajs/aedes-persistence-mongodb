'use strict'

var test = require('tape').test
var persistence = require('./')
var abs = require('aedes-cached-persistence/abstract')
var mqemitterMongo = require('mqemitter-mongodb')
var clean = require('mongo-clean')
var mongourl = 'mongodb://127.0.0.1/aedes-test'
var cleanopts = {
  action: 'remove'
}

clean(mongourl, cleanopts, function (err, db) {
  if (err) {
    throw err
  }

  test.onFinish(function () {
    db.unref()
    db.close()
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

      emitter.status.on('stream', function () {
        t.pass('mqemitter 1 ready')

        var emitter2 = mqemitterMongo(dbopts)

        emitter2.status.on('stream', function () {
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
                  instance.destroy(t.pass.bind(t, 'first dies'))
                  instance2.destroy(t.pass.bind(t, 'second dies'))
                  emitter.close(t.pass.bind(t, 'first emitter dies'))
                  emitter2.close(t.pass.bind(t, 'second emitter dies'))
                })
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
})
