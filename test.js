'use strict'

var test = require('tape').test
var persistence = require('./')
var abs = require('aedes-cached-persistence/abstract')
var mqemitterMongo = require('mqemitter-mongodb')
var clean = require('mongo-clean')
var mongourl = 'mongodb://127.0.0.1/aedes-test'
var cleanopts = {
  exclude: ['pubsub'],
  action: 'remove'
}

clean(mongourl, cleanopts, function (err, db) {
  if (err) {
    throw err
  }

  test.onFinish(function () {
    db.close()
  })

  var dbopts = {
    db: db
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
    t.plan(8)

    clean(db, cleanopts, function (err) {
      t.error(err)

      var emitter = mqemitterMongo(dbopts)
      var emitter2 = mqemitterMongo(dbopts)

      var instance = persistence(dbopts)
      var instance2 = persistence(dbopts)

      instance.broker = toBroker('1', emitter)
      instance2.broker = toBroker('2', emitter2)

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
