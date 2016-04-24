'use strict'

var test = require('tape').test
var persistence = require('./')
var abs = require('aedes-cached-persistence/abstract')
var mqemitterMongo = require('mqemitter-mongodb')
var clean = require('mongo-clean')
var mongourl = 'mongodb://127.0.0.1/aedes-test'

clean(mongourl, { exclude: ['pubsub'] }, function (err, db) {
  if (err) {
    throw err
  }

  var dbopts = {
    url: mongourl
  }

  var lastEmitter = null

  db.close()

  abs({
    test: test,
    buildEmitter: function () {
      var emitter = mqemitterMongo(dbopts)
      lastEmitter = emitter
      return emitter
    },
    persistence: function build (cb) {
      clean(mongourl, { exclude: ['pubsub'] }, function (err, db) {
        if (err) {
          return cb(err)
        }
        db.close()

        var instance = persistence(dbopts)

        var oldDestroy = instance.destroy

        instance.destroy = function (cb) {
          oldDestroy.call(this, function () {
            lastEmitter.close(cb)
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

    clean(mongourl, function (err, db) {
      t.error(err)
      db.close()

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
        // timeout needed because of mongodb latency
        // TODO remove
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
            instance.destroy(t.pass.bind(t, 'first dies'))
            instance2.destroy(t.pass.bind(t, 'second dies'))
            emitter.close(t.pass.bind(t, 'first emitter dies'))
            emitter2.close(t.pass.bind(t, 'second emitter dies'))
          })
        }, 200)
      })
    })
  })
})
