# aedes-persistence-mongodb

[Aedes][aedes] [persistence][persistence], backed by [MongoDB][mongodb].

See [aedes-persistence][persistence] for the full API, and [Aedes][aedes] for usage.

## Install

```
npm i aedes aedes-persistence-mongodb --save
```

## API

<a name="constructor"></a>
### aedesPersistenceMongoDB([opts])

Creates a new instance of aedes-persistence-mongodb.
It accepts a connections string `url` or you can pass your existing `db` object. Also, you can choose to set a `ttl` (time to live) for your subscribers or packets. This option will help you to empty your db from keeping useless data.

Example:

```js
aedesPersistenceMongoDB({
  url: 'mongodb://127.0.0.1/aedes-test', // Optional when you pass db object
  // Optional ttl settings
  ttl: {
      packets: 300, // Number of seconds
      subscriptions: 300,
  }
})
```

With the previous configuration all packets will have a ttl of 300 seconds. You can also provide different ttl settings for each collection:

```js
ttl: {
      packets: {
        incoming: 100,
        outgoing: 100,
        will: 300,
        retained: -1
      }, // Number of seconds
      subscriptions: 300,
}
```

If you want a specific collection to be **persistent** just set a ttl of `-1`.

If you want to reuse an existing MongoDb instance just set the `db` option:

```js
aedesPersistenceMongoDB({
 db:db
})
```

With mongoose:

```js
aedesPersistenceMongoDB({
 db: mongoose.connection.useDb('myDbName')
})
```

## License

MIT

[aedes]: https://github.com/mcollina/aedes
[persistence]: https://github.com/mcollina/aedes-persistence
[mongodb]: https://www.mongodb.com
