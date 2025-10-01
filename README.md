# aedes-persistence-mongodb

![.github/workflows/ci.yml](https://github.com/moscajs/aedes-persistence-mongodb/workflows/.github/workflows/ci.yml/badge.svg)
\
[![Known Vulnerabilities](https://snyk.io/test/github/moscajs/aedes-persistence-mongodb/badge.svg)](https://snyk.io/test/github/moscajs/aedes-persistence-mongodb)
[![Coverage Status](https://coveralls.io/repos/github/moscajs/aedes-persistence-mongodb/badge.svg?branch=master)](https://coveralls.io/github/moscajs/aedes-persistence-mongodb?branch=master)
[![NPM version](https://img.shields.io/npm/v/aedes-persistence-mongodb.svg?style=flat)](https://npm.im/aedes-persistence-mongodb)
[![NPM downloads](https://img.shields.io/npm/dm/aedes-persistence-mongodb.svg?style=flat)](https://npm.im/aedes-persistence-mongodb)

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

### Options

- `url`: The MongoDB connection url
- `mongoOptions`: Extra options to pass to MongoDB driver (see [node-mongodb-native](http://mongodb.github.io/node-mongodb-native/3.6/api/MongoClient.html))
- `ttl`: Used to set a ttl (time to live) to documents stored in collections
  - `packets`: Could be an integer value that specify the ttl in seconds of all packets collections or an Object that specifies for each collection its ttl in seconds. Packets collections are: `incoming`, `outgoing`, `retained`, `will`.
  - `susbscriptions`: Set a ttl (in seconds)
- `db`: Existing MongoDB instance (if no `url` option is specified)
- `dropExistingIndexes`: Flag used to drop any existing index previously created on collections (except default index `_id`)
- `ttlAfterDisconnected`: Flag used to enable alternative behavior of the subscription ttl, the subscription will expire based on time since client last disconnected from the broker instead of time since the subscription was created.
- `collectionPrefix`: Prefix to use for collection names (default: empty string)

> When changing ttl durations or switching on/off **ttlAfterDisconnected** on an existing database, **dropExistingIndexes** needs to be set to true for ttl indexes to be updated.

### Examples

```js
aedesPersistenceMongoDB({
  url: 'mongodb://127.0.0.1/aedes-test', // Optional when you pass db object
  // Optional mongo options
  mongoOptions: { 
    auth: {
      user: 'username',
      password: 'password'
    }
  },
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

If you want a specific collection to be **persistent** just set a ttl of `-1` or `null` or `undefined`.

If you want to reuse an existing MongoDb instance just set the `db` option:

```js
aedesPersistenceMongoDB({
 db:db
})
```

With mongoose:

```js
aedesPersistenceMongoDB({
 db: mongoose.connection.useDb('myDbName').db
})
```

## License

MIT

[aedes]: https://github.com/moscajs/aedes
[persistence]: https://github.com/moscajs/aedes-persistence
[mongodb]: https://www.mongodb.com
