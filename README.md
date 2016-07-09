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
It accepts a connections string url.

Example:

```js
aedesPersistenceRedis({
  url: 'mongodb://127.0.0.1/aedes-test'
})
```

## License

MIT

[aedes]: https://github.com/mcollina/aedes
[persistence]: https://github.com/mcollina/aedes-persistence
[mongodb]: https://www.mongodb.com
