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
It accepts a connections string url or you can pass you existing db object.

Example:

```js
aedesPersistenceMongoDB({
  url: 'mongodb://127.0.0.1/aedes-test' //optional when you pass db object
})
```
Or

```
aedesPersistenceMongoDB({
 db:db
})
```
## License

MIT

[aedes]: https://github.com/mcollina/aedes
[persistence]: https://github.com/mcollina/aedes-persistence
[mongodb]: https://www.mongodb.com
