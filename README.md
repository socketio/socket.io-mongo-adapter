# Socket.IO MongoDB adapter

The `@socket.io/mongo-adapter` package allows broadcasting packets between multiple Socket.IO servers.

![Adapter diagram](./assets/adapter.png)

Unlike the existing [`socket.io-adapter-mongo`](https://github.com/lklepner/socket.io-adapter-mongo) package which uses [tailable cursors](https://docs.mongodb.com/manual/core/tailable-cursors/), this package relies on [change streams](https://docs.mongodb.com/manual/changeStreams/) and thus requires a replica set or a sharded cluster.

Supported features:

- [broadcasting](https://socket.io/docs/v4/broadcasting-events/)
- [utility methods](https://socket.io/docs/v4/server-instance/#Utility-methods)
  - [`socketsJoin`](https://socket.io/docs/v4/server-instance/#socketsJoin)
  - [`socketsLeave`](https://socket.io/docs/v4/server-instance/#socketsLeave)
  - [`disconnectSockets`](https://socket.io/docs/v4/server-instance/#disconnectSockets)
  - [`fetchSockets`](https://socket.io/docs/v4/server-instance/#fetchSockets)
  - [`serverSideEmit`](https://socket.io/docs/v4/server-instance/#serverSideEmit)

**Table of contents**

- [Installation](#installation)
- [Usage](#usage)
- [Known errors](#known-errors)
- [License](#license)

## Installation

```
npm install @socket.io/mongo-adapter mongodb
```

For TypeScript users, you might also need `@types/mongodb`.

## Usage

```js
const { Server } = require("socket.io");
const { createAdapter } = require("@socket.io/mongo-adapter");
const { MongoClient } = require("mongodb");

const DB = "mydb";
const COLLECTION = "socket.io-adapter-events";

const io = new Server();

const mongoClient = new MongoClient("mongodb://localhost:27017/?replicaSet=rs0", {
  useUnifiedTopology: true,
});

const main = async () => {
  await mongoClient.connect();

  try {
    await mongoClient.db(DB).createCollection(COLLECTION, {
      capped: true,
      size: 1e6
    });
  } catch (e) {
    // collection already exists
  }
  const mongoCollection = mongoClient.db(DB).collection(COLLECTION);

  io.adapter(createAdapter(mongoCollection));
  io.listen(3000);
}

main();
```

Note: the [capped collection](https://docs.mongodb.com/manual/core/capped-collections/) prevents the collection from growing too big.

## Known errors

- `MongoError: The $changeStream stage is only supported on replica sets`

Change streams are only available for replica sets and sharded clusters.

More information [here](https://docs.mongodb.com/manual/changeStreams/).

Please note that, for development purposes, you can have a single MongoDB process acting as a replica set by running `rs.initiate()` on the node.

- `TypeError: this.mongoCollection.insertOne is not a function`

You probably passed a MongoDB client instead of a MongoDB collection to the `createAdapter` method.

## License

[MIT](LICENSE)
