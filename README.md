# Socket.IO MongoDB adapter

The `@socket.io/mongo-adapter` package allows broadcasting packets between multiple Socket.IO servers.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="./assets/adapter_dark.png">
  <img alt="Diagram of Socket.IO packets forwarded through MongoDB" src="./assets/adapter.png">
</picture>

Unlike the existing [`socket.io-adapter-mongo`](https://github.com/lklepner/socket.io-adapter-mongo) package which uses [tailable cursors](https://docs.mongodb.com/manual/core/tailable-cursors/), this package relies on [change streams](https://docs.mongodb.com/manual/changeStreams/) and thus requires a replica set or a sharded cluster.

**Table of contents**

- [Supported features](#supported-features)
- [Installation](#installation)
- [Usage](#usage)
  - [Usage with a capped collection](#usage-with-a-capped-collection)
  - [Usage with a TTL index](#usage-with-a-ttl-index)
- [Known errors](#known-errors)
- [License](#license)

## Supported features

| Feature                         | `socket.io` version | Support                                        |
|---------------------------------|---------------------|------------------------------------------------|
| Socket management               | `4.0.0`             | :white_check_mark: YES (since version `0.1.0`) |
| Inter-server communication      | `4.1.0`             | :white_check_mark: YES (since version `0.1.0`) |
| Broadcast with acknowledgements | `4.5.0`             | :white_check_mark: YES (since version `0.2.0`) |
| Connection state recovery       | `4.6.0`             | :white_check_mark: YES (since version `0.3.0`) |

## Installation

```
npm install @socket.io/mongo-adapter mongodb
```

## Usage

Broadcasting packets within a Socket.IO cluster is achieved by creating MongoDB documents and using a [change stream](https://docs.mongodb.com/manual/changeStreams/) on each Socket.IO server.

There are two ways to clean up the documents in MongoDB:

- a [capped collection](https://www.mongodb.com/docs/manual/core/capped-collections/)
- a [TTL index](https://www.mongodb.com/docs/manual/core/index-ttl/)

### Usage with a capped collection

```js
import { Server } from "socket.io";
import { createAdapter } from "@socket.io/mongo-adapter";
import { MongoClient } from "mongodb";

const DB = "mydb";
const COLLECTION = "socket.io-adapter-events";

const io = new Server();

const mongoClient = new MongoClient("mongodb://localhost:27017/?replicaSet=rs0");

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
```

### Usage with a TTL index

```js
import { Server } from "socket.io";
import { createAdapter } from "@socket.io/mongo-adapter";
import { MongoClient } from "mongodb";

const DB = "mydb";
const COLLECTION = "socket.io-adapter-events";

const io = new Server();

const mongoClient = new MongoClient("mongodb://localhost:27017/?replicaSet=rs0");

await mongoClient.connect();

const mongoCollection = mongoClient.db(DB).collection(COLLECTION);

await mongoCollection.createIndex(
  { createdAt: 1 },
  { expireAfterSeconds: 3600, background: true }
);

io.adapter(createAdapter(mongoCollection, {
  addCreatedAtField: true
}));

io.listen(3000);
```

## Known errors

- `MongoError: The $changeStream stage is only supported on replica sets`

Change streams are only available for replica sets and sharded clusters.

More information [here](https://docs.mongodb.com/manual/changeStreams/).

Please note that, for development purposes, you can have a single MongoDB process acting as a replica set by running `rs.initiate()` on the node.

- `TypeError: this.mongoCollection.insertOne is not a function`

You probably passed a MongoDB client instead of a MongoDB collection to the `createAdapter` method.

## License

[MIT](LICENSE)
