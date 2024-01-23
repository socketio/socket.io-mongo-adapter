import { MongoClient } from "mongodb";
import { Server } from "socket.io";
import { createAdapter } from "@socket.io/mongo-adapter";
import { io } from "socket.io-client";

async function initMongoCollection() {
  const mongoClient = new MongoClient(
    "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
  );

  await mongoClient.connect();

  const mongoCollection = mongoClient
    .db("mydb")
    .collection("socket.io-adapter-events-ttl");

  await mongoCollection.createIndex(
    { createdAt: 1 },
    { expireAfterSeconds: 3600, background: true }
  );

  return mongoCollection;
}

function initServer(mongoCollection) {
  const io = new Server({
    connectionStateRecovery: {},
  });

  io.adapter(
    createAdapter(mongoCollection, {
      addCreatedAtField: true,
    })
  );

  return io;
}

function initClient(port) {
  const socket = io(`http://localhost:${port}`);

  socket.on("connect", () => {
    console.log(`[${port}] connected (recovered? ${socket.recovered})`);
  });

  socket.on("ping", () => {
    console.log(`[${port}] got ping`);
  });

  socket.on("disconnect", (reason) => {
    console.log(`[${port}] disconnected due to ${reason}`);
  });

  return socket;
}

const mongoCollection = await initMongoCollection();

const io1 = initServer(mongoCollection);
const io2 = initServer(mongoCollection);
const io3 = initServer(mongoCollection);

io1.listen(3000);
io2.listen(3001);
io3.listen(3002);

initClient(3000);
initClient(3001);
initClient(3002);

setInterval(() => {
  io1.emit("ping");
}, 2000);

// uncomment to test connection state recovery
// setTimeout(() => {
//   io2.close(() => {
//     io2.listen(3001);
//   });
// }, 3000);
