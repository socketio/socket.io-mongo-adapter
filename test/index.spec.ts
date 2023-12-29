import { createServer } from "http";
import { Server, Socket as ServerSocket } from "socket.io";
import { io as ioc, Socket as ClientSocket } from "socket.io-client";
import { createAdapter, MongoAdapter } from "../lib";
import { AddressInfo } from "net";
import { MongoClient } from "mongodb";
import { times, sleep } from "./util";
import waitForExpect from "wait-for-expect";

const NODES_COUNT = 3;

describe("@socket.io/mongodb-adapter", () => {
  let servers: Server[];
  let serverSockets: ServerSocket[];
  let clientSockets: ClientSocket[];
  let mongoClient: MongoClient;

  beforeEach(async () => {
    servers = [];
    serverSockets = [];
    clientSockets = [];

    mongoClient = new MongoClient(
      "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
    );
    await mongoClient.connect();

    const collection = mongoClient.db("test").collection("events");

    for (let i = 0; i < NODES_COUNT; i++) {
      const httpServer = createServer();
      const io = new Server(httpServer);
      io.adapter(createAdapter(collection));
      httpServer.listen(() => {
        const port = (httpServer.address() as AddressInfo).port;
        const clientSocket = ioc(`http://localhost:${port}`);

        io.on("connection", (socket) => {
          clientSockets.push(clientSocket);
          serverSockets.push(socket);
          servers.push(io);
        });
      });
    }

    // Wait for all connections being established (done in loop above)
    await waitForExpect(async () => {
      expect(servers.length).toEqual(NODES_COUNT);
    });

    for (let i = 0; i < NODES_COUNT; i++) {
      // ensure all nodes know each other
      servers[i].emit("ping");
    }

    // TODO: find a better way to wait for all nodes to be connected
    await sleep(200);
  });

  afterEach(async () => {
    servers.forEach((server) => {
      // @ts-ignore
      server.httpServer.close();
      server.of("/").adapter.close();
    });
    clientSockets.forEach((socket) => {
      socket.disconnect();
    });

    // TODO: somehow this still raises an error "connection establishment was cancelled"
    // try {
    //   await mongoClient.close();
    // } catch (e) {
    //   console.log(e);
    //   // ignore, we are in teardown
    // }
  });

  describe("broadcast", () => {
    it("broadcasts to all clients", (done) => {
      const partialDone = times(3, done);

      clientSockets.forEach((clientSocket) => {
        clientSocket.on("test", (arg1, arg2, arg3) => {
          expect(arg1).toEqual(1);
          expect(arg2).toEqual("2");
          expect(Buffer.isBuffer(arg3)).toEqual(true);
          partialDone();
        });
      });

      servers[0].emit("test", 1, "2", Buffer.from([3, 4]));
    });

    it("broadcasts to all clients in a namespace", (done) => {
      const partialDone = times(3, () => {
        servers.forEach((server) => server.of("/custom").adapter.close());
        done();
      });

      servers.forEach((server) => server.of("/custom"));

      const onConnect = times(3, () => {
        servers[0].of("/custom").emit("test");
      });

      clientSockets.forEach((clientSocket) => {
        const socket = clientSocket.io.socket("/custom");
        socket.on("connect", onConnect);
        socket.on("test", () => {
          socket.disconnect();
          partialDone();
        });
      });
    });

    it("broadcasts to all clients in a room", (done) => {
      serverSockets[1].join("room1");

      clientSockets[0].on("test", () => {
        done(new Error("should not happen"));
      });

      clientSockets[1].on("test", () => {
        done();
      });

      clientSockets[2].on("test", () => {
        done(new Error("should not happen"));
      });

      servers[0].to("room1").emit("test");
    });

    it("broadcasts to all clients except in room", (done) => {
      const partialDone = times(2, done);
      serverSockets[1].join("room1");

      clientSockets[0].on("test", () => {
        partialDone();
      });

      clientSockets[1].on("test", () => {
        done(new Error("should not happen"));
      });

      clientSockets[2].on("test", () => {
        partialDone();
      });

      servers[0].of("/").except("room1").emit("test");
    });

    it("broadcasts to local clients only", (done) => {
      clientSockets[0].on("test", () => {
        done();
      });

      clientSockets[1].on("test", () => {
        done(new Error("should not happen"));
      });

      clientSockets[2].on("test", () => {
        done(new Error("should not happen"));
      });

      servers[0].local.emit("test");
    });

    it("broadcasts with multiple acknowledgements", (done) => {
      clientSockets[0].on("test", (cb) => {
        cb(1);
      });

      clientSockets[1].on("test", (cb) => {
        cb(2);
      });

      clientSockets[2].on("test", (cb) => {
        cb(3);
      });

      servers[0].timeout(500).emit("test", (err: Error, responses: any[]) => {
        expect(err).toEqual(null);
        expect(responses).toContain(1);
        expect(responses).toContain(2);
        expect(responses).toContain(3);

        setTimeout(() => {
          // @ts-ignore
          expect(servers[0].of("/").adapter.ackRequests.size).toEqual(0);

          done();
        }, 500);
      });
    });

    it("broadcasts with multiple acknowledgements (binary content)", (done) => {
      clientSockets[0].on("test", (cb) => {
        cb(Buffer.from([1]));
      });

      clientSockets[1].on("test", (cb) => {
        cb(Buffer.from([2]));
      });

      clientSockets[2].on("test", (cb) => {
        cb(Buffer.from([3]));
      });

      servers[0].timeout(500).emit("test", (err: Error, responses: any[]) => {
        expect(err).toEqual(null);
        responses.forEach((response) => {
          expect(Buffer.isBuffer(response)).toEqual(true);
        });

        done();
      });
    });

    it("broadcasts with multiple acknowledgements (no client)", (done) => {
      servers[0]
        .to("abc")
        .timeout(500)
        .emit("test", (err: Error, responses: any[]) => {
          expect(err).toEqual(null);
          expect(responses).toEqual([]);

          done();
        });
    });

    it("broadcasts with multiple acknowledgements (timeout)", (done) => {
      clientSockets[0].on("test", (cb) => {
        cb(1);
      });

      clientSockets[1].on("test", (cb) => {
        cb(2);
      });

      clientSockets[2].on("test", (cb) => {
        // do nothing
      });

      servers[0].timeout(500).emit("test", (err: Error, responses: any[]) => {
        expect(err).toBeInstanceOf(Error);
        expect(responses).toContain(1);
        expect(responses).toContain(2);

        done();
      });
    });
  });

  describe("socketsJoin", () => {
    it("makes all socket instances join the specified room", async () => {
      servers[0].socketsJoin("room1");

      await sleep(200);

      expect(serverSockets[0].rooms.has("room1")).toEqual(true);
      expect(serverSockets[1].rooms.has("room1")).toEqual(true);
      expect(serverSockets[2].rooms.has("room1")).toEqual(true);
    });

    it("makes the matching socket instances join the specified room", async () => {
      serverSockets[0].join("room1");
      serverSockets[2].join("room1");

      servers[0].in("room1").socketsJoin("room2");

      await sleep(200);

      expect(serverSockets[0].rooms.has("room2")).toEqual(true);
      expect(serverSockets[1].rooms.has("room2")).toEqual(false);
      expect(serverSockets[2].rooms.has("room2")).toEqual(true);
    });

    it("makes the given socket instance join the specified room", async () => {
      servers[0].in(serverSockets[1].id).socketsJoin("room3");

      await sleep(200);

      expect(serverSockets[0].rooms.has("room3")).toEqual(false);
      expect(serverSockets[1].rooms.has("room3")).toEqual(true);
      expect(serverSockets[2].rooms.has("room3")).toEqual(false);
    });
  });

  describe("socketsLeave", () => {
    it("makes all socket instances leave the specified room", async () => {
      serverSockets[0].join("room1");
      serverSockets[2].join("room1");

      servers[0].socketsLeave("room1");

      await sleep(200);

      expect(serverSockets[0].rooms.has("room1")).toEqual(false);
      expect(serverSockets[1].rooms.has("room1")).toEqual(false);
      expect(serverSockets[2].rooms.has("room1")).toEqual(false);
    });

    it("makes the matching socket instances leave the specified room", async () => {
      serverSockets[0].join(["room1", "room2"]);
      serverSockets[1].join(["room1", "room2"]);
      serverSockets[2].join(["room2"]);

      servers[0].in("room1").socketsLeave("room2");

      await sleep(200);

      expect(serverSockets[0].rooms.has("room2")).toEqual(false);
      expect(serverSockets[1].rooms.has("room2")).toEqual(false);
      expect(serverSockets[2].rooms.has("room2")).toEqual(true);
    });

    it("makes the given socket instance leave the specified room", async () => {
      serverSockets[0].join("room3");
      serverSockets[1].join("room3");
      serverSockets[2].join("room3");

      servers[0].in(serverSockets[1].id).socketsLeave("room3");

      await sleep(200);

      expect(serverSockets[0].rooms.has("room3")).toEqual(true);
      expect(serverSockets[1].rooms.has("room3")).toEqual(false);
      expect(serverSockets[2].rooms.has("room3")).toEqual(true);
    });
  });

  describe("disconnectSockets", () => {
    it("makes all socket instances disconnect", (done) => {
      const partialDone = times(3, done);

      clientSockets.forEach((clientSocket) => {
        clientSocket.on("disconnect", (reason) => {
          expect(reason).toEqual("io server disconnect");
          partialDone();
        });
      });

      servers[0].disconnectSockets();
    });
  });

  describe("fetchSockets", () => {
    it("returns all socket instances", async () => {
      const sockets = await servers[0].fetchSockets();

      expect(sockets).toBeInstanceOf(Array);
      expect(sockets).toHaveLength(3);
      // @ts-ignore
      expect(servers[0].of("/").adapter.requests.size).toEqual(0); // clean up
    });

    it("returns a single socket instance", async () => {
      serverSockets[1].data = "test" as any;

      const [remoteSocket] = await servers[0]
        .in(serverSockets[1].id)
        .fetchSockets();

      expect(remoteSocket.handshake).toEqual(serverSockets[1].handshake);
      expect(remoteSocket.data).toEqual("test");
      expect(remoteSocket.rooms.size).toEqual(1);
    });

    it("returns only local socket instances", async () => {
      const sockets = await servers[0].local.fetchSockets();

      expect(sockets).toHaveLength(1);
    });
  });

  describe("serverSideEmit", () => {
    it("sends an event to other server instances", (done) => {
      const partialDone = times(2, done);

      servers[0].serverSideEmit("hello", "world", 1, "2");

      servers[0].on("hello", () => {
        done(new Error("should not happen"));
      });

      servers[1].on("hello", (arg1, arg2, arg3) => {
        expect(arg1).toEqual("world");
        expect(arg2).toEqual(1);
        expect(arg3).toEqual("2");
        partialDone();
      });

      servers[2].of("/").on("hello", () => {
        partialDone();
      });
    });

    it("sends an event and receives a response from the other server instances", (done) => {
      servers[0].serverSideEmit("hello", (err: Error, response: any) => {
        expect(err).toEqual(null);
        expect(response).toBeInstanceOf(Array);
        expect(response).toContain(2);
        expect(response).toContain("3");
        done();
      });

      servers[0].on("hello", () => {
        done(new Error("should not happen"));
      });

      servers[1].on("hello", (cb) => {
        cb(2);
      });

      servers[2].on("hello", (cb) => {
        cb("3");
      });
    });

    it("sends an event but timeout if one server does not respond", (done) => {
      (servers[0].of("/").adapter as MongoAdapter).requestsTimeout = 200;

      servers[0].serverSideEmit("hello", (err: Error, response: any) => {
        expect(err.message).toEqual(
          "timeout reached: only 1 responses received out of 2"
        );
        expect(response).toBeInstanceOf(Array);
        expect(response).toContain(2);
        done();
      });

      servers[0].on("hello", () => {
        done(new Error("should not happen"));
      });

      servers[1].on("hello", (cb) => {
        cb(2);
      });

      servers[2].on("hello", () => {
        // do nothing
      });
    });

    it("should not throw when receiving a drop event", async () => {
      await mongoClient.db("test").dropCollection("events");

      await sleep(100);
    });

    it("should resume the change stream upon reconnection", async () => {
      await mongoClient.close(true);

      return new Promise<void>(async (resolve) => {
        const partialDone = times(3, resolve);
        clientSockets[0].on("test1", partialDone);
        clientSockets[1].on("test2", partialDone);
        clientSockets[2].on("test3", partialDone);

        await mongoClient.connect();
        servers[0].to(clientSockets[1].id).emit("test2");

        await sleep(500);

        servers[1].to(clientSockets[2].id).emit("test3");
        servers[2].to(clientSockets[0].id).emit("test1");
      });
    });
  });
});
