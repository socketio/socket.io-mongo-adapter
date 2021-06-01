import { Adapter, BroadcastOptions, Room } from "socket.io-adapter";
import { randomBytes } from "crypto";

const randomId = () => randomBytes(8).toString("hex");
const debug = require("debug")("socket.io-mongo-adapter");

/**
 * Event types, for messages between nodes
 */

enum EventType {
  INITIAL_HEARTBEAT = 1,
  HEARTBEAT,
  BROADCAST,
  SOCKETS_JOIN,
  SOCKETS_LEAVE,
  DISCONNECT_SOCKETS,
  FETCH_SOCKETS,
  FETCH_SOCKETS_RESPONSE,
  SERVER_SIDE_EMIT,
  SERVER_SIDE_EMIT_RESPONSE,
}

interface Request {
  type: EventType;
  resolve: Function;
  timeout: NodeJS.Timeout;
  expected: number;
  current: number;
  responses: any[];
}

/**
 * UID of an emitter using the `@socket.io/mongo-emitter` package
 */
const EMITTER_UID = "emitter";

export interface MongoAdapterOptions {
  /**
   * the name of this node
   * @default a random id
   */
  uid: string;
  /**
   * after this timeout the adapter will stop waiting from responses to request
   * @default 5000
   */
  requestsTimeout: number;
  /**
   * Number of ms between two heartbeats
   * @default 5000
   */
  heartbeatInterval: number;
  /**
   * Number of ms without heartbeat before we consider a node down
   * @default 10000
   */
  heartbeatTimeout: number;
}

/**
 * It seems the `promoteBuffers` option is not always honored, so we manually replace Binary objects by the underlying
 * Buffer objects.
 *
 * Reference:
 * - http://mongodb.github.io/node-mongodb-native/3.6/api/Binary.html
 * - https://jira.mongodb.org/browse/NODE-1421
 */
const replaceBinaryObjectsByBuffers = (obj: any) => {
  if (!obj || typeof obj !== "object") {
    return obj;
  }
  if (obj._bsontype === "Binary" && Buffer.isBuffer(obj.buffer)) {
    return obj.buffer;
  }
  if (Array.isArray(obj)) {
    for (let i = 0; i < obj.length; i++) {
      obj[i] = replaceBinaryObjectsByBuffers(obj[i]);
    }
  } else {
    for (const key in obj) {
      if (Object.prototype.hasOwnProperty.call(obj, key)) {
        obj[key] = replaceBinaryObjectsByBuffers(obj[key]);
      }
    }
  }
  return obj;
};

/**
 * Returns a function that will create a MongoAdapter instance.
 *
 * @param mongoCollection - a MongoDB collection instance
 * @param opts - additional options
 *
 * @public
 */
export function createAdapter(
  mongoCollection: any,
  opts: Partial<MongoAdapterOptions> = {}
) {
  return function (nsp: any) {
    return new MongoAdapter(nsp, mongoCollection, opts);
  };
}

export class MongoAdapter extends Adapter {
  public readonly uid: string;
  public requestsTimeout: number;
  public heartbeatInterval: number;
  public heartbeatTimeout: number;

  private readonly mongoCollection: any;
  private changeStream: any;
  private nodesMap: Map<string, number> = new Map<string, number>(); // uid => timestamp of last message
  private heartbeatTimer: NodeJS.Timeout | undefined;
  private requests: Map<string, Request> = new Map();

  /**
   * Adapter constructor.
   *
   * @param nsp - the namespace
   * @param mongoCollection - a MongoDB collection instance
   * @param opts - additional options
   *
   * @public
   */
  constructor(
    nsp: any,
    mongoCollection: any,
    opts: Partial<MongoAdapterOptions> = {}
  ) {
    super(nsp);
    this.mongoCollection = mongoCollection;
    this.uid = opts.uid || randomId();
    this.requestsTimeout = opts.requestsTimeout || 5000;
    this.heartbeatInterval = opts.heartbeatInterval || 5000;
    this.heartbeatTimeout = opts.heartbeatTimeout || 10000;

    this.initChangeStream();
    this.publish({
      type: EventType.INITIAL_HEARTBEAT,
    });
  }

  close(): Promise<void> | void {
    if (this.changeStream) {
      this.changeStream.removeAllListeners("close");
      this.changeStream.close();
    }
    if (this.heartbeatTimer) {
      clearTimeout(this.heartbeatTimer);
    }
  }

  private initChangeStream() {
    if (this.changeStream) {
      this.changeStream.removeAllListeners("change");
      this.changeStream.removeAllListeners("close");
    }
    this.changeStream = this.mongoCollection.watch([
      {
        $match: {
          "fullDocument.nsp": {
            $eq: this.nsp.name, // ignore events from other namespaces
          },
          "fullDocument.uid": {
            $ne: this.uid, // ignore events from self
          },
        },
      },
    ]);
    this.changeStream.on("change", this.onEvent.bind(this));
    this.changeStream.on("close", () => {
      debug("change stream was closed, scheduling reconnection...");
      setTimeout(() => {
        this.initChangeStream();
      }, 1000);
    });
  }

  public async onEvent(event: any) {
    const document = event.fullDocument;
    debug(
      "new event of type %d for %s from %s",
      document.type,
      document.nsp,
      document.uid
    );

    if (document.uid && document.uid !== EMITTER_UID) {
      this.nodesMap.set(document.uid, Date.now());
    }

    switch (document.type) {
      case EventType.INITIAL_HEARTBEAT: {
        this.publish({
          type: EventType.HEARTBEAT,
        });
        break;
      }
      case EventType.BROADCAST: {
        debug("broadcast with opts %j", document.data.opts);
        super.broadcast(
          replaceBinaryObjectsByBuffers(document.data.packet),
          MongoAdapter.deserializeOptions(document.data.opts)
        );
        break;
      }
      case EventType.SOCKETS_JOIN: {
        debug("calling addSockets with opts %j", document.data.opts);
        super.addSockets(
          MongoAdapter.deserializeOptions(document.data.opts),
          document.data.rooms
        );
        break;
      }
      case EventType.SOCKETS_LEAVE: {
        debug("calling delSockets with opts %j", document.data.opts);
        super.delSockets(
          MongoAdapter.deserializeOptions(document.data.opts),
          document.data.rooms
        );
        break;
      }
      case EventType.DISCONNECT_SOCKETS: {
        debug("calling disconnectSockets with opts %j", document.data.opts);
        super.disconnectSockets(
          MongoAdapter.deserializeOptions(document.data.opts),
          document.data.close
        );
        break;
      }
      case EventType.FETCH_SOCKETS: {
        debug("calling fetchSockets with opts %j", document.data.opts);
        const localSockets = await super.fetchSockets(
          MongoAdapter.deserializeOptions(document.data.opts)
        );

        this.publish({
          type: EventType.FETCH_SOCKETS_RESPONSE,
          data: {
            requestId: document.data.requestId,
            sockets: localSockets.map((socket) => ({
              id: socket.id,
              handshake: socket.handshake,
              rooms: [...socket.rooms],
              data: socket.data,
            })),
          },
        });
        break;
      }
      case EventType.FETCH_SOCKETS_RESPONSE: {
        const request = this.requests.get(document.data.requestId);

        if (!request) {
          return;
        }

        request.current++;
        document.data.sockets.forEach((socket: any) =>
          request.responses.push(socket)
        );

        if (request.current === request.expected) {
          clearTimeout(request.timeout);
          request.resolve(request.responses);
          this.requests.delete(document.data.requestId);
        }
        break;
      }
      case EventType.SERVER_SIDE_EMIT: {
        const packet = document.data.packet;
        const withAck = document.data.requestId !== undefined;
        if (!withAck) {
          this.nsp._onServerSideEmit(packet);
          return;
        }
        let called = false;
        const callback = (arg: any) => {
          // only one argument is expected
          if (called) {
            return;
          }
          called = true;
          debug("calling acknowledgement with %j", arg);
          this.publish({
            type: EventType.SERVER_SIDE_EMIT_RESPONSE,
            data: {
              requestId: document.data.requestId,
              packet: arg,
            },
          });
        };

        packet.push(callback);
        this.nsp._onServerSideEmit(packet);
        break;
      }
      case EventType.SERVER_SIDE_EMIT_RESPONSE: {
        const request = this.requests.get(document.data.requestId);

        if (!request) {
          return;
        }

        request.current++;
        request.responses.push(document.data.packet);

        if (request.current === request.expected) {
          clearTimeout(request.timeout);
          request.resolve(null, request.responses);
          this.requests.delete(document.data.requestId);
        }
      }
    }
  }

  private scheduleHeartbeat() {
    if (this.heartbeatTimer) {
      clearTimeout(this.heartbeatTimer);
    }
    this.heartbeatTimer = setTimeout(() => {
      this.publish({
        type: EventType.HEARTBEAT,
      });
      this.scheduleHeartbeat();
    }, this.heartbeatInterval);
  }

  private publish(document: any) {
    document.uid = this.uid;
    document.nsp = this.nsp.name;

    this.mongoCollection.insertOne(document);

    this.scheduleHeartbeat();
  }

  /**
   * Transform ES6 Set into plain arrays
   */
  private static serializeOptions(opts: BroadcastOptions) {
    return {
      rooms: [...opts.rooms],
      except: opts.except ? [...opts.except] : [],
      flags: opts.flags,
    };
  }

  private static deserializeOptions(opts: any): BroadcastOptions {
    return {
      rooms: new Set(opts.rooms),
      except: new Set(opts.except),
      flags: opts.flags,
    };
  }

  public broadcast(packet: any, opts: BroadcastOptions) {
    const onlyLocal = opts?.flags?.local;
    if (!onlyLocal) {
      this.publish({
        type: EventType.BROADCAST,
        data: {
          packet,
          opts: MongoAdapter.serializeOptions(opts),
        },
      });
    }

    // packets with binary contents are modified by the broadcast method, hence the nextTick()
    process.nextTick(() => {
      super.broadcast(packet, opts);
    });
  }

  addSockets(opts: BroadcastOptions, rooms: Room[]) {
    super.addSockets(opts, rooms);

    const onlyLocal = opts.flags?.local;
    if (onlyLocal) {
      return;
    }

    this.publish({
      type: EventType.SOCKETS_JOIN,
      data: {
        opts: MongoAdapter.serializeOptions(opts),
        rooms,
      },
    });
  }

  delSockets(opts: BroadcastOptions, rooms: Room[]) {
    super.delSockets(opts, rooms);

    const onlyLocal = opts.flags?.local;
    if (onlyLocal) {
      return;
    }

    this.publish({
      type: EventType.SOCKETS_LEAVE,
      data: {
        opts: MongoAdapter.serializeOptions(opts),
        rooms,
      },
    });
  }

  disconnectSockets(opts: BroadcastOptions, close: boolean) {
    super.disconnectSockets(opts, close);

    const onlyLocal = opts.flags?.local;
    if (onlyLocal) {
      return;
    }

    this.publish({
      type: EventType.DISCONNECT_SOCKETS,
      data: {
        opts: MongoAdapter.serializeOptions(opts),
        close,
      },
    });
  }

  private getExpectedResponseCount() {
    this.nodesMap.forEach((lastSeen, uid) => {
      const nodeSeemsDown = Date.now() - lastSeen > this.heartbeatTimeout;
      if (nodeSeemsDown) {
        debug("node %s seems down", uid);
        this.nodesMap.delete(uid);
      }
    });
    return this.nodesMap.size;
  }

  async fetchSockets(opts: BroadcastOptions): Promise<any[]> {
    const localSockets = await super.fetchSockets(opts);
    const expectedResponseCount = this.getExpectedResponseCount();

    if (opts.flags?.local || expectedResponseCount === 0) {
      return localSockets;
    }

    const requestId = randomId();

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const storedRequest = this.requests.get(requestId);
        if (storedRequest) {
          reject(
            new Error(
              `timeout reached: only ${storedRequest.current} responses received out of ${storedRequest.expected}`
            )
          );
          this.requests.delete(requestId);
        }
      }, this.requestsTimeout);

      const storedRequest = {
        type: EventType.FETCH_SOCKETS,
        resolve,
        timeout,
        current: 0,
        expected: expectedResponseCount,
        responses: localSockets,
      };
      this.requests.set(requestId, storedRequest);

      this.publish({
        type: EventType.FETCH_SOCKETS,
        data: {
          opts: MongoAdapter.serializeOptions(opts),
          requestId,
        },
      });
    });
  }

  public serverSideEmit(packet: any[]): void {
    const withAck = typeof packet[packet.length - 1] === "function";

    if (withAck) {
      this.serverSideEmitWithAck(packet).catch(() => {
        // ignore errors
      });
      return;
    }

    this.publish({
      type: EventType.SERVER_SIDE_EMIT,
      data: {
        packet,
      },
    });
  }

  private async serverSideEmitWithAck(packet: any[]) {
    const ack = packet.pop();
    const expectedResponseCount = this.getExpectedResponseCount();

    debug(
      'waiting for %d responses to "serverSideEmit" request',
      expectedResponseCount
    );

    if (expectedResponseCount <= 0) {
      return ack(null, []);
    }

    const requestId = randomId();

    const timeout = setTimeout(() => {
      const storedRequest = this.requests.get(requestId);
      if (storedRequest) {
        ack(
          new Error(
            `timeout reached: only ${storedRequest.current} responses received out of ${storedRequest.expected}`
          ),
          storedRequest.responses
        );
        this.requests.delete(requestId);
      }
    }, this.requestsTimeout);

    const storedRequest = {
      type: EventType.FETCH_SOCKETS,
      resolve: ack,
      timeout,
      current: 0,
      expected: expectedResponseCount,
      responses: [],
    };
    this.requests.set(requestId, storedRequest);

    this.publish({
      type: EventType.SERVER_SIDE_EMIT,
      data: {
        requestId, // the presence of this attribute defines whether an acknowledgement is needed
        packet,
      },
    });
  }
}
