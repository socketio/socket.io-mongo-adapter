import {
  Adapter,
  BroadcastOptions,
  PrivateSessionId,
  Room,
  Session,
} from "socket.io-adapter";
import { randomBytes } from "crypto";
import { ObjectId, MongoServerError } from "mongodb";
import type { Collection, ChangeStream, ResumeToken } from "mongodb";

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
  BROADCAST_CLIENT_COUNT,
  BROADCAST_ACK,
  SESSION,
}

/**
 * The format of the documents in the MongoDB collection
 */
interface AdapterEvent {
  /**
   * The type of the event
   */
  type: EventType;
  /**
   * The UID of the server, to filter event created by itself (see watch() call)
   */
  uid?: string;
  /**
   * The namespace
   */
  nsp?: string;
  /**
   * The date of creation of the event, to be able to manually clean up the collection.
   *
   * @see MongoAdapterOptions.addCreatedAtField
   */
  createdAt?: Date;
  /**
   * Some additional data, depending on the event type
   */
  data?: any;
}

interface Request {
  type: EventType;
  resolve: Function;
  timeout: NodeJS.Timeout;
  expected: number;
  current: number;
  responses: any[];
}

interface AckRequest {
  type: EventType.BROADCAST;
  clientCountCallback: (clientCount: number) => void;
  ack: (...args: any[]) => void;
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

  /**
   * Add a createdAt field to each MongoDB document
   * @default false
   */
  addCreatedAtField: boolean;
}

/**
 * It seems the `promoteBuffers` option is not always honored, so we manually replace Binary objects by the underlying
 * Buffer objects.
 *
 * Update: it seems to be fixed with `mongodb@4`, but we'll keep it for backward compatibility
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
  mongoCollection: Collection,
  opts: Partial<MongoAdapterOptions> = {}
) {
  opts.uid = opts.uid || randomId();

  let isClosed = false;
  let adapters = new Map<string, MongoAdapter>();
  let changeStream: ChangeStream;
  let resumeToken: ResumeToken;

  const initChangeStream = () => {
    if (isClosed || (changeStream && !changeStream.closed)) {
      return;
    }
    debug("opening change stream");
    changeStream = mongoCollection.watch(
      [
        {
          $match: {
            "fullDocument.uid": {
              $ne: opts.uid, // ignore events from self
            },
          },
        },
      ],
      {
        resumeAfter: resumeToken,
      }
    );

    changeStream.on("change", (event: any) => {
      if (event.operationType === "insert") {
        resumeToken = changeStream.resumeToken;
        adapters.get(event.fullDocument?.nsp)?.onEvent(event);
      }
    });

    changeStream.on("error", (err: Error) => {
      debug("change stream encountered an error: %s", err.message);
      if (
        err instanceof MongoServerError &&
        !err.hasErrorLabel("ResumableChangeStreamError")
      ) {
        // the resume token was not found in the oplog
        resumeToken = null;
      }
    });

    changeStream.on("close", () => {
      debug("change stream was closed, scheduling reconnection...");
      setTimeout(() => {
        initChangeStream();
      }, 1000);
    });
  };

  return function (nsp: any) {
    if (!changeStream) {
      isClosed = false;
      initChangeStream();
    }

    let adapter = new MongoAdapter(nsp, mongoCollection, opts);

    adapters.set(nsp.name, adapter);

    const defaultClose = adapter.close;

    adapter.close = () => {
      adapters.delete(nsp.name);

      if (adapters.size === 0) {
        changeStream.removeAllListeners("close");
        changeStream.close();
        // @ts-ignore
        changeStream = null;
        isClosed = true;
      }

      defaultClose.call(adapter);
    };

    return adapter;
  };
}

export class MongoAdapter extends Adapter {
  public readonly uid: string;
  public requestsTimeout: number;
  public heartbeatInterval: number;
  public heartbeatTimeout: number;
  public addCreatedAtField: boolean;

  private readonly mongoCollection: Collection;
  private nodesMap: Map<string, number> = new Map<string, number>(); // uid => timestamp of last message
  private heartbeatTimer: NodeJS.Timeout | undefined;
  private requests: Map<string, Request> = new Map();
  private ackRequests: Map<string, AckRequest> = new Map();
  private isClosed = false;

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
    mongoCollection: Collection,
    opts: Partial<MongoAdapterOptions> = {}
  ) {
    super(nsp);
    this.mongoCollection = mongoCollection;
    this.uid = opts.uid!;
    this.requestsTimeout = opts.requestsTimeout || 5000;
    this.heartbeatInterval = opts.heartbeatInterval || 5000;
    this.heartbeatTimeout = opts.heartbeatTimeout || 10000;
    this.addCreatedAtField = !!opts.addCreatedAtField;

    this.publish({
      type: EventType.INITIAL_HEARTBEAT,
    });
  }

  close(): Promise<void> | void {
    this.isClosed = true;
    if (this.heartbeatTimer) {
      clearTimeout(this.heartbeatTimer);
    }
    return;
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

        const withAck = document.data.requestId !== undefined;
        if (withAck) {
          super.broadcastWithAck(
            replaceBinaryObjectsByBuffers(document.data.packet),
            MongoAdapter.deserializeOptions(document.data.opts),
            (clientCount) => {
              debug("waiting for %d client acknowledgements", clientCount);
              this.publish({
                type: EventType.BROADCAST_CLIENT_COUNT,
                data: {
                  requestId: document.data.requestId,
                  clientCount,
                },
              });
            },
            (arg) => {
              debug("received acknowledgement with value %j", arg);
              this.publish({
                type: EventType.BROADCAST_ACK,
                data: {
                  requestId: document.data.requestId,
                  packet: arg,
                },
              });
            }
          );
        } else {
          const packet = replaceBinaryObjectsByBuffers(document.data.packet);
          const opts = MongoAdapter.deserializeOptions(document.data.opts);

          this.addOffsetIfNecessary(packet, opts, document._id);

          super.broadcast(packet, opts);
        }
        break;
      }

      case EventType.BROADCAST_CLIENT_COUNT: {
        const request = this.ackRequests.get(document.data.requestId);
        request?.clientCountCallback(document.data.clientCount);
        break;
      }

      case EventType.BROADCAST_ACK: {
        const request = this.ackRequests.get(document.data.requestId);
        const clientResponse = replaceBinaryObjectsByBuffers(
          document.data.packet
        );
        request?.ack(clientResponse);
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
      debug("sending heartbeat");
      this.publish({
        type: EventType.HEARTBEAT,
      });
      this.scheduleHeartbeat();
    }, this.heartbeatInterval);
  }

  private publish(document: AdapterEvent): Promise<string> {
    if (this.isClosed) {
      return Promise.reject("adapter is closed");
    }
    debug("publish document %d", document.type);
    document.uid = this.uid;
    document.nsp = this.nsp.name;

    if (this.addCreatedAtField) {
      document.createdAt = new Date();
    }

    this.scheduleHeartbeat();

    return this.mongoCollection
      .insertOne(document)
      .then((result) => result.insertedId.toString("hex"));
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

  public async broadcast(packet: any, opts: BroadcastOptions) {
    const onlyLocal = opts?.flags?.local;
    if (!onlyLocal) {
      try {
        const offset = await this.publish({
          type: EventType.BROADCAST,
          data: {
            packet,
            opts: MongoAdapter.serializeOptions(opts),
          },
        });
        this.addOffsetIfNecessary(packet, opts, offset);
      } catch (err) {
        debug("error while inserting document: %s", err);
        return;
      }
    }

    // packets with binary contents are modified by the broadcast method, hence the nextTick()
    // update: this should be fixed now, but we'll keep it for backward compatibility
    // see: https://github.com/socketio/socket.io-parser/commit/ae8dd88995dbd7f89c97e5cc15e5b489fa0efece
    process.nextTick(() => {
      super.broadcast(packet, opts);
    });
  }

  /**
   * Adds an offset at the end of the data array in order to allow the client to receive any missed packets when it
   * reconnects after a temporary disconnection.
   *
   * @param packet
   * @param opts
   * @param offset
   * @private
   */
  private addOffsetIfNecessary(
    packet: any,
    opts: BroadcastOptions,
    offset: string
  ) {
    if (!this.nsp.server.opts.connectionStateRecovery) {
      return;
    }
    const isEventPacket = packet.type === 2;
    // packets with acknowledgement are not stored because the acknowledgement function cannot be serialized and
    // restored on another server upon reconnection
    const withoutAcknowledgement = packet.id === undefined;
    const notVolatile = opts.flags?.volatile === undefined;

    if (isEventPacket && withoutAcknowledgement && notVolatile) {
      packet.data.push(offset);
    }
  }

  public broadcastWithAck(
    packet: any,
    opts: BroadcastOptions,
    clientCountCallback: (clientCount: number) => void,
    ack: (...args: any[]) => void
  ) {
    const onlyLocal = opts?.flags?.local;
    if (!onlyLocal) {
      const requestId = randomId();

      this.publish({
        type: EventType.BROADCAST,
        data: {
          packet,
          requestId,
          opts: MongoAdapter.serializeOptions(opts),
        },
      });

      this.ackRequests.set(requestId, {
        type: EventType.BROADCAST,
        clientCountCallback,
        ack,
      });

      // we have no way to know at this level whether the server has received an acknowledgement from each client, so we
      // will simply clean up the ackRequests map after the given delay
      setTimeout(() => {
        this.ackRequests.delete(requestId);
      }, opts.flags!.timeout);
    }

    // packets with binary contents are modified by the broadcast method, hence the nextTick()
    process.nextTick(() => {
      super.broadcastWithAck(packet, opts, clientCountCallback, ack);
    });
  }

  public serverCount(): Promise<number> {
    return Promise.resolve(1 + this.nodesMap.size);
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

  override persistSession(session: any) {
    debug("persisting session: %j", session);
    this.publish({
      type: EventType.SESSION,
      data: session,
    });
  }

  override async restoreSession(
    pid: PrivateSessionId,
    offset: string
  ): Promise<Session> {
    if (!ObjectId.isValid(offset)) {
      return Promise.reject("invalid offset");
    }
    debug("restoring session: %s", pid);
    const eventOffset = new ObjectId(offset);

    let results;
    try {
      results = await Promise.all([
        // could use a sparse index on [data.pid] (only index the documents whose type is EventType.SESSION)
        this.mongoCollection.findOneAndDelete({
          type: EventType.SESSION,
          "data.pid": pid,
        }),
        this.mongoCollection.findOne({
          type: EventType.BROADCAST,
          _id: eventOffset,
        }),
      ]);
    } catch (e) {
      return Promise.reject("error while fetching session");
    }

    if (!results[0].value || !results[1]) {
      return Promise.reject("session or offset not found");
    }

    const session = results[0].value.data;

    // could use a sparse index on [_id, data.opts.rooms, data.opts.except] (only index the documents whose type is EventType.BROADCAST)
    const cursor = this.mongoCollection.find({
      $and: [
        {
          type: EventType.BROADCAST,
        },
        {
          _id: {
            $gt: eventOffset,
          },
        },
        {
          nsp: this.nsp.name,
        },
        {
          $or: [
            {
              "data.opts.rooms": {
                $size: 0,
              },
            },
            {
              "data.opts.rooms": {
                $in: session.rooms,
              },
            },
          ],
        },
        {
          $or: [
            {
              "data.opts.except": {
                $size: 0,
              },
            },
            {
              "data.opts.except": {
                $nin: session.rooms,
              },
            },
          ],
        },
      ],
    });

    session.missedPackets = [];

    try {
      await cursor.forEach((document: any) => {
        const packetData = document?.data?.packet?.data;
        if (packetData) {
          session.missedPackets.push(packetData);
        }
      });
    } catch (e) {
      return Promise.reject("error while fetching missed packets");
    }

    return session;
  }
}
