## [0.2.1](https://github.com/socketio/socket.io-mongo-adapter/compare/0.2.0...0.2.1) (2022-05-03)


### Bug Fixes

* properly handle invalidate events ([938674d](https://github.com/socketio/socket.io-mongo-adapter/commit/938674d101fc01add3b6e01d59d20c2aa84b48eb))



# [0.2.0](https://github.com/socketio/socket.io-mongo-adapter/compare/0.1.0...0.2.0) (2022-04-27)


### Features

* add an option to use a TTL index ([#4](https://github.com/socketio/socket.io-mongo-adapter/issues/4)) ([7fdbb25](https://github.com/socketio/socket.io-mongo-adapter/commit/7fdbb25831255e5f6a37a5df25b4fc41c770ab6a))

The `addCreatedAtField` option allows to use a TTL index instead of a capped collection, which is slightly less efficient but more predictable.

* broadcast and expect multiple acks ([e87a0ce](https://github.com/socketio/socket.io-mongo-adapter/commit/e87a0cec4c6920b5e4ef38c4de3e45c1eba5e4cf))

This feature was added in `socket.io@4.5.0`:

```js
io.timeout(1000).emit("some-event", (err, responses) => {
  // ...
});
```

Thanks to this change, it will now work with multiple Socket.IO servers.

* use a single stream for all namespaces ([9b5f4c8](https://github.com/socketio/socket.io-mongo-adapter/commit/9b5f4c83038cc212b898b7fb7ff0ccec3124447c))

The adapter will now create one single MongoDB stream for all namespaces, instead of one per namespace, which could lead to performance issues.

# 0.1.0 (2021-06-01)

Initial commit

