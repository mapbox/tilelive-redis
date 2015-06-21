[![Build Status](https://travis-ci.org/mapbox/tilelive-redis.png?branch=master)](https://travis-ci.org/mapbox/tilelive-redis)

tilelive-redis
--------------
Module for adding a redis-based caching layer in front a [node-tilejson](https://github.com/mapbox/node-tilejson) tilelive source.

It wraps `node-tilejson`, providing a new source constructor with redis superpowers:

    var options = {
        mode: 'readthrough', // optional, cache mode either 'readthrough' or 'race'
        client: client, // optional, instantiated redis client
        expires: 600    // optional, object expiration time in seconds
    };
    var TileJSON = require('tilelive-redis')(options, require('tilejson'));

    new TileJSON( ... )

### Requirements

Required minimal/supported version of redis-server is 2.8.x

### Cache modes

Two modes for caching are available.

- **readthrough** hits redis first and only calls a `get` on the original source if a cache miss occurs.
- **race** always hits both redis and the original source concurrently. The IO operation that completes fastest will handle the `get` call. After both operations are complete the cache may be updated if the original source's contents have changed.

### Command queue high water mark

`node-redis` supports a `command_queue_high_water` option, which tilelive-redis
uses in order to avoid back pressure in the application as a result of a failing
redis server.  tilelive-redis will skip redis and instead request only from the
source once the command queue high water mark is hit.  The default value for
`command_queue_high_water` is set by node-redis and is 1000; set a custom value
in your redis client if you desire.
