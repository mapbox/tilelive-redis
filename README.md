[![Build Status](https://travis-ci.org/mapbox/tilelive-redis.png?branch=master)](https://travis-ci.org/mapbox/tilelive-redis)

tilelive-redis
------------------
node-tilejson wrapping source for tilelive.

    var options = {
        mode: 'readthrough', // optional, cache mode either 'readthrough' or 'race'
        client: client, // optional, instantiated redis client
        expires: 600    // optional, object expiration time in seconds
    };
    var TileJSON = require('tilelive-redis')(options, require('tilejson'));

    new TileJSON( ... )

### Cache modes

Two modes for caching are available.

- **readthrough** hits redis first and only calls a `get` on the original source if a cache miss occurs.
- **race** always hits both redis and the original source concurrently. The IO operation that completes fastest will handle the `get` call. After both operations are complete the cache may be updated if the original source's contents have changed.
