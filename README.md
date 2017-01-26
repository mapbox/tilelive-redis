[![Build Status](https://travis-ci.org/mapbox/tilelive-redis.png?branch=master)](https://travis-ci.org/mapbox/tilelive-redis)

tilelive-redis
--------------
Module for adding a redis-based caching layer in front a [node-tilejson](https://github.com/mapbox/node-tilejson) tilelive source.

It wraps `node-tilejson`, providing a new source constructor with redis superpowers:

    var options = {
        client: client,             // optional, instantiated redis client
        ttl: <number> or <object>,  // optional, object cache ttl in seconds
        stale: <number> or <object>,// optional, max number of seconds to allow a stale object to be served
        timeout: <number>           // optional, max ms to wait before bypassing redis. Defaults to 50
    };
    var TileJSON = require('tilelive-redis')(options, require('tilejson'));

    new TileJSON( ... )

For `options.ttl` and `options.stale` an object may be provided to specify
different times for different keys. For example:

```js
var options = {
    ttl: {
        'bananas': 5,
        'oranges': 1000
    }
};
```

will set any keys matching the string `bananas` to have a ttl of 5 seconds and
any keys matching `oranges` will be set to have a ttl of 1000 seconds.

### Requirements

Required minimal/supported version of redis-server is 2.8.x

### Command queue high water mark

`node-redis` supports a `command_queue_high_water` option, which tilelive-redis
uses in order to avoid back pressure in the application as a result of a failing
redis server.  tilelive-redis will skip redis and instead request only from the
source once the command queue high water mark is hit.  The default value for
`command_queue_high_water` is set by node-redis and is 1000; set a custom value
in your redis client if you desire.
