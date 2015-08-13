var urlParse = require('url').parse;
var util = require('util');
var redis = require('redis');

module.exports = function(options, Source) {
    if (!Source) throw new Error('No source provided');
    if (!Source.prototype.get) throw new Error('No get method found on source');

    function Caching() { return Source.apply(this, arguments) };

    // Inheritance.
    util.inherits(Caching, Source);

    // References for testing, convenience, post-call overriding.
    Caching.redis = options;

    Caching.prototype.get = module.exports.cachingGet('TL2', options, Source.prototype.get);

    return Caching;
};

module.exports.cachingGet = function(namespace, options, get) {
    if (!get) throw new Error('No get function provided');
    if (!namespace) throw new Error('No namespace provided');

    options = options || {};
    if (options.client) {
        if (options.client.options.return_buffers !== true) {
            throw new Error('Provided redis client does not expect buffers');
        }
    } else {
        options.client = redis.createClient({return_buffers: true});
    }
    options.expires = ('expires' in options) ? options.expires : 300;
    options.mode = ('mode' in options) ? options.mode : 'readthrough';

    if (!options.client) throw new Error('No redis client');
    if (!options.expires) throw new Error('No expires option set');

    var caching;
    if (options.mode === 'readthrough') {
        caching = readthrough;
    } else if (options.mode === 'race') {
        caching = race;
    } else {
        throw new Error('Invalid value for options.mode ' + options.mode);
    }

    function detectExpiry(url, options) {
        if (typeof options.expires === 'number') {
            return options.expires;
        }
        return options.expires[urlParse(url).hostname] || options.expires.default || 300;
    }

    // Allow command_queue_high_water to act as throttle to prevent
    // back pressure from what may be an ailing redis-server
    function checkCommandQueue(client, key) {
        var node = client.clusterNodeLookup ? client.clusterNodeLookup(key) : client;
        return node.command_queue.length < node.command_queue_high_water
    }

    function race(url, callback) {
        var key = namespace + '-' + url;
        var source = this;
        var client = options.client;
        var expires = detectExpiry(url, options);
        var sent = false;
        var cached = null;
        var current = null;

        // GET upstream.
        get.call(source, url, function(err, buffer, headers) {
            current = encode(err, buffer, headers);
            if (cached && current) finalize();
            if (sent) return;
            sent = true;
            callback(err, buffer, headers);
        });

        // GET redis.
        if (checkCommandQueue(client, key)) {
            client.get(key, function(err, encoded) {
                // If error on redis, do not flip first flag.
                // Finalize will never occur (no cache set).
                if (err) return (err.key = key) && client.emit('error', err);

                cached = encoded || '500';
                if (cached && current) finalize();
                if (sent || !encoded) return;
                var data;
                try {
                    data = decode(cached);
                } catch(err) {
                    (err.key = key) && client.emit('error', err);
                    cached = '500';
                }
                if (data) {
                    sent = true;
                    callback(data.err, data.buffer, data.headers);
                }
            });
        } else {
            client.emit('error', new Error('Redis command queue at high water mark'));
        }

        function finalize() {
            if (cached === current) return;
            client.setex(key, expires, current, function(err) {
                if (!err) return;
                err.key = key;
                client.emit('error', err);
            });
        }
    };

    function readthrough(url, callback) {
        var key = namespace + '-' + url;
        var source = this;
        var client = options.client;
        var expires = detectExpiry(url, options);

        if (checkCommandQueue(client, key)) {
            client.get(key, function(err, encoded) {
                // If error on redis get, pass through to original source
                // without attempting a set after retrieval.
                if (err) {
                    err.key = key;
                    client.emit('error', err);
                    return get(url, callback);
                }

                // Cache hit.
                var data;
                if (encoded) try {
                    data = decode(encoded);
                } catch(err) {
                    err.key = key;
                    client.emit('error', err);
                }
                if (data) return callback(data.err, data.buffer, data.headers);

                // Cache miss, error, or otherwise no data
                get.call(source, url, function(err, buffer, headers) {
                    if (err && !errcode(err)) return callback(err);
                    callback(err, buffer, headers);
                    // Callback does not need to wait for redis set to occur.
                    client.setex(key, expires, encode(err, buffer, headers), function(err) {
                        if (!err) return;
                        err.key = key;
                        client.emit('error', err);
                    });
                });
            });
        } else {
            client.emit('error', new Error('Redis command queue at high water mark'));
            return get.call(source, url, callback);
        }
    };

    return caching;
};

module.exports.createRedisClusterClient = function(servers, options) {
    var HashRing = require('hashring');

    // Copy from cachingGet
    options.expires = ('expires' in options) ? options.expires : 300;
    options.mode = ('mode' in options) ? options.mode : 'readthrough';
    // end copy

    var client = {};
    // Copy options onto our client wrapper so they can be examined.
    client.options = options;

    var ring = new HashRing(servers);
    var nodes = servers.reduce(function(m, v) {
        var l = v.split(':');
        m[v] = (options._redis || redis).createClient.call(this, l[1], l[0], options);
        return m;
    }, {});

    client.clusterNodes = function() {
        return Object.keys(nodes).map(function(k) { return nodes[k]; });
    };
    client.clusterNodeLookup = function(key) { return nodes[ring.get(key)]; };

    // Proxy limited set of methods
    ['get', 'setex'].forEach(function(method) {
        client[method] = function() {
            var node = ring.get(arguments[0]);
            nodes[node][method].apply(nodes[node], arguments);
        };
    });
    client.flushdb = function(done) {
        var cnt = 0;
        var len = client.clusterNodes.length;
        function wait() {
            if (cnt === len) done();
            cnt++;
        }
        client.clusterNodes().forEach(function(n) {
            n.flushdb(wait);
        });
    };
    return client;
};

module.exports.redis = redis;
module.exports.encode = encode;
module.exports.decode = decode;

function errcode(err) {
    if (!err) return;
    if (err.status === 404) return 404;
    if (err.status === 403) return 403;
    if (err.code === 404) return 404;
    if (err.code === 403) return 403;
    return;
}

function encode(err, buffer, headers) {
    if (errcode(err)) return errcode(err).toString();

    // Unhandled error.
    if (err) return null;

    headers = headers || {};

    // Turn objects into JSON string buffers.
    if (buffer && typeof buffer === 'object' && !(buffer instanceof Buffer)) {
        headers['x-redis-json'] = true;
        buffer = new Buffer(JSON.stringify(buffer));
    // Turn strings into buffers.
    } else if (buffer && !(buffer instanceof Buffer)) {
        buffer = new Buffer(buffer);
    }

    headers = new Buffer(JSON.stringify(headers), 'utf8');

    if (headers.length > 1024) {
        throw new Error('Invalid cache value - headers exceed 1024 bytes: ' + JSON.stringify(headers));
    }

    var padding = new Buffer(1024 - headers.length);
    padding.fill(' ');
    var len = headers.length + padding.length + buffer.length;
    return Buffer.concat([headers, padding, buffer], len);
};

function decode(encoded) {
    if (encoded.length == 3) {
        encoded = encoded.toString();
        if (encoded === '404' || encoded === '403') {
            var err = new Error();
            err.code = parseInt(encoded, 10);
            err.status = parseInt(encoded, 10);
            err.redis = true;
            return { err: err };
        }
    }

    // First 1024 bytes reserved for header + padding.
    var offset = 1024;
    var data = {};
    data.headers = encoded.slice(0, offset).toString().trim();

    try {
        data.headers = JSON.parse(data.headers);
    } catch(e) {
        throw new Error('Invalid cache value');
    }

    data.headers['x-redis'] = 'hit';
    data.buffer = encoded.slice(offset);

    // Return JSON-encoded objects to true form.
    if (data.headers['x-redis-json']) data.buffer = JSON.parse(data.buffer);

    if (data.headers['content-length'] && data.headers['content-length'] != data.buffer.length)
        throw new Error('Content length does not match');
    return data;
};
