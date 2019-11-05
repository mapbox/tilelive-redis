var assert = require('assert');
var bufferEqual = require('buffer-equal');
var Redsource = require('../index');
var redis = Redsource.redis;
var deadclient = redis.createClient(6380, '127.0.0.1', {
    retry_max_delay: 500
});

deadclient.on('error', function(err) {
    // No op.  Otherwise errors out the tests.
});

var Testsource = require('./testsource');
var tiles = Testsource.tiles;
var grids = Testsource.grids;
var now = Testsource.now;

describe('load', function() {
    it('fails without source', function(done) {
        assert.throws(function() { Redsource({}) });
        assert.throws(function() { Redsource({}, {}) });
        done();
    });
    it('loads + sets default values', function(done) {
        var Source = Redsource({}, Testsource);
        assert.ok(Source.redis);
        assert.ok(Source.redis.client);
        assert.ok(Source.redis.ttl, 300);
        new Source('fakeuri', function(err, source) {
            assert.ifError(err);
            assert.ok(source instanceof Testsource);
            assert.equal(source._uri, 'fakeuri');
            done();
        });
    });
    it('sets ttl from opts', function(done) {
        var Source = Redsource({ ttl:5 }, Testsource);
        assert.ok(Source.redis);
        assert.ok(Source.redis.ttl, 5);
        done();
    });
    it('sets client from opts', function(done) {
        var client = redis.createClient({return_buffers: true});
        var Source = Redsource({ client: client, ttl:5 }, Testsource);
        assert.ok(Source.redis);
        assert.strictEqual(Source.redis.client, client);
        done();
    });
});

describe('getConfig', function() {
    it('creates config', function(done) {
        assert.deepEqual(Redsource.getConfig({}), {
            ttl: [ { pattern: new RegExp(''), value:300 } ],
            stale: [ { pattern: new RegExp(''), value:300 } ]
        });
        assert.deepEqual(Redsource.getConfig({
            ttl: 500,
            stale: 1000
        }), {
            ttl: [ { pattern: new RegExp(''), value:500 } ],
            stale: [ { pattern: new RegExp(''), value:1000 } ]
        });
        assert.deepEqual(Redsource.getConfig({
            ttl: { bananas: 500 },
            stale: { oranges: 1000 }
        }), {
            ttl: [ { pattern: new RegExp('bananas'), value:500 } ],
            stale: [ { pattern: new RegExp('oranges'), value:1000 } ]
        });
        done();
    });
});

var tile = function(expected, cached, done) {
    return function(err, data, headers) {
        assert.ifError(err);
        assert.ok(data instanceof Buffer);
        assert.ok(cached ? headers['x-redis'] : !headers['x-redis']);
        assert[cached ? 'deepEqual' : 'strictEqual'](data, expected);
        assert.equal(data.length, expected.length);
        assert.equal(headers['content-type'], 'image/png');
        assert.equal(headers['last-modified'], now.toUTCString());
        done();
    };
};
var grid = function(expected, cached, done) {
    return function(err, data, headers) {
        assert.ifError(err);
        assert.ok(cached ? headers['x-redis'] : !headers['x-redis']);
        assert.deepEqual(data, expected);
        assert.equal(headers['content-type'], 'application/json');
        assert.equal(headers['last-modified'], now.toUTCString());
        done();
    };
};
var error = function(message, cached, done) {
    return function(err, data, headers) {
        assert.ok(cached ? err.redis : !err.redis);
        assert.equal(err.message, message);
        done();
    };
};

describe('relay', function() {
    var source;
    var longsource;
    var deadsource;
    var stalesource;
    before(function(done) {
        var Source = Redsource({}, Testsource);
        Source.redis.client.flushdb(done);
    });
    before(function(done) {
        var Source = Redsource({
            stale: 1,
            ttl: 1
        }, Testsource);
        new Source({ delay:50 }, function(err, redsource) {
            if (err) throw err;
            source = redsource;
            done();
        });
    });
    before(function(done) {
        var Source = Redsource({
            ttl: 300
        }, Testsource);
        new Source({ hostname:'long', delay:50 }, function(err, redsource) {
            if (err) throw err;
            longsource = redsource;
            done();
        });
    });
    before(function(done) {
        var Source = Redsource({
            ttl: 1
        }, Testsource);
        new Source({ hostname:'stale', delay:50 }, function(err, redsource) {
            if (err) throw err;
            stalesource = redsource;
            done();
        });
    });
    before(function(done) {
        var Dead = Redsource({ stale: 1, client:deadclient }, Testsource);
        new Dead({ delay:50 }, function(err, redsource) {
            if (err) throw err;
            deadsource = redsource;
            done();
        });
    });
    it('tile 200 a miss', function(done) {
        source.getTile(0, 0, 0, tile(tiles.a, false, done));
    });
    it('tile 200 a hit', function(done) {
        source.getTile(0, 0, 0, tile(tiles.a, true, done));
    });
    it('tile 200 b miss', function(done) {
        source.getTile(1, 0, 0, tile(tiles.b, false, done));
    });
    it('tile 200 b hit', function(done) {
        source.getTile(1, 0, 0, tile(tiles.b, true, done));
    });
    it('tile 40x miss', function(done) {
        source.getTile(4, 0, 0, error('Tile does not exist', false, done));
    });
    it('tile 40x hit', function(done) {
        source.getTile(4, 0, 0, error('Tile does not exist', true, done));
    });
    it('tile 500 miss', function(done) {
        source.getTile(2, 0, 0, error('Unexpected error', false, done));
    });
    it('tile 500 miss', function(done) {
        source.getTile(2, 0, 0, error('Unexpected error', false, done));
    });
    it('grid 200 a miss', function(done) {
        source.getGrid(0, 0, 0, grid(grids.a, false, done));
    });
    it('grid 200 a hit', function(done) {
        source.getGrid(0, 0, 0, grid(grids.a, true, done));
    });
    it('grid 200 b miss', function(done) {
        source.getGrid(1, 0, 0, grid(grids.b, false, done));
    });
    it('grid 200 b hit', function(done) {
        source.getGrid(1, 0, 0, grid(grids.b, true, done));
    });
    it('grid 40x miss', function(done) {
        source.getGrid(4, 0, 0, error('Grid does not exist', false, done));
    });
    it('grid 40x hit', function(done) {
        source.getGrid(4, 0, 0, error('Grid does not exist', true, done));
    });
    it('long tile 200 a miss', function(done) {
        longsource.getTile(0, 0, 0, tile(tiles.a, false, done));
    });
    it('long tile 200 b miss', function(done) {
        longsource.getTile(1, 0, 0, tile(tiles.b, false, done));
    });
    it('long grid 200 a miss', function(done) {
        longsource.getGrid(0, 0, 0, grid(grids.a, false, done));
    });
    it('long grid 200 b miss', function(done) {
        longsource.getGrid(1, 0, 0, grid(grids.b, false, done));
    });
    it('stale tile 200 a miss', function(done) {
        stalesource.getTile(0, 0, 0, tile(tiles.a, false, done));
    });
    it('stale tile 200 b miss', function(done) {
        stalesource.getTile(1, 0, 0, tile(tiles.b, false, done));
    });
    it('stale grid 200 a miss', function(done) {
        stalesource.getGrid(0, 0, 0, grid(grids.a, false, done));
    });
    it('stale grid 200 b miss', function(done) {
        stalesource.getGrid(1, 0, 0, grid(grids.b, false, done));
    });
    it('dead tile 200 a miss', function(done) {
        deadsource.getTile(0, 0, 0, tile(tiles.a, false, done));
    });
    it('dead tile 200 b miss', function(done) {
        deadsource.getTile(1, 0, 0, tile(tiles.b, false, done));
    });
    it('dead grid 200 a miss', function(done) {
        deadsource.getGrid(0, 0, 0, grid(grids.a, false, done));
    });
    it('dead grid 200 b miss', function(done) {
        deadsource.getGrid(1, 0, 0, grid(grids.b, false, done));
    });
    describe('expires', function() {
        before(function(done) {
            setTimeout(done, 1000);
        });
        it('tile 200 a expires', function(done) {
            source.getTile(0, 0, 0, tile(tiles.a, false, done));
        });
        it('tile 200 b expires', function(done) {
            source.getTile(1, 0, 0, tile(tiles.b, false, done));
        });
        it('tile 40x expires', function(done) {
            source.getTile(4, 0, 0, error('Tile does not exist', false, done));
        });
        it('grid 200 a expires', function(done) {
            source.getGrid(0, 0, 0, grid(grids.a, false, done));
        });
        it('grid 200 b expires', function(done) {
            source.getGrid(1, 0, 0, grid(grids.b, false, done));
        });
        it('grid 40x expires', function(done) {
            source.getGrid(4, 0, 0, error('Grid does not exist', false, done));
        });
        it('long tile 200 a hit', function(done) {
            longsource.getTile(0, 0, 0, tile(tiles.a, true, done));
        });
        it('long tile 200 b hit', function(done) {
            longsource.getTile(1, 0, 0, tile(tiles.b, true, done));
        });
        it('long grid 200 a hit', function(done) {
            longsource.getGrid(0, 0, 0, grid(grids.a, true, done));
        });
        it('long grid 200 b hit', function(done) {
            longsource.getGrid(1, 0, 0, grid(grids.b, true, done));
        });
        it('dead tile 200 a miss', function(done) {
            deadsource.getTile(0, 0, 0, tile(tiles.a, false, done));
        });
        it('dead tile 200 b miss', function(done) {
            deadsource.getTile(1, 0, 0, tile(tiles.b, false, done));
        });
        it('dead grid 200 a miss', function(done) {
            deadsource.getGrid(0, 0, 0, grid(grids.a, false, done));
        });
        it('dead grid 200 b miss', function(done) {
            deadsource.getGrid(1, 0, 0, grid(grids.b, false, done));
        });
    });
    describe('refresh', function() {
        it('long tile 200 a hit', function(done) {
            longsource.getTile(0, 0, 0, function(err, data, headers) {
                var origExpires = headers['x-redis-expires'];
                setTimeout(function() {
                    longsource.getTile(0, 0, 0, function(err, data, headers) {
                        assert.equal(origExpires, headers['x-redis-expires']);
                        tile(tiles.a, true, done)(err, data, headers);
                    });
                }, 500);
            });
        });
        it('stale tile 200 a refresh hit', function(done) {
            stalesource.getTile(0, 0, 0, function(err, data, headers) {
                var origExpires = headers['x-redis-expires'];
                setTimeout(function() {
                    stalesource.getTile(0, 0, 0, function(err, data, headers) {
                        assert.notEqual(origExpires, headers['x-redis-expires']);
                        tile(tiles.a, true, done)(err, data, headers);
                    });
                }, 500);
            });
        });
    });

    it('stale tile 40x', function(done) {
        var gets = stalesource.stat.get;
        req1();
        function req1() {
            stalesource.getTile(4, 0, 0, function(err, data, headers) {
                assert.equal(err.toString(), 'Error: Tile does not exist');
                assert.equal(stalesource.stat.get, ++gets, 'cache miss, does i/o');
                req2();
            });
        }
        function req2() {
            stalesource.getTile(4, 0, 0, function(err, data, headers) {
                assert.equal(err.toString(), 'Error: Tile does not exist');
                assert.equal(stalesource.stat.get, gets, 'returns stale error object before doing i/o');
                req3();
            });
        }
        function req3() {
            stalesource.getTile(4, 0, 0, function(err, data, headers) {
                assert.equal(err.toString(), 'Error: Tile does not exist');
                assert.equal(stalesource.stat.get, gets, 'has not done i/o for previous stale get');
                req4();
            });
        }
        function req4() {
            stalesource.getTile(4, 0, 0, function(err, data, headers) {
                assert.equal(err.toString(), 'Error: Tile does not exist');
                assert.equal(stalesource.stat.get, gets, 'has not done i/o for previous stale get');
                done();
            });
        }
    });

    describe('high water mark', function() {
        var hwmHit;
        var highwater = function(err) {
            hwmHit = true;
            assert.equal(err.message, 'Redis command queue at high water mark');
        };
        var Source = Redsource({}, Testsource);
        var source;
        before(function(done) {
            Source.redis.client.command_queue_high_water = 0;
            Source.redis.client.on('error', highwater);
            done();
        });
        before(function(done) {
            new Source({ delay:50 }, function(err, redsource) {
                if (err) throw err;
                source = redsource;
                done();
            });
        });
        after(function(done) {
            Source.redis.client.command_queue_high_water = 1000;
            Source.redis.client.removeListener('error', highwater);
            done();
        });
        it('error on high water mark', function(done) {
            source.getTile(0, 0, 0, function(err, res) {
                assert.equal(hwmHit, true);
                done();
            });
        });
    });
});

describe('upstream expires', function() {
    var customExpires;
    var stats = {};
    var options = {};
    var getter = function(id, callback) {
        stats[id] = stats[id] || 0;
        stats[id]++;

        if (id === 'missing') {
            var err = new Error('Not found');
            err.statusCode = 404;
            return callback(err);
        }
        if (id === 'denied') {
            var err = new Error('Access denied');
            err.statusCode = 403;
            return callback(err);
        }
        if (id === 'fatal') {
            var err = new Error('Fatal');
            err.statusCode = 500;
            return callback(err);
        }
        if (id === 'nocode') {
            var err = new Error('Unexpected');
            return callback(err);
        }

        return callback(null, {id:id}, { Expires: customExpires });
    };
    var wrapped = Redsource.cachingGet('test', options, getter);
    before(function(done) {
        options.client.flushdb(done);
        customExpires = (new Date(+new Date() + 1000)).toUTCString();
    });
    it('getter 200 miss', function(done) {
        wrapped('asdf', function(err, data, headers) {
            assert.ifError(err);
            assert.deepEqual(data, {id:'asdf'}, 'returns data');
            assert.deepEqual(headers.expires, customExpires, 'passes customExpires through');
            assert.deepEqual(headers['x-redis-expires'], customExpires, 'sets x-redis-expires based on customExpires');
            assert.deepEqual(headers['x-redis'], undefined, 'cache miss');
            assert.equal(stats.asdf, 1, 'asdf IO x1');
            done();
        });
    });
    it('getter 200 hit', function(done) {
        wrapped('asdf', function(err, data, headers) {
            assert.ifError(err);
            assert.deepEqual(data, {id:'asdf'}, 'returns data');
            assert.deepEqual(headers.expires, customExpires, 'passes customExpires through');
            assert.deepEqual(headers['x-redis-expires'], customExpires, 'sets x-redis-expires based on customExpires');
            assert.deepEqual(headers['x-redis'], 'hit');
            assert.deepEqual(headers['x-redis-json'], true);
            assert.equal(stats.asdf, 1, 'asdf IO x1');
            done();
        });
    });
    it('getter 200 miss', function(done) {
        this.timeout(4000);
        setTimeout(function() {
            wrapped('asdf', function(err, data, headers) {
                assert.ifError(err);
                assert.deepEqual(data, {id:'asdf'}, 'returns data');
                assert.deepEqual(headers.expires, customExpires, 'passes customExpires through');
                assert.deepEqual(headers['x-redis-expires'], customExpires, 'sets x-redis-expires based on customExpires');
                assert.deepEqual(headers['x-redis'], undefined, 'cache miss');
                assert.equal(stats.asdf, 2, 'asdf IO x2');
                done();
            });
        }, 3000);
    });
});

describe('cachingGet', function() {
    var stats = {};
    var options = {};
    var getter = function(id, callback) {
        stats[id] = stats[id] || 0;
        stats[id]++;

        if (id === 'denied') {
            var err = new Error('Access denied');
            err.statusCode = 403;
            return callback(err);
        }
        if (id === 'missing') {
            var err = new Error('Not found');
            err.statusCode = 404;
            return callback(err);
        }
        if (id === 'fatal') {
            var err = new Error('Fatal');
            err.statusCode = 500;
            return callback(err);
        }
        if (id === 'nocode') {
            var err = new Error('Unexpected');
            return callback(err);
        }

        return callback(null, {id:id});
    };
    var wrapped = Redsource.cachingGet('test', options, getter);
    before(function(done) {
        options.client.flushdb(done);
    });
    it('getter 200 miss', function(done) {
        wrapped('asdf', function(err, data, headers) {
            assert.ifError(err);
            assert.deepEqual(data, {id:'asdf'}, 'returns data');
            assert.deepEqual(Object.keys(headers), ['x-redis-expires', 'x-redis-json'], 'sets x-redis-expires header');
            assert.equal(stats.asdf, 1, 'asdf IO x1');
            done();
        });
    });
    it('getter 200 hit', function(done) {
        wrapped('asdf', function(err, data, headers) {
            assert.ifError(err);
            assert.deepEqual(data, {id:'asdf'}, 'returns data');
            assert.deepEqual(Object.keys(headers), ['x-redis-expires', 'x-redis-json', 'x-redis'], 'sets x-redis-expires header');
            assert.deepEqual(headers['x-redis'], 'hit');
            assert.deepEqual(headers['x-redis-json'], true);
            assert.equal(stats.asdf, 1, 'asdf IO x1');
            done();
        });
    });
    it('getter 403 miss', function(done) {
        wrapped('denied', function(err, data, headers) {
            assert.equal(err.toString(), 'Error: Access denied', 'access denied err');
            assert.equal(err.statusCode, 403, 'err statusCode 403');
            assert.deepEqual(Object.keys(headers), ['x-redis-expires', 'x-redis-err'], 'sets x-redis-expires header');
            assert.equal(stats.denied, 1, 'missing IO x1');
            done();
        });
    });
    it('getter 403 hit', function(done) {
        wrapped('denied', function(err, data, headers) {
            assert.equal(err.toString(), 'Error', 'access denied err');
            assert.equal(err.statusCode, 403, 'err statusCode 403');
            assert.equal(stats.denied, 1, 'missing IO x1');
            done();
        });
    });
    it('getter 404 miss', function(done) {
        wrapped('missing', function(err, data, headers) {
            assert.equal(err.toString(), 'Error: Not found', 'not found err');
            assert.equal(err.statusCode, 404, 'err statusCode 404');
            assert.deepEqual(Object.keys(headers), ['x-redis-expires', 'x-redis-err'], 'sets x-redis-expires header');
            assert.equal(stats.missing, 1, 'missing IO x1');
            done();
        });
    });
    it('getter 404 hit', function(done) {
        wrapped('missing', function(err, data, headers) {
            assert.equal(err.toString(), 'Error', 'not found err');
            assert.equal(err.statusCode, 404, 'err statusCode 404');
            assert.equal(stats.missing, 1, 'missing IO x1');
            done();
        });
    });
    it('getter 500 miss', function(done) {
        wrapped('fatal', function(err, data, headers) {
            assert.equal(err.toString(), 'Error: Fatal', 'fatal err');
            assert.equal(err.statusCode, 500, 'err statusCode 500');
            assert.ok(!headers, 'no headers');
            assert.equal(stats.fatal, 1, 'fatal IO x1');
            done();
        });
    });
    it('getter 500 miss', function(done) {
        wrapped('fatal', function(err, data, headers) {
            assert.equal(err.toString(), 'Error: Fatal', 'fatal err');
            assert.equal(err.statusCode, 500, 'err statusCode 500');
            assert.ok(!headers, 'no headers');
            assert.equal(stats.fatal, 2, 'fatal IO x1');
            done();
        });
    });
    it('getter nocode', function(done) {
        wrapped('nocode', function(err, data, headers) {
            assert.equal(err.toString(), 'Error: Unexpected', 'unexpected err');
            assert.equal(err.statusCode, undefined, 'no err statusCode');
            assert.ok(!headers, 'no headers');
            assert.equal(stats.nocode, 1, 'nocode IO x1');
            done();
        });
    });
    it('getter nocode', function(done) {
        wrapped('nocode', function(err, data, headers) {
            assert.equal(err.toString(), 'Error: Unexpected', 'unexpected err');
            assert.equal(err.statusCode, undefined, 'no err statusCode');
            assert.ok(!headers, 'no headers');
            assert.equal(stats.nocode, 2, 'nocode IO x1');
            done();
        });
    });
});

describe('cachingGet with key patterns', function() {
    var stats = {};
    var options = {
        ttl: {
            streets: 1,
            satellite: 1
        },
        stale: {
            streets: 1,
            satellite: 3
        }
    };
    var getter = function(id, callback) {
        stats[id] = stats[id] || 0;
        stats[id]++;
        return callback(null, {id:id});
    };
    var wrapped = Redsource.cachingGet('test', options, getter);
    before(function(done) {
        options.client.flushdb(done);
    });
    it('streets miss', function(done) {
        wrapped('streets', function(err, data, headers) {
            assert.ifError(err);
            assert.deepEqual(data, {id:'streets'}, 'returns data');
            assert.deepEqual(Object.keys(headers), ['x-redis-expires', 'x-redis-json'], 'sets x-redis-expires header');
            assert.equal(stats.streets, 1, 'streets IO x1');
            done();
        });
    });
    it('streets hit (hit: 1s)', function(done) {
        this.timeout(2000);
        setTimeout(function() {
            wrapped('streets', function(err, data, headers) {
                assert.ifError(err);
                assert.deepEqual(data, {id:'streets'}, 'returns data');
                assert.deepEqual(Object.keys(headers), ['x-redis-expires', 'x-redis-json', 'x-redis'], 'sets x-redis-expires header');
                assert.deepEqual(headers['x-redis'], 'hit');
                assert.deepEqual(headers['x-redis-json'], true);
                assert.equal(stats.streets, 1, 'streets IO x1');
                done();
            });
        }, 1100);
    });
    it('streets miss (stale: 2s)', function(done) {
        this.timeout(3000);
        setTimeout(function() {
            wrapped('streets', function(err, data, headers) {
                assert.ifError(err);
                assert.deepEqual(data, {id:'streets'}, 'returns data');
                assert.deepEqual(Object.keys(headers), ['x-redis-expires', 'x-redis-json'], 'sets x-redis-expires header');
                assert.equal(stats.streets, 3, 'streets IO x3');
                done();
            });
        }, 2100);
    });
    it('satellite miss', function(done) {
        wrapped('satellite', function(err, data, headers) {
            assert.ifError(err);
            assert.deepEqual(data, {id:'satellite'}, 'returns data');
            assert.deepEqual(Object.keys(headers), ['x-redis-expires', 'x-redis-json'], 'sets x-redis-expires header');
            assert.equal(stats.satellite, 1, 'satellite IO x1');
            done();
        });
    });
    it('satellite hit (hit: 3s)', function(done) {
        this.timeout(4000);
        setTimeout(function() {
            wrapped('satellite', function(err, data, headers) {
                assert.ifError(err);
                assert.deepEqual(data, {id:'satellite'}, 'returns data');
                assert.deepEqual(Object.keys(headers), ['x-redis-expires', 'x-redis-json', 'x-redis'], 'sets x-redis-expires header');
                assert.deepEqual(headers['x-redis'], 'hit');
                assert.deepEqual(headers['x-redis-json'], true);
                assert.equal(stats.satellite, 1, 'satellite IO x1');
                done();
            });
        }, 3000);
    });
    it('satellite miss (stale: 5s)', function(done) {
        this.timeout(6000);
        setTimeout(function() {
            wrapped('satellite', function(err, data, headers) {
                assert.ifError(err);
                assert.deepEqual(data, {id:'satellite'}, 'returns data');
                assert.deepEqual(Object.keys(headers), ['x-redis-expires', 'x-redis-json'], 'sets x-redis-expires header');
                assert.equal(stats.satellite, 3, 'satellite IO x3');
                done();
            });
        }, 5000);
    });
});

describe('cachingGet timeout', function() {
    var stats = {};
    var getter = function(id, callback) {
        stats[id] = stats[id] || 0;
        stats[id]++;
        return callback(null, {id:id}, { expires: (new Date(Date.now() + (100 * 1000))).toUTCString() });
    };

    // Test that get commands timeout
    it('client.get timeout', function(done) {
        var options = {
            client: {
                command_queue: [],
                options: {},
                emit: function(event, err) {
                    assert.equal(event, 'error');
                    assert.equal(err.toString(), 'TimeoutError: timeout of 50ms exceeded for callback redisGet');
                },
                get: function(id, callback) {
                    setTimeout(function() { callback(); }, 1000);
                }
            }
        };
        var wrapped = Redsource.cachingGet('test', options, getter);
        wrapped('asdf', function(err, data, headers) {
            assert.ifError(err);
            assert.deepEqual(data, {id:'asdf'}, 'returns data');
            assert.deepEqual(Object.keys(headers), [ 'expires' ], 'sets headers');
            assert.equal(stats.asdf, 1, 'asdf IO x1');
            done();
        });
    });

    // Test that setex commands timeout
    it('client.setex timeout', function(done) {
        var options = {
            client: {
                command_queue: [],
                options: {},
                emit: function(event, err) {
                    assert.equal(event, 'error');
                    assert.equal(err.toString(), 'TimeoutError: timeout of 50ms exceeded for callback redisSetEx');
                },
                get: function(id, callback) {
                    callback();
                },
                setex: function(id, expires, data, callback) {
                    setTimeout(function() { callback(); }, 1000);
                }
            }
        };
        var wrapped = Redsource.cachingGet('test', options, getter);
        wrapped('asdf', function(err, data, headers) {
            assert.ifError(err);
            assert.deepEqual(data, {id:'asdf'}, 'returns data');
            assert.deepEqual(Object.keys(headers), ['expires', 'x-redis-expires', 'x-redis-json'], 'sets x-redis headers');
            setTimeout(function() {
                done();
            }, 100);
        });
    });
});

describe('unit', function() {
    it('encode', function(done) {
        var errstatCode404 = new Error(); errstatCode404.statusCode = 404;
        var errstatCode403 = new Error(); errstatCode403.statusCode = 403;
        var errstatCode500 = new Error(); errstatCode500.statusCode = 500;
        assert.ok(bufferEqual(Redsource.encode(errstatCode404), Buffer.from(
            '{"x-redis-err":"404"}'
        )));
        assert.ok(bufferEqual(Redsource.encode(errstatCode403), Buffer.from(
            '{"x-redis-err":"403"}'
        )));
        assert.equal(Redsource.encode(errstatCode500), null);

        assert.ok(bufferEqual(Redsource.encode(null, {id:'foo'}), Buffer.from(
            '{"x-redis-json":true}' +
            new Array(1025 - '{"x-redis-json":true}'.length).join(' ') +
            '{"id":"foo"}'
        )), 'encodes object');

        assert.ok(bufferEqual(Redsource.encode(null, 'hello world'), Buffer.from(
            '{}' +
            new Array(1025 - '{}'.length).join(' ') +
            'hello world'
        ), 'encodes string'));

        assert.ok(bufferEqual(Redsource.encode(null, Buffer.alloc(0)), Buffer.from(
            '{}' +
            new Array(1025 - '{}'.length).join(' ') +
            ''
        ), 'encodes empty buffer'));

        assert.ok(bufferEqual(Redsource.encode(null, Buffer.alloc(0), { 'content-type': 'image/png' }), Buffer.from(
            '{"content-type":"image/png"}' +
            new Array(1025 - '{"content-type":"image/png"}'.length).join(' ') +
            ''
        ), 'encodes headers'));

        assert.throws(function() {
            Redsource.encode(null, Buffer.alloc(0), { data: new Array(1024).join(' ') });
        }, Error, 'throws when headers exceed 1024 bytes');

        done();
    });
    it('decode', function(done) {
        assert.deepEqual(Redsource.decode('404'), {err:{statusCode:404,redis:true}});
        assert.deepEqual(Redsource.decode('403'), {err:{statusCode:403,redis:true}});

        assert.deepEqual(
            Redsource.decode(Buffer.from('{"x-redis-err":"404","x-redis":"hit"}')),
            {err:{statusCode:404,redis:true}, headers:{'x-redis-err': '404','x-redis': 'hit'}}
        );

        assert.deepEqual(
            Redsource.decode(Buffer.from('{"x-redis-err":"403","x-redis":"hit"}')),
            {err:{statusCode:403,redis:true}, headers:{'x-redis-err': '403','x-redis': 'hit'}}
        );

        var headers = JSON.stringify({'x-redis-json':true,'x-redis':'hit'});
        var encoded = Buffer.from(
            headers +
            new Array(1025 - headers.length).join(' ') +
            JSON.stringify({'id':'foo'})
        );
        assert.deepEqual(Redsource.decode(encoded), {
            headers:{'x-redis-json':true,'x-redis':'hit'},
            buffer:{'id':'foo'}
        }, 'decodes object');

        var headers = JSON.stringify({'x-redis':'hit'});
        var encoded = Buffer.from(
            headers +
            new Array(1025 - headers.length).join(' ') +
            'hello world'
        );
        assert.deepEqual(Redsource.decode(encoded), {
            headers:{'x-redis':'hit'},
            buffer: Buffer.from('hello world'),
        }, 'decodes string (as buffer)');

        var headers = JSON.stringify({'x-redis':'hit'});
        var encoded = Buffer.from(
            headers +
            new Array(1025 - headers.length).join(' ') +
            ''
        );
        assert.deepEqual(Redsource.decode(encoded), {
            headers:{'x-redis':'hit'},
            buffer: Buffer.alloc(0),
        }, 'decodes empty buffer');

        var encoded = Buffer.from('bogus');
        assert.throws(function() {
            Redsource.decode(encoded);
        }, Error, 'throws when encoded buffer does not include headers');

        done();
    });
});

describe('perf', function() {
    var buffer = require('fs').readFileSync(__dirname + '/encode-buster.pbf');
    it('encodes buster PBF in < 10ms', function(done) {
        var time = + new Date();
        for (var i = 0; i < 10; i++) Redsource.encode(null, buffer);
        time = + new Date() - time;
        assert.equal(time < 10, true, 'encodes buster PBF 10x in ' + time + 'ms');
        done();
    });
});


describe('perf-source', function() {
    var source;
    var Source = Redsource({}, Testsource);
    before(function(done) {
        Source.redis.client.flushdb(done);
    });
    before(function(done) {
        new Source({hostname:'perf'}, function(err, redsource) {
            if (err) throw err;
            source = redsource;
            done();
        });
    });
    it('gets buster tile 10x in < 20ms', function(done) {
        var remaining = 10;
        var time = + new Date();
        for (var i = 0; i < 10; i++) source.getTile(0,0,0, function(err, data, headers) {
            assert.ifError(err);
            assert.equal(data.length, 783167);
            if (!--remaining) {
                time = + new Date() - time;
                assert.equal(time < 40, true, 'getTile buster PBF 10x in ' + time + 'ms');
                done();
            }
        });
    });
});
