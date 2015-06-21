var assert = require('assert');
var bufferEqual = require('buffer-equal');
var Redsource = require('../index');
var redis = Redsource.redis;
var deadclient = redis.createClient(6380, '127.0.0.1', {});

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
        assert.ok(Source.redis.expires, 300);
        new Source('fakeuri', function(err, source) {
            assert.ifError(err);
            assert.ok(source instanceof Testsource);
            assert.equal(source._uri, 'fakeuri');
            done();
        });
    });
    it('sets expires from opts', function(done) {
        var Source = Redsource({ expires:5 }, Testsource);
        assert.ok(Source.redis);
        assert.ok(Source.redis.expires, 5);
        done();
    });
    it('sets mode from opts', function(done) {
        assert.throws(function() {
            var Source = Redsource({ mode:'awesome' }, Testsource);
        }, /Invalid value for options\.mode/);
        var Source = Redsource({ mode:'race' }, Testsource);
        assert.ok(Source.redis.mode, 'readthrough');
        done();
    });
    it('sets client from opts', function(done) {
        var client = redis.createClient({return_buffers: true});
        var Source = Redsource({ client: client, expires:5 }, Testsource);
        assert.ok(Source.redis);
        assert.strictEqual(Source.redis.client, client);
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

describe('readthrough', function() {
    var source;
    var longsource;
    var deadsource;
    var Source = Redsource({ expires: {
        long: 60000,
        test: 1
    } }, Testsource);
    before(function(done) {
        Source.redis.client.flushdb(done);
    });
    before(function(done) {
        new Source('', function(err, redsource) {
            if (err) throw err;
            source = redsource;
            done();
        });
    });
    before(function(done) {
        new Source({hostname:'long'}, function(err, redsource) {
            if (err) throw err;
            longsource = redsource;
            done();
        });
    });
    before(function(done) {
        var Dead = Redsource({ expires: {
            long: 60000,
            test: 1
        }, mode:'race', client:deadclient }, Testsource);
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
    describe('high water mark', function() {
        var highwater = function(err) {
            assert.equal(err.message, 'Redis command queue at high water mark');
        };
        before(function(done) {
            Source.redis.client.command_queue_high_water = 0;
            Source.redis.client.on('error', highwater);
            done();
        });
        after(function(done) {
            Source.redis.client.command_queue_high_water = 1000;
            Source.redis.client.removeListener('error', highwater);
            done();
        });
        it('error on high water mark', function(done) {
            source.getTile(0, 0, 0, function(err, res) {
                done();
            });
        });
    });
});
describe('race', function() {
    var source;
    var longsource;
    var fastsource;
    var deadsource;
    var Source = Redsource({ expires: {
        long: 60000,
        test: 1
    }, mode:'race' }, Testsource);
    before(function(done) {
        Source.redis.client.flushdb(done);
    });
    before(function(done) {
        new Source({ delay:50 }, function(err, redsource) {
            if (err) throw err;
            source = redsource;
            done();
        });
    });
    before(function(done) {
        new Source({ hostname:'long', delay:50 }, function(err, redsource) {
            if (err) throw err;
            longsource = redsource;
            done();
        });
    });
    before(function(done) {
        new Source({ delay:0 }, function(err, redsource) {
            if (err) throw err;
            fastsource = redsource;
            done();
        });
    });
    before(function(done) {
        var Dead = Redsource({ expires: {
            long: 60000,
            test: 1
        }, mode:'race', client:deadclient }, Testsource);
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
    it('fast tile 200 a miss', function(done) {
        fastsource.getTile(0, 0, 0, tile(tiles.a, false, done));
    });
    it('fast tile 200 a miss', function(done) {
        fastsource.getTile(0, 0, 0, tile(tiles.a, false, done));
    });
    it('fast grid 200 a miss', function(done) {
        fastsource.getGrid(0, 0, 0, grid(grids.a, false, done));
    });
    it('fast grid 200 a miss', function(done) {
        fastsource.getGrid(0, 0, 0, grid(grids.a, false, done));
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
    describe('high water mark', function() {
        var highwater = function(err) {
            assert.equal(err.message, 'Redis command queue at high water mark');
        };
        before(function(done) {
            Source.redis.client.command_queue_high_water = 0;
            Source.redis.client.on('error', highwater);
            done();
        });
        after(function(done) {
            Source.redis.client.command_queue_high_water = 1000;
            Source.redis.client.removeListener('error', highwater);
            done();
        });
        it('error on high water mark', function(done) {
            source.getTile(0, 0, 0, function(err, res) {
                done();
            });
        });
    });
});


describe('cachingGet', function() {
    var stats = {};
    var options = { mode: 'readthrough' };
    var getter = function(id, callback) {
        stats[id] = stats[id] || 0;
        stats[id]++;

        if (id === 'missing') {
            var err = new Error('Not found');
            err.code = 404;
            return callback(err);
        }
        if (id === 'fatal') {
            var err = new Error('Fatal');
            err.code = 500;
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
            assert.ok(!headers, 'no headers');
            assert.equal(stats.asdf, 1, 'asdf IO x1');
            done();
        });
    });
    it('getter 200 hit', function(done) {
        wrapped('asdf', function(err, data, headers) {
            assert.ifError(err);
            assert.deepEqual(data, {id:'asdf'}, 'returns data');
            assert.deepEqual(headers, {'x-redis-json':true, 'x-redis':'hit'}, 'headers, hit');
            assert.equal(stats.asdf, 1, 'asdf IO x1');
            done();
        });
    });
    it('getter 404 miss', function(done) {
        wrapped('missing', function(err, data, headers) {
            assert.equal(err.toString(), 'Error: Not found', 'not found err');
            assert.equal(err.code, 404, 'err code 404');
            assert.ok(!headers, 'no headers');
            assert.equal(stats.missing, 1, 'missing IO x1');
            done();
        });
    });
    it('getter 404 hit', function(done) {
        wrapped('missing', function(err, data, headers) {
            assert.equal(err.toString(), 'Error', 'not found err');
            assert.equal(err.code, 404, 'err code 404');
            assert.ok(!headers, 'no headers');
            assert.equal(stats.missing, 1, 'missing IO x1');
            done();
        });
    });
    it('getter 500 miss', function(done) {
        wrapped('fatal', function(err, data, headers) {
            assert.equal(err.toString(), 'Error: Fatal', 'fatal err');
            assert.equal(err.code, 500, 'err code 500');
            assert.ok(!headers, 'no headers');
            assert.equal(stats.fatal, 1, 'fatal IO x1');
            done();
        });
    });
    it('getter 500 miss', function(done) {
        wrapped('fatal', function(err, data, headers) {
            assert.equal(err.toString(), 'Error: Fatal', 'fatal err');
            assert.equal(err.code, 500, 'err code 500');
            assert.ok(!headers, 'no headers');
            assert.equal(stats.fatal, 2, 'fatal IO x1');
            done();
        });
    });
    it('getter nocode', function(done) {
        wrapped('nocode', function(err, data, headers) {
            assert.equal(err.toString(), 'Error: Unexpected', 'unexpected err');
            assert.equal(err.code, undefined, 'no err code');
            assert.ok(!headers, 'no headers');
            assert.equal(stats.nocode, 1, 'nocode IO x1');
            done();
        });
    });
    it('getter nocode', function(done) {
        wrapped('nocode', function(err, data, headers) {
            assert.equal(err.toString(), 'Error: Unexpected', 'unexpected err');
            assert.equal(err.code, undefined, 'no err code');
            assert.ok(!headers, 'no headers');
            assert.equal(stats.nocode, 2, 'nocode IO x1');
            done();
        });
    });
});

describe('unit', function() {
    it('encode', function(done) {
        var errcode404 = new Error(); errcode404.code = 404;
        var errcode403 = new Error(); errcode403.code = 403;
        var errcode500 = new Error(); errcode500.code = 500;
        var errstat404 = new Error(); errstat404.status = 404;
        var errstat403 = new Error(); errstat403.status = 403;
        var errstat500 = new Error(); errstat500.status = 500;
        assert.equal(Redsource.encode(errcode404), '404');
        assert.equal(Redsource.encode(errcode403), '403');
        assert.equal(Redsource.encode(errcode500), null);
        assert.equal(Redsource.encode(errstat404), '404');
        assert.equal(Redsource.encode(errstat403), '403');
        assert.equal(Redsource.encode(errstat500), null);

        assert.ok(bufferEqual(Redsource.encode(null, {id:'foo'}), new Buffer(
            '{"x-redis-json":true}' +
            new Array(1025 - '{"x-redis-json":true}'.length).join(' ') +
            '{"id":"foo"}'
        )), 'encodes object');

        assert.ok(bufferEqual(Redsource.encode(null, 'hello world'), new Buffer(
            '{}' +
            new Array(1025 - '{}'.length).join(' ') +
            'hello world'
        ), 'encodes string'));

        assert.ok(bufferEqual(Redsource.encode(null, new Buffer(0)), new Buffer(
            '{}' +
            new Array(1025 - '{}'.length).join(' ') +
            ''
        ), 'encodes empty buffer'));

        assert.ok(bufferEqual(Redsource.encode(null, new Buffer(0), { 'content-type': 'image/png' }), new Buffer(
            '{"content-type":"image/png"}' +
            new Array(1025 - '{"content-type":"image/png"}'.length).join(' ') +
            ''
        ), 'encodes headers'));

        assert.throws(function() {
            Redsource.encode(null, new Buffer(0), { data: new Array(1024).join(' ') });
        }, Error, 'throws when headers exceed 1024 bytes');

        done();
    });
    it('decode', function(done) {
        assert.deepEqual(Redsource.decode('404'), {err:{code:404,status:404,redis:true}});
        assert.deepEqual(Redsource.decode('403'), {err:{code:403,status:403,redis:true}});

        var headers = JSON.stringify({'x-redis-json':true,'x-redis':'hit'});
        var encoded = new Buffer(
            headers +
            new Array(1025 - headers.length).join(' ') +
            JSON.stringify({'id':'foo'})
        );
        assert.deepEqual(Redsource.decode(encoded), {
            headers:{'x-redis-json':true,'x-redis':'hit'},
            buffer:{'id':'foo'}
        }, 'decodes object');

        var headers = JSON.stringify({'x-redis':'hit'});
        var encoded = new Buffer(
            headers +
            new Array(1025 - headers.length).join(' ') +
            'hello world'
        );
        assert.deepEqual(Redsource.decode(encoded), {
            headers:{'x-redis':'hit'},
            buffer: new Buffer('hello world'),
        }, 'decodes string (as buffer)');

        var headers = JSON.stringify({'x-redis':'hit'});
        var encoded = new Buffer(
            headers +
            new Array(1025 - headers.length).join(' ') +
            ''
        );
        assert.deepEqual(Redsource.decode(encoded), {
            headers:{'x-redis':'hit'},
            buffer: new Buffer(0),
        }, 'decodes empty buffer');

        var encoded = new Buffer('bogus');
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
            assert.equal(data.length, 818051);
            if (!--remaining) {
                time = + new Date() - time;
                assert.equal(time < 20, true, 'getTile buster PBF 10x in ' + time + 'ms');
                done();
            }
        });
    });
});

