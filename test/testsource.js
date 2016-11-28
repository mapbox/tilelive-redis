module.exports = Testsource;

var now = new Date;
var tiles = {
    a: require('fs').readFileSync(__dirname + '/a.png'),
    b: require('fs').readFileSync(__dirname + '/b.png'),
};

var grids = {
    a: { grid:'', keys: ['', '1' ], data:{'1': {'name':'foo'}} },
    b: { grid:'', keys: ['', '1' ], data:{'1': {'name':'bar'}} },
};

var perf = {
    a: require('fs').readFileSync(__dirname + '/encode-buster.pbf'),
};

Testsource.now = new Date;
Testsource.tiles = tiles;
Testsource.grids = grids;
Testsource.perf = perf;

// Define a mock test source.
function Testsource(uri, callback) {
    this._uri = uri;
    this.delay = uri.delay || 0;
    this.hostname = uri.hostname || 'test';
    this.stat = {
        'get': 0
    };
    callback(null, this);
};
Testsource.prototype.get = function(url, callback) {
    if (this._uri === undefined) throw new Error('Called without proper context');

    var stat = this.stat;

    if (this.delay) {
        stat.get++;
        setTimeout(function() { get(url, callback); }, this.delay);
    } else {
        stat.get++;
        get();
    }

    function get() {
        switch (url) {
        case 'http://test/0/0/0.png':
            return callback(null, tiles.a, {
                'content-type': 'image/png',
                'content-length': 11541,
                'last-modified': now.toUTCString()
            });
        case 'http://test/1/0/0.png':
            return callback(null, tiles.b, {
                'content-type': 'image/png',
                'content-length': 6199,
                'last-modified': now.toUTCString()
            });
        case 'http://test/2/0/0.png':
            return callback(new Error('Unexpected error'));
        case 'http://test/0/0/0.grid.json':
            return callback(null, JSON.stringify(grids.a), {
                'content-type': 'application/json',
                'last-modified': now.toUTCString()
            });
        case 'http://test/1/0/0.grid.json':
            return callback(null, JSON.stringify(grids.b), {
                'content-type': 'application/json',
                'last-modified': now.toUTCString()
            });
        case 'http://long/0/0/0.png':
            return callback(null, tiles.a, {
                'content-type': 'image/png',
                'content-length': 11541,
                'last-modified': now.toUTCString()
            });
        case 'http://long/1/0/0.png':
            return callback(null, tiles.b, {
                'content-type': 'image/png',
                'content-length': 6199,
                'last-modified': now.toUTCString()
            });
        case 'http://long/0/0/0.grid.json':
            return callback(null, JSON.stringify(grids.a), {
                'content-type': 'application/json',
                'last-modified': now.toUTCString()
            });
        case 'http://long/1/0/0.grid.json':
            return callback(null, JSON.stringify(grids.b), {
                'content-type': 'application/json',
                'last-modified': now.toUTCString()
            });
        case 'http://perf/0/0/0.png':
            return callback(null, perf.a, {
                'content-type': 'application/x-protobuf',
                'last-modified': now.toUTCString()
            });
        case 'http://stale/0/0/0.png':
            return callback(null, tiles.a, {
                'content-type': 'image/png',
                'content-length': 11541,
                'last-modified': now.toUTCString()
            });
        case 'http://stale/1/0/0.png':
            return callback(null, tiles.b, {
                'content-type': 'image/png',
                'content-length': 6199,
                'last-modified': now.toUTCString()
            });
        case 'http://stale/0/0/0.grid.json':
            return callback(null, JSON.stringify(grids.a), {
                'content-type': 'application/json',
                'last-modified': now.toUTCString()
            });
        case 'http://stale/1/0/0.grid.json':
            return callback(null, JSON.stringify(grids.b), {
                'content-type': 'application/json',
                'last-modified': now.toUTCString()
            });
        default:
            var err = new Error;
            err.statusCode = 404;
            return callback(err);
        }
    }
};
Testsource.prototype.getTile = function(z, x, y, callback) {
    this.get('http://' + this.hostname + '/' + [z,x,y].join('/') + '.png', function(err, buffer, headers) {
        if (err) {
            err.message = err.message || 'Tile does not exist';
            return callback(err);
        }
        return callback(null, buffer, headers);
    });
};
Testsource.prototype.getGrid = function(z, x, y, callback) {
    this.get('http://' + this.hostname + '/' + [z,x,y].join('/') + '.grid.json', function(err, buffer, headers) {
        if (err) {
            err.message = err.message || 'Grid does not exist';
            return callback(err);
        }
        return callback(null, JSON.parse(buffer), headers);
    });
};

