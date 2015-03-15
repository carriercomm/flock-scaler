var Marathon = require('marathon.node'),
    request = require('request'),
    csv = require('csv-stream'),
    url = require('url'),
    _ = require('underscore');

// TODO read from consul
var MARATHON_API = process.env.MARATHON_API || 'http://10.141.141.10:8080';

// XXX
// TODO use host when DNS is available
var HAPROXY_HOST = url.parse(MARATHON_API).hostname;
var HAPROXY_STATS_CSV = 'http://' + HAPROXY_HOST + ':9090/haproxy?stats;csv';
var HAPROXY_USER = 'admin';
var HAPROXY_PASS = 'admin';

var client = new Marathon({base_url: MARATHON_API});

var scaleDown = function scaleDown(appName, callback) {
    client.app(appName).get().then(function onapp(res) {
        var currentInstances = res.app.instances;
        var instances = currentInstances > 1 ? currentInstances - 1 : 0;
        client.app(appName).update({instances: instances}).then(function onscale(res) {
            console.log('scale down response', res);
            callback();
        });
    });
};

var scaleUp = function scaleUp(appName, callback) {
    client.app(appName).get().then(function onapp(res) {
        var currentInstances = res.app.instances;
        var instances = currentInstancs + 1;
        client.app(appName).update({instances: instances}).then(function onscale(res) {
            console.log('scale up response', res);
            callback();
        });
    });
};

var getHaproxyStats = function getHaproxyStats(callback) {
    var stats = [];

    var ondata = function ondata(data) {
        stats.push(data);
    };

    var onerror = function onerror(err) {
        callback(err);
    };

    var onend = function onend() {
        var err;
        callback(undefined, stats);
    };

    request.get({url:HAPROXY_STATS_CSV})
        .auth(HAPROXY_USER, HAPROXY_PASS, false)
        .pipe(csv.createStream())
        .on('error', onerror)
        .on('data', ondata)
        .on('end', onend)
};

getHaproxyStats(function onstats(err, stats) {
    var filterFlockInstances = function isFlockServer(server) {
        // TODO improve algorithm
        return server['# pxname'].indexOf('flock-backup') === -1 &&
               server['# pxname'].indexOf('flock') === 0 &&
               server['svname'].indexOf('server_backup') === -1 &&
               server['svname'].indexOf('BACKEND') === 0;
               // server['svname'].indexOf('server_') === 0;
    };

    var FIELDS = ['# pxname', 'lastsess', 'qcur', 'qmax', 'scur', 'smax', 'status', 'svname', 'qtime', 'ctime', 'rtime', 'lastchg'];
    var servers = _.chain(stats)
        .filter(filterFlockInstances)
        .map(function (row) { return _.pick(row, FIELDS) })
        .value();

    console.log('current servers', servers);

    var filterIdleServers = function isIdle(server) {
        return server.lastsess > 50 || (server.lastsess == -1 && server.lastchg > 50);
    };

    var idleServers = _.filter(servers, filterIdleServers);
    idleServers.forEach(function (server) {
        var appId = '/' + server['# pxname'];
        scaleDown(appId, function () {
            // fire and forget
            console.log('try to scale down app %s', appId);
        });
    });

    // TODO use 'xxxxxxxx' to decide when to scale up
});
