#!/usr/bin/env node

var util = require('util');
var fs = require('fs');
var url = require('url');
var qs = require('querystring').stringify;
var knox = require('knox');
var Henry = require('henry');
var Step = require('step');
var request = require('request');
var _ = require('underscore');
var argv = require('optimist')
    .config(['config', 'jobflows'])
    .usage('Import CouchDB Database from s3\n' +
           'Usage: $0 [required options] [--remoteName]'
    )
    .demand(['inputBucket', 'database'])
    .argv;
var s3Client;

process.title = 's32couchdb';

var henry = new Henry({
    api: argv.awsMetadataEndpoint
}).on('refresh', function(credentials) {
    util.log('Henry refresh: ' + credentials.key);
});

var dbUrl = url.parse(argv.database);
var dbName = argv.remoteName || dbUrl.pathname.split('/')[1];

var importData = function(docs, callback) {
    docs = _(docs).map(function(v) {
        v = JSON.parse(v);
        // support wrapped docs.
        if (v.doc && v.doc._id) return v.doc;
        return v;
    });

    var uri = url.format({
        protocol: dbUrl.protocol,
        host: dbUrl.host,
        pathname: dbUrl.pathname + "/_bulk_docs"
    });
    request({
        method: 'POST',
        headers: {'Content-Type': 'application/json' },
        body: JSON.stringify({"new_edits": false, "docs": docs}),
        uri: uri,
        auth: dbUrl.auth
    }, function(err, res){
        if (res.statusCode != 201)
            return callback(new Error('Got '+ res.statusCode +' from CouchDB'));
        return callback(err);
    });
};

var pad = function(n) {
    var prefix = function(v, l) {
        while (v.length < l) { v = '0' + v; }
        return v;
    };
    var len = 2;
    var s = n.toString();
    return s.length == len ? s : prefix(s, len);
};

Step(function() {
    s3Client = knox.createClient({
        // Cannot pass null or empty key/secret to knox constructor
        // 'x' is just filler and Henry will update it.
        key: argv.awsKey || 'x',
        secret: argv.awsSecret || 'x',
        bucket: argv.inputBucket
    });
    henry.add(s3Client, function(err) {
        if (err && ['ETIMEDOUT', 'EHOSTUNREACH', 'ECONNREFUSED']
            .indexOf(err.code) === -1) return this(err);
        this(null);
    }.bind(this));
}, function(err) {
    if (err) throw err;
    var callback = this;
    var d = new Date(Date.now() - 864e5); // Look 1 day back.
    var options = {
        prefix: util.format('db/%s-', dbName),
        marker: util.format('db/%s-%s-%s-%s', dbName, d.getUTCFullYear(),
            pad(d.getUTCMonth() + 1), pad(d.getUTCDate()))
    };

    // Do a listing... use most recent file, log which it was.
    s3Client.get('?' + qs(options)).on('response', function(res) {
        var xml = '';
        res.on('error', callback);
        res.on('close', callback);
        res.on('data', function(chunk) { xml += chunk; });
        res.on('end', function(){
            var parsed = _({
                error:      new RegExp('[^>]+(?=<\\/Error>)', 'g'),
                prefixes:   new RegExp('[^>]+' + options.delimiter + '(?=<\\/Prefix>)', 'g'),
                truncated:  new RegExp('[^>]+(?=<\\/IsTruncated>)', 'g'),
                keys:       new RegExp('[^>]+(?=<\\/Key>)', 'g'),
                nextmarker: new RegExp('[^>]+(?=<\\/NextMarker>)', 'g')
            }).reduce(function(memo, pattern, key) {
                memo[key] = xml.match(pattern) || [];
                return memo;
            }, {});

            if (parsed.keys.length === 0)
                return callback(new Error('Unable to locate db on s3'));

            callback(null, parsed.keys.pop());
        });

    }).end();
}, function(err, key) {
    if (err) throw err;
    console.log('Using database %s', key);
    var importSize = 10000;
    var callback = this;
    s3Client.get(key).on('response', function(res){
        if (res.statusCode != 200) throw new Error('Got '+ res.statusCode + 'from s3');

        var data = [];
        var prev = '';
        res.setEncoding('utf8');
        res.on('data', function(buf) {
            if (prev) {
                buf = prev + buf;
                prev = '';
            }

            data = data.concat(_(buf.split('\n')).compact());
            if (buf[buf.length - 1] !== '\n')
                prev = data.pop();
            
            if (data.length > importSize)
                importData(data.splice(0, importSize), function(err) {
                    if (err) {
                        console.error(err);
                        process.exit(1);
                    }
                });
        });
        var ended = false;
        res.on('end', function() {
            ended = true;
            importData(data, callback);
        });
        res.on('close', function() {
            if (ended) return;
            callback(new Error('Connection to s3 terminate before end'));
        });
        res.on('error', callback);
    }).end();

}, function(err) {
    if (err) {
        console.error(err);
        process.exit(1);
    }
    console.log('Import of %s database into %s completed', dbName , argv.database);
});
