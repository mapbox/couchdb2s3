#!/usr/bin/env node

var util = require('util');
var fs = require('fs');
var url = require('url');
var crypto = require('crypto');
var knox = require('knox');
var Step = require('step');
var request = require('request');
var argv = require('optimist')
    .config(['config', 'jobflows'])
    .usage('Export CouchDB Database to s3\n' +
           'Usage: $0 [options]'
    )
    .demand(['awsKey', 'awsSecret', 'outputBucket', 'database'])
    .argv;

process.title = 'couchdb2s3';

var s3Client = knox.createClient({
    key: argv.awsKey,
    secret: argv.awsSecret,
    bucket: argv.outputBucket
});

var dbUrl = url.parse(argv.database);
var dbName = dbUrl.pathname.split('/')[1];
var dbClient;

var tempFilepath = '/tmp/couchdb2s3-'+ dbName +'-'+ (new Date()).getTime();

var rand = crypto.createHash('md5')
    .update(crypto.randomBytes(256))
    .digest('base64')
    .replace(/[^a-zA-Z0-9]/g,'');

var pad = function(n) {
    var prefix = function(v, l) {
        while (v.length < l) { v = '0' + v; }
        return v;
    };
    var len = 2;
    var s = n.toString();
    return s.length == len ? s : prefix(s, len);
};

var d = new Date();
var s3Key = util.format('/db/%s-%s-%s-%s-%s-%s', dbName, d.getUTCFullYear(),
    pad(d.getUTCMonth() + 1), pad(d.getUTCDate()), pad(d.getUTCHours()), rand);

Step(function() {
    var uri = url.format({
        protocol: dbUrl.protocol,
        host: dbUrl.host,
        pathname: dbUrl.pathname + "/_all_docs?include_docs=true"
    });
    request({ uri: uri, auth: dbUrl.auth }, this);
}, function(err, res, data) {
    if (err) throw err;
    if (res.statusCode != 200) throw new Error("Could not connect to CouchDB");
    data = JSON.parse(data).rows.map(function(v) { return JSON.stringify(v.doc); });
    fs.writeFile(tempFilepath, data.join('\n'), 'utf8', this);
}, function(err) {
    if (err) throw err;
    var next = this;
    var tries = 3;
    var put = function() {
        s3Client.putFile(tempFilepath, s3Key, {
            'x-amz-server-side-encryption':'AES256'
        }, function(err, res) {
            if (!err) return next(null, res);
            else if (tries > 0) {
                console.log('Upload to s3 failed, trying again.');
                tries--;
                return put();
            }
            return next(err);
        });
    };
    put();

}, function(err, res) {
    if (err) throw err;
    if (!res) throw new Error('upload failed');
    if (res.statusCode !== 200) throw new Error('s3 returned ' + res.statusCode);

    fs.unlink(tempFilepath, this);
}, function(err) {
    if (err) {
        console.error(err);
        process.exit(1);
    }
    console.log('%s : Uploaded %s database to %s', (new Date), argv.database, s3Key);
});
