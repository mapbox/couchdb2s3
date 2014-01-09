#!/usr/bin/env node

var util = require('util');
var url = require('url');
var qs = require('querystring').stringify;
var Writable = require('stream').Writable;
var AWS = require('aws-sdk');
var nano = require('nano');
var argv = require('optimist')
    .config(['config', 'jobflows'])
    .usage('Import CouchDB Database from S3\n' +
           'Usage: $0 [required options] [--remoteName]'
    )
    .demand(['inputBucket', 'database'])
    .argv;

process.title = 's32couchdb';

var dbUrl = url.parse(argv.database);
var dbName = dbUrl.pathname.split('/')[1];
var db = nano(url.format({
    protocol: dbUrl.protocol,
    host: dbUrl.host,
    auth: dbUrl.auth
})).use(dbName);

var remoteName = argv.remoteName || dbName;

AWS.config.update({
    accessKeyId: argv.awsKey,
    secretAccessKey: argv.awsSecret,
    region: 'us-east-1'
});
var s3 = new AWS.S3;

// ImportStream class writes to CouchDB
//
util.inherits(ImportStream, Writable);
function ImportStream(opts) {
    opts = opts || {};
    opts.decodeStrings = true;
    Writable.call(this, opts);
    this.buffer = '';
    this.bufferLim = Math.pow(2, 18)
}
ImportStream.prototype._write = function(chunk, encoding, done) {
    this.buffer += chunk.toString('utf8');
    if (this.buffer.length < this.bufferLim) return done();

    this.flush(done);
};
ImportStream.prototype.flush = function(done) {
    var docs = this.buffer.split('\n');
    this.buffer = docs.pop();

    docs = docs.filter(function(v) {
        return v.length > 0;
    }).map(function(v) {
        try {
            v = JSON.parse(v);
        } catch(e) {
            console.log(v);
            console.log('---');
            console.log(this.buffer);
            throw e;
        }
        // support wrapped docs.
        if (v.doc && v.doc._id) return v.doc;
        return v;
    }.bind(this));

    db.bulk({ new_edits: false, docs: docs }, {}, done);
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

var d = new Date(Date.now() - 864e5); // Look 1 day back.

s3.listObjects({
    Bucket: argv.inputBucket,
    Prefix: util.format('db/%s-', remoteName),
    Marker: util.format('db/%s-%s-%s-%s', remoteName, d.getUTCFullYear(),
        pad(d.getUTCMonth() + 1), pad(d.getUTCDate()))
}, function(err, data) {
    if (err) throw err;
    if (data.Contents.length === 0)
        throw new Error('Unable to locate db on s3');

    var key = data.Contents.pop().Key

    var importer = new ImportStream();
    var reader = s3.getObject({
        Bucket: argv.inputBucket,
        Key: key
    }).createReadStream();
    reader.pipe(importer, { end: false });
    reader.on('end', function() {
        importer.flush(function(err) {
            if (err) {
                console.error(err);
                process.exit(1);
            }
            console.log('%s : Imported %s into %s/%s', (new Date()), key, dbUrl.host, dbName);
        });
    });
});
