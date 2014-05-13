#!/usr/bin/env node

var util = require('util');
var url = require('url');
var zlib = require('zlib');
var qs = require('querystring').stringify;
var StringDecoder = require('string_decoder').StringDecoder;
var Writable = require('stream').Writable;
var utils = require('../lib/utils.js');
var nano = require('nano');
var argv = require('optimist')
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

var s3 = utils.s3();

// ImportStream class writes to CouchDB
//
util.inherits(ImportStream, Writable);
function ImportStream(opts) {
    Writable.call(this, opts);
    this._buffer = '';
    this._bufferLim = Math.pow(2, 18)
    this._decoder = new StringDecoder('utf8')
}
ImportStream.prototype._write = function(chunk, encoding, done) {
    this._buffer += this._decoder.write(chunk);
    if (this._buffer.length < this._bufferLim) return done();

    this.flush(done);
};
ImportStream.prototype.flush = function(done) {
    var docs = this._buffer.split('\n');
    this._buffer = docs.pop();

    docs = docs.filter(function(v) {
        return v.length > 0;
    }).map(function(v) {
        v = JSON.parse(v);
        // support wrapped docs.
        if (v.doc && v.doc._id) return v.doc;
        return v;
    });

    db.bulk({ new_edits: false, docs: docs }, {}, done);
};

var d = new Date(Date.now() - 864e5); // Look 1 day back.

s3.listObjects({
    Bucket: argv.inputBucket,
    Prefix: util.format('db/%s-', remoteName),
    Marker: util.format('db/%s-%s-%s-%s', remoteName, d.getUTCFullYear(),
        utils.pad(d.getUTCMonth() + 1), utils.pad(d.getUTCDate()))
}, function(err, data) {
    if (err) throw err;
    if (data.Contents.length === 0)
        throw new Error('Unable to locate db on s3');

    var key = data.Contents.pop().Key

    var reader = s3.getObject({
        Bucket: argv.inputBucket,
        Key: key
    }).createReadStream();

    var importer = new ImportStream();
    var finish = function() {
        importer.flush(function(err) {
            if (err) throw err;
            console.log('%s : Imported %s into %s/%s', (new Date()), key, dbUrl.host, dbName);
        });
    };

    if (/\.gz$/.test(key)) {
        var gunzip = zlib.createGunzip();
        reader.pipe(gunzip).pipe(importer, { end: false });
        gunzip.on('end', finish);
    } else {
        reader.pipe(importer, { end: false });
        reader.on('end', finish);
    }
});
