#!/usr/bin/env node

var util = require('util');
var fs = require('fs');
var url = require('url');
var crypto = require('crypto');
var Transform = require('stream').Transform;
var AWS = require('aws-sdk');
var nano = require('nano');
var argv = require('optimist')
    .config(['config', 'jobflows'])
    .usage('Export CouchDB Database to s3\n' +
           'Usage: $0 [options]'
    )
    .demand(['outputBucket', 'database'])
    .argv;

process.title = 'couchdb2s3';

var dbUrl = url.parse(argv.database);
var dbName = dbUrl.pathname.split('/')[1];
var db = nano(url.format({
    protocol: dbUrl.protocol,
    host: dbUrl.host,
    auth: dbUrl.auth
}));

AWS.config.update({
    accessKeyId: argv.awsKey,
    secretAccessKey: argv.awsSecret,
    region: 'us-east-1'
});
var s3 = new AWS.S3;

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
var s3Key = util.format('db/%s-%s-%s-%s-%s-%s', dbName, d.getUTCFullYear(),
    pad(d.getUTCMonth() + 1), pad(d.getUTCDate()), pad(d.getUTCHours()), rand);

// LineProcessor class, transforms database into line oriented records.
//
util.inherits(LineProcessor, Transform);
function LineProcessor(opts) {
    opts = opts || {};
    opts.decodeStrings = true;
    Transform.call(this, opts);

    // Buffer
    this.bufferLim = Math.pow(2, 18)
    this.buffer = '';

    // Iterator variables
    this._extra = '';
    this._lines = [];
    this._errorCount = 0;
}
LineProcessor.prototype.lineIterator = function(line, i, arr) {
    // The last element may be truncated, just hold onto it for now.
    if (i == (arr.length - 1)) {
        this._extra = line;
        return;
    }

    if (line.length == 0) return;

    try {
        line = JSON.parse(line.replace(/,\s?$/, ""));
        line = JSON.stringify(line.doc) + "\n";
        this._lines.push(line);
    }
    catch(e) {
        this._errorCount++;
    }
};

LineProcessor.prototype._transform = function(chunk, encoding, done) {
    this.buffer += chunk.toString('utf8');
    if (this.buffer.length < this.bufferLim) return done();

    chunk = this._extra + this.buffer;
    this.buffer = '';
    this._extra = '';

    chunk.split('\n').forEach(this.lineIterator, this);

    if (this._errorCount > 2)
        return done(new Error('Failed to parse database'));

    this.push(this._lines.join(''));
    this._lines = [];
    done();
};
LineProcessor.prototype._flush = function(done) {
    (this._extra + this.buffer).split('\n').forEach(this.lineIterator, this);

    if (this._errorCount != 2) {
        return done(new Error('Failed to parse database'));
    }
    this.push(this._lines.join(''));
    done();
};

var parser = new LineProcessor();
var writer = fs.createWriteStream(tempFilepath);

db.relax({
    db: dbName,
    path: '_all_docs',
    params: { include_docs: true},
    method: 'GET'
}).pipe(parser).pipe(writer).on('error', function(err) {
    console.error(err);
    process.exit(1);
}).on('finish', function() {
    var reader = fs.createReadStream(tempFilepath);
    s3.putObject({
        Bucket: argv.outputBucket,
        Key: s3Key,
        Body: reader,
        ServerSideEncryption:'AES256'
    }, function(err) {
        if (err) {
            console.error(err);
            process.exit(1);
        }
        console.log('%s : Uploaded %s database to %s', (new Date()), argv.database, s3Key);
        fs.unlink(tempFilepath);
    });
});

