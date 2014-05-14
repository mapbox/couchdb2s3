#!/usr/bin/env node

process.title = 'couchdb2s3';

var util = require('util');
var fs = require('fs');
var url = require('url');
var crypto = require('crypto');
var zlib = require('zlib');
var StringDecoder = require('string_decoder').StringDecoder;
var Transform = require('stream').Transform;
var AWS = require('aws-sdk');
var nano = require('nano');
var utils = require('../lib/utils.js');

var argv = utils.config({
    demand: ['outputBucket', 'database'],
    optional: ['gzip', 'awsKey', 'awsSecret'],
    usage: 'Export CouchDB Database to s3\n' +
           'Usage: $0 [options] [--gzip]'
});

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

var d = new Date();
var s3Key = util.format('db/%s-%s-%s-%s-%s-%s', dbName, d.getUTCFullYear(),
    utils.pad(d.getUTCMonth() + 1), utils.pad(d.getUTCDate()), utils.pad(d.getUTCHours()), rand);

// LineProcessor class, transforms database into line oriented records.
//
util.inherits(LineProcessor, Transform);
function LineProcessor(opts) {
    opts = opts || {};
    opts.decodeStrings = true;
    Transform.call(this, opts);

    // String decoder provides better utf8 support.
    this._decoder = new StringDecoder('utf8');

    // Buffer
    this._bufferLim = Math.pow(2, 18)
    this._buffer = '';

    // Iterator variables
    this._lines = [];
    this._errorCount = 0;
}
LineProcessor.prototype.lineIterator = function(line, i, arr) {
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
    this._buffer += this._decoder.write(chunk);

    // Documents can be as large as 10mb
    if (this._buffer.length < this._bufferLim)
        return done();

    // split on newlines
    var lines = this._buffer.split('\n')
    // keep the last partial line buffered
    this._buffer = lines.pop();

    lines.forEach(this.lineIterator, this);

    // First and last lines will always fail to parse
    if (this._errorCount > 2)
        return done(new Error('Failed to parse database'));

    this.push(this._lines.join(''));
    this._lines = [];
    done();
};
LineProcessor.prototype._flush = function(done) {
    this._buffer.split('\n').forEach(this.lineIterator, this);

    if (this._errorCount != 2) {
        return done(new Error('Failed to parse database'));
    }
    this.push(this._lines.join(''));
    done();
};

var parser = new LineProcessor();
var writer = fs.createWriteStream(tempFilepath);

var dbStream = db.relax({
    db: dbName,
    path: '_all_docs',
    params: { include_docs: true},
    method: 'GET'
}).pipe(parser)

var fsStream;
if (argv.gzip) {
    var gzip = zlib.createGzip();
    fsStream = dbStream.pipe(gzip).pipe(writer);
    s3Key += '.gz';
} else {
    fsStream = dbStream.pipe(writer);
}

fsStream.on('error', function(err) {
    console.error(err);
    process.exit(1);
});
fsStream.on('finish', function() {
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

