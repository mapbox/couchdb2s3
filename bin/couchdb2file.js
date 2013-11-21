#!/usr/bin/env node

var util = require('util');
var fs = require('fs');
var url = require('url');
var crypto = require('crypto');
var Step = require('step');
var request = require('request');
var carrier = require('carrier');
var argv = require('optimist')
    .config(['config', 'jobflows'])
    .usage('Export CouchDB Database to s3\n' +
           'Usage: $0 [options]'
    )
    .demand(['database', 'file'])
    .argv;
var s3Client;

process.title = 'couchdb2file';

var dbUrl = url.parse(argv.database);
var dbName = dbUrl.pathname.split('/')[1];
var dbClient;

var uri = url.format({
    protocol: dbUrl.protocol,
    host: dbUrl.host,
    pathname: dbUrl.pathname + "/_all_docs",
    query: {include_docs:"true"}
});

var next = this;
var lines = [];
var errorCount = 0;

request({uri: uri, auth: dbUrl.auth})
   .on('error', function() { throw new Error("Could not connect to CouchDB"); })
   .on('response', function(res) {
       if (res.statusCode != 200) throw new Error("Bad response from CouchDB");
       carrier.carry(res, function(line) {
           try {
               line = JSON.parse(line.replace(/(,$)/, ""));
               lines.push(JSON.stringify(line.doc));
           }
           catch(e) {
               errorCount++;
           }
        }).on('end', function() {
            // Error count should be exactly 2.
            if (errorCount == 2) return done(null, lines);
            done(new Error("Failed to parse database"));
        });
    });

function done(err, data) {
    if (err) throw err;
    fs.writeFile(argv.file, data.join('\n') + '\n', 'utf8');
}
