var AWS = require('aws-sdk');
var env = require('superenv')('couchdb2s3');

module.exports = {};
module.exports.pad = function(n) {
    var prefix = function(v, l) {
        while (v.length < l) { v = '0' + v; }
        return v;
    };
    var len = 2;
    var s = n.toString();
    return s.length == len ? s : prefix(s, len);
};

module.exports.s3 = function() {
    AWS.config.update({
        accessKeyId: env.awsKey,
        secretAccessKey: env.awsSecret,
        region: 'us-east-1'
    });
    return new AWS.S3;
};
