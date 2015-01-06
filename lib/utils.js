require('dotenv').load();
var parseArgs = require('minimist');
var AWS = require('aws-sdk');

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

AWS.config.update({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    sessionToken: process.env.AWS_SESSION_TOKEN,
    region: process.env.AWS_DEFAULT_REGION || 'us-east-1'
});
module.exports.s3 = new AWS.S3;

// `opts` should be an object with keys: demand, optional, usage
module.exports.config = function(opts) {
    var args = parseArgs(process.argv.slice(2), opts);

    var filtered = {};
    opts.demand.concat(opts.optional).forEach(function(k) {
        if (args[k] !== undefined) filtered[k] = args[k];
    });

    // Ensure required params are present
    if (opts.demand.reduce(function(m, k) {
        return m || filtered[k] == undefined;
    }, false)) {
        console.error('> Missing required argument')
        console.error(opts.usage);
        process.exit(1);
    }

    return filtered;
};
