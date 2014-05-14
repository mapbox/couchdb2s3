var env = require('superenv')('couchdb2s3');
var parseArgs = require('minimist');

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

// `opts` should be an object with keys: demand, optional, usage
module.exports.config = function(opts) {
    var args = parseArgs(process.argv.slice(2), opts);

    var supported = opts.demand.concat(opts.optional);

    // Allow arguments to be specified in a config file.
    var filtered = {};
    supported.forEach(function(k) {
        if (args[k] !== undefined) filtered[k] = args[k];
        else if (env[k] !== undefined) filtered[k] = env[k];
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
