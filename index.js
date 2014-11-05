var debug = require('debug')(require('./package.json').name);
var path = require('path');
var _ = require('lodash');
var fs = require('fs-extra');
var Decompress = require('decompress');
var Download = require('download');
var async = require('async');
var exec = require('child_process').exec;



var download = function(args, done) {
  debug('download', args);

  if (!args) {
    return done(new Error('args missing'));
  } else if (!args.url) {
    return done(new Error('url missing'));
  } else if (!args.dir) {
    return done(new Error('dir missing'));
  }

  args.strip = args.strip || 1;

  try {
    fs.mkdirsSync(args.dir);
  } catch(err) {
    return done(err);
  }

  var download = new Download({
    extract: true, //TODO: make this configurable through args?
    strip: args.strip
  }).get(args.url);

  if (args.file_name) download.rename(file_name);

  download.dest(args.dir).run(function(err, files) {
    done(err);
  });
};

var extract = function(args, done) {
  debug('extract', args);

  if (!args) {
    return done(new Error('args missing'));
  } else if (!args.file) {
    return done(new Error('file missing'));
  } else if (!args.dir) {
    return done(new Error('dir missing'));
  }

  try {
    fs.mkdirsSync(args.dir);
  } catch(err) {
    return done(err);
  }

  var decompress = new Decompress({
    mode: '755' //TODO: configurable?
  }).src(args.file).dest(args.dir)
    .use(Decompress.zip({ strip: 1 }))
    .use(Decompress.tar({ strip: 1 }))
    .use(Decompress.targz({ strip: 1 }))
    .use(Decompress.tarbz2({ strip: 1 }));

  decompress.run(function(err) {
    done(err);
  });
};

var checkoutGit = function(args, done) {
  debug('checkout', args);

  if (!args) {
    return done(new Error('args missing'));
  } else if (!args.url) {
    return done(new Error('url missing'));
  } else if (!args.dir) {
    return done(new Error('dir missing'));
  }

  var git = exec('git clone --recursive ' + args.url + ' ' + args.dir, function(err, stdout, stderr) {
    if (err) {
      err.stdout = stdout;
      err.stderr = stderr;

      return done(err);
    }

    done();
  });
};

var checkoutBzr = function(args, done) {
  debug('checkout', args);

  if (!args) {
    return done(new Error('args missing'));
  } else if (!args.url) {
    return done(new Error('url missing'));
  } else if (!args.dir) {
    return done(new Error('dir missing'));
  }

  var git = exec('bzr branch ' + args.url + ' ' + args.dir, function(err, stdout, stderr) {
    if (err) {
      err.stdout = stdout;
      err.stderr = stderr;

      return done(err);
    }

    done();
  });
};

var readInput = function(context, callback) {
  var spec;

  if (context && context.apispec_path) {
    try {
      if (fs.statSync(context.apispec_path).isDirectory()) {
        context.apispec_path = path.join(context.apispec_path, 'apispec.json');
      }

      var apiSpecPathAbs = path.resolve(context.apispec_path);

      if (!fs.existsSync(apiSpecPathAbs)) {
        return callback(new Error('API spec ' + apiSpecPathAbs + ' missing'));
      }

      spec = JSON.parse(fs.readFileSync(context.apispec_path));

      spec.apispec_path = apiSpecPathAbs;

      if (context.invoker_path) {
        spec.invoker_path = context.invoker_path;
      }
    } catch (err) {
      return callback(err);
    }
  } else if (process.env.APISPEC) {
    try {
      spec = JSON.parse(process.env.APISPEC);
    } catch (err) {
      return callback(err);
    }
  } else {
    return callback(new Error('neither environment variable APISPEC nor context.apispec_path defined'));
  }

  var params = {};

  try {
    if (process.env.PARAMETERS) {
      params = JSON.parse(process.env.PARAMETERS);
    }
  } catch (err) {
    return callback(err);
  }

  _.each(spec.parameters, function(param, name) {
    if (!params[name] && param.default) params[name] = param.default;
  });

  callback(null, spec, params);
};

var placeExecutable = function(args, callback) {
  if (!args) {
    return done(new Error('args missing'));
  } else if (!args.spec) {
    return done(new Error('spec missing'));
  } else if (!args.access) {
    return done(new Error('access missing'));
  } else if (!args.dir) {
    return done(new Error('dir missing'));
  }

  var localExecPath = path.resolve(args.spec.apispec_path, '..', args.spec.executable_path);

  args.spec.apispec_path = path.join(args.dir, 'apispec.json');

  args.spec.executable_path = args.dir;

  async.series([
    async.apply(args.access.mkdir, { path: args.spec.executable_path }),
    async.apply(args.access.writeFile, { path: args.spec.apispec_path, content: JSON.stringify(args.spec) }),
    async.apply(args.access.copyDirToRemote, { sourcePath: localExecPath, targetPath: args.spec.executable_path })
  ], callback);
};



module.exports = {
  download: download,
  extract: extract,
  checkoutGit: checkoutGit,
  checkoutBzr: checkoutBzr,
  readInput: readInput,
  placeExecutable: placeExecutable
};
