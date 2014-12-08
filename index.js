var debug = require('debug')(require('./package.json').name);
var path = require('path');
var _ = require('lodash');
var fs = require('fs-extra');
var Decompress = require('decompress');
var Download = require('download');
var async = require('async');
var shortId = require('shortid');
var childProc = require('child_process');



var specFile = 'apispec.json';



var download = function(args, callback) {
  debug('download', args);

  if (!args) {
    return callback(new Error('args missing'));
  } else if (!args.url) {
    return callback(new Error('url missing'));
  } else if (!args.dir) {
    return callback(new Error('dir missing'));
  }

  args.strip = args.strip || 1;

  try {
    fs.mkdirsSync(args.dir);
  } catch(err) {
    return callback(err);
  }

  var download = new Download({
    extract: true, //TODO: make this configurable through args?
    strip: args.strip
  }).get(args.url);

  if (args.file_name) download.rename(file_name);

  download.dest(args.dir).run(function(err, files) {
    callback(err);
  });
};

var extract = function(args, callback) {
  debug('extract', args);

  if (!args) {
    return callback(new Error('args missing'));
  } else if (!args.file) {
    return callback(new Error('file missing'));
  } else if (!args.dir) {
    return callback(new Error('dir missing'));
  }

  try {
    fs.mkdirsSync(args.dir);
  } catch(err) {
    return callback(err);
  }

  var decompress = new Decompress({
    mode: '755' //TODO: configurable?
  }).src(args.file).dest(args.dir)
    .use(Decompress.zip({ strip: 1 }))
    .use(Decompress.tar({ strip: 1 }))
    .use(Decompress.targz({ strip: 1 }))
    .use(Decompress.tarbz2({ strip: 1 }));

  decompress.run(function(err) {
    callback(err);
  });
};

var checkoutGit = function(args, callback) {
  debug('checkout', args);

  if (!args) {
    return callback(new Error('args missing'));
  } else if (!args.url) {
    return callback(new Error('url missing'));
  } else if (!args.dir) {
    return callback(new Error('dir missing'));
  }

  var git = childProc.exec('git clone --recursive ' + args.url + ' ' + args.dir, function(err, stdout, stderr) {
    if (err) {
      err.stdout = stdout;
      err.stderr = stderr;

      return callback(err);
    }

    callback();
  });
};

var checkoutBzr = function(args, callback) {
  debug('checkout', args);

  if (!args) {
    return callback(new Error('args missing'));
  } else if (!args.url) {
    return callback(new Error('url missing'));
  } else if (!args.dir) {
    return callback(new Error('dir missing'));
  }

  var git = childProc.exec('bzr branch ' + args.url + ' ' + args.dir, function(err, stdout, stderr) {
    if (err) {
      err.stdout = stdout;
      err.stderr = stderr;

      return callback(err);
    }

    callback();
  });
};

var readInput = function(args, callback) {
  args = args || {};
  args.specPath = args.specPath || args.apispec_path;

  var apiSpec;

  if (args.specPath) {
    try {
      if (fs.statSync(args.specPath).isDirectory()) {
        args.specPath = path.join(args.specPath, specFile);
      }

      var apiSpecPathAbs = path.resolve(args.specPath);

      if (!fs.existsSync(apiSpecPathAbs)) {
        return callback(new Error('API spec ' + apiSpecPathAbs + ' missing'));
      }

      apiSpec = JSON.parse(fs.readFileSync(args.specPath));

      apiSpec.apispec_path = apiSpecPathAbs;

      /*
      if (args.invokers) {
        apiSpec.invokers = apiSpec.invokers || {};

        _.merge(apiSpec.invokers, args.invokers);
      }
      */
    } catch (err) {
      return callback(err);
    }
  } else if (process.env.APISPEC) {
    try {
      apiSpec = JSON.parse(process.env.APISPEC);
    } catch (err) {
      return callback(err);
    }
  } else {
    return callback(new Error('neither environment variable \'APISPEC\' nor argument \'apispec_path\' defined'));
  }

  apiSpec.executables = apiSpec.executables || {};
  apiSpec.invokers = apiSpec.invokers || {};
  apiSpec.implementation = apiSpec.implementation || {};

  var params = {};

  try {
    if (process.env.PARAMETERS) {
      params = JSON.parse(process.env.PARAMETERS);
    }
  } catch (err) {
    return callback(err);
  }

  _.each(apiSpec.parameters_schema, function(param, name) {
    if (!params[name] && param.default) params[name] = param.default;
  });

  callback(null, apiSpec, params);
};

var writeSpec = function(args, callback) {
  if (!args) {
    return callback(new Error('args missing'));
  } else if (!args.apiSpec) {
    return callback(new Error('API spec missing'));
  }

  var apiSpec = args.apiSpec;
  var specPath = args.specPath || apiSpec.apispec_path;

  delete apiSpec.apispec_path;

  var writeFile;

  if (args.access) {
    writeFile = function(callback) {
      args.access.writeFile(
        { path: specPath, content: JSON.stringify(apiSpec, null, 2) }, callback);
    }
  } else {
    writeFile = function(callback) {
      fs.writeFile(specPath, JSON.stringify(apiSpec, null, 2), callback);
    }
  }

  writeFile(function(err) {
    if (err) return callback(err);

    if (specPath) apiSpec.apispec_path = specPath;

    callback(null, apiSpec);
  });
};

var cloneSpec = function(apiSpec, callback) {
  callback(null, cloneSpecSync(apiSpec));
};

var cloneSpecSync = function(apiSpec) {
  var clonedSpec = _.cloneDeep(apiSpec);

  clonedSpec.apispec_path = path.join(path.dirname(apiSpec.apispec_path), 'tmp-apispec-' + shortId.generate() + '.json');

  return clonedSpec;
};

var updateInvokers = function(args, done) {
  if (!args.invokers) return done();
  else if (!args.apiSpec) return done(new Error('API spec missing'));

  var invokers = args.invokers;
  var apiSpec = args.apiSpec;

  apiSpec.invokers = apiSpec.invokers || {};

  _.each(apiSpec.executables, function(props, name) {
    if (props.invoker_name && apiSpec.invokers[props.invoker_name]) return;

    if (!props.invoker_name) props.invoker_name = props.type;

    if (!props.invoker_name) {
      return done(new Error('neither invoker name nor executable type defined in API spec for executable ' + name));
    } else {
      apiSpec.invokers[props.invoker_name] = apiSpec.invokers[props.invoker_name] || {};
    }
  });

  _.each(apiSpec.invokers, function(props, name) {
    props.path = props.path || invokers.getPathSync(name);

    if (!props.path) return done(new Error('invoker ' + name + ' missing'));

    props.expose = true;
  });

  done(null, apiSpec);
};

var prepareBuildtime = function(args, done) {
  var apiSpec = args.apiSpec;
  if (!apiSpec) return done(new Error('API spec missing'));

  var preparedInvokers = args.preparedInvokers || {};

  var invokerName = args.invoker_name;

  if (!invokerName && apiSpec.executables[args.executable_name]) {
    invokerName = apiSpec.executables[args.executable_name].invoker_name;
  }

  var invoker = apiSpec.invokers[invokerName];

  if (!invoker) return done(new Error('valid invoker or executable must be specified'));

  if (preparedInvokers[invokerName]) return done();

  preparedInvokers[invokerName] = true;

  childProc.exec('npm run prepare-buildtime',
    { cwd: path.resolve(apiSpec.apispec_path, '..', invoker.path),
      env: { PATH: process.env.PATH } },
    function(err, stdout, stderr) {
      if (err) {
        err.stderr = stderr;
        err.stdout = stdout;

        return done(err);
      }

      done();
    });
};

var prepareExecutable = function(args, done) {
  var apiSpec = args.apiSpec;
  if (!apiSpec) return done(new Error('API spec missing'));

  if (apiSpec.executables[args.executable_name].prepared) return done();

  var invoker = null;

  if (apiSpec.executables[args.executable_name]) {
    invoker = apiSpec.invokers[apiSpec.executables[args.executable_name].invoker_name];
  }

  if (!invoker) return done(new Error('valid executable with invoker assigned must be specified'));

  debug('preparing executable', apiSpec.executables[args.executable_name]);

  apiSpec.executables[args.executable_name].prepared = true;

  childProc.exec('npm run prepare-executable',
    { cwd: path.resolve(apiSpec.apispec_path, '..', invoker.path),
      env: { APISPEC: JSON.stringify(apiSpec),
             PARAMETERS: JSON.stringify({ _: { executable_name: args.executable_name } }),
             PATH: process.env.PATH } },
    function(err, stdout, stderr) {
      if (err) {
        err.stderr = stderr;
        err.stdout = stdout;

        return done(err);
      }

      readInput(apiSpec, done);
    });
};



module.exports = {
  download: download,
  extract: extract,
  checkoutGit: checkoutGit,
  checkoutBzr: checkoutBzr,
  readInput: readInput,
  writeSpec: writeSpec,
  cloneSpec: cloneSpec,
  cloneSpecSync: cloneSpecSync,
  updateInvokers: updateInvokers,
  prepareBuildtime: prepareBuildtime,
  prepareExecutable: prepareExecutable,
};
