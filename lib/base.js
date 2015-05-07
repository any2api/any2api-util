var debug = require('debug')(require('../package.json').name);
var path = require('path');
var _ = require('lodash');
var fs = require('fs-extra');
var S = require('string');
var Decompress = null;
var Download = null;
var async = require('async');
var shortId = require('shortid');
var through2 = require('through2');
var split = require('split');
var concat = require('concat-stream');
var temp = require('temp').track();
var request = null;
var childProc = require('child_process');
var specUtil = require('./spec');



var childProcTimeout = 5 * 60 * 1000; // 5mins
var childProcKillSignal = 'SIGKILL';
var childProcMaxBuffer = 500 * 1024; // 500kb



var download = function(args, callback) {
  debug('download', args);

  if (!Download) Download = require('download');

  args = args || {};

  if (!args.url) {
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
    extract: true, //TODO: make configurable
    strip: args.strip
  }).get(args.url);

  if (args.file_name) download.rename(file_name);

  download.dest(args.dir).run(function(err, files) {
    callback(err);
  });
};

var extract = function(args, callback) {
  debug('extract', args);

  if (!Decompress) Decompress = require('decompress');

  args = args || {};

  if (!args.file) {
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
    mode: '755' //TODO: make configurable
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

  args = args || {};

  if (!args.url) {
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

  args = args || {};

  if (!args.url) {
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

var updateInvokers = function(args, done) {
  if (!args.invokers) return done();
  else if (!args.apiSpec) return done(new Error('API spec missing'));

  var invokers = args.invokers;
  var apiSpec = args.apiSpec;

  apiSpec.invokers = apiSpec.invokers || {};

  _.each(apiSpec.executables, function(props, name) {
    if (props.invoker_name && apiSpec.invokers[props.invoker_name]) return;

    props.invoker_name = props.invoker_name || props.kind || props.type;

    if (!props.invoker_name) {
      return done(new Error('neither invoker name nor kind of executable defined in API spec for executable ' + name));
    } else {
      apiSpec.invokers[props.invoker_name] = apiSpec.invokers[props.invoker_name] || {};
    }
  });

  _.each(apiSpec.invokers, function(props, name) {
    if (props.path && !fs.existsSync(path.join(path.dirname(apiSpec.apispec_path), props.path, 'invoker.json'))) {
      return done(new Error('invalid path specified for invoker ' + name + ': invoker.json file not found'));
    }

    props.path = props.path || invokers.getPathSync(name);

    if (!props.path) return done(new Error('invoker ' + name + ' missing'));

    if (!_.isBoolean(props.expose)) props.expose = true;
  });

  done(null, apiSpec);
};

var prepareInvoker = function(args, done) {
  args = args || {};

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

  var options = { 
    cwd: path.resolve(apiSpec.apispec_path, '..', invoker.path),
    env: process.env || {},
    timeout: args.timeout || childProcTimeout,
    killSignal: childProcKillSignal,
    maxBuffer: childProcMaxBuffer
  };

  childProc.exec('npm run ' + args.command, options, function(err, stdout, stderr) {
    if (err) {
      err.stderr = stderr;
      err.stdout = stdout;

      return done(err);
    }

    done();
  });
};

var prepareBuildtime = function(args, done) {
  args = args || {};
  args.command = 'prepare-buildtime';

  prepareInvoker(args, done);
};

var prepareRuntime = function(args, done) {
  args = args || {};
  args.command = 'prepare-runtime';

  prepareInvoker(args, done);
};

var prepareExecutable = function(args, done) {
  var apiSpec = args.apiSpec;
  if (!apiSpec) return done(new Error('API spec missing'));

  var executable = apiSpec.executables[args.executable_name];
  var invoker = apiSpec.invokers[executable.invoker_name];

  executable.name = args.executable_name;
  invoker.name = executable.invoker_name;

  if (executable.prepared) return done();

  if (!invoker) return done(new Error('valid executable with invoker assigned must be specified'));

  debug('preparing executable', apiSpec.executables[args.executable_name]);

  var options = {
    cwd: path.resolve(apiSpec.apispec_path, '..', invoker.path),
    env: {
      APISPEC_INVOKER: JSON.stringify(invoker),
      APISPEC_EXECUTABLE: JSON.stringify(executable),
      APISPEC_PATH: apiSpec.apispec_path
    },
    timeout: args.timeout || childProcTimeout,
    killSignal: childProcKillSignal,
    maxBuffer: childProcMaxBuffer
  };

  if (apiSpec.enriched) options.env.APISPEC_ENRICHED = 'true';

  options.env = _.extend(_.clone(process.env || {}), options.env);

  childProc.exec('npm run prepare-executable', options, function(err, stdout, stderr) {
    if (err) {
      err.stderr = stderr;
      err.stdout = stdout;

      return done(err);
    }

    specUtil.readSpec({ specPath: apiSpec.apispec_path }, function(err, apiSpec) {
      if (err) return done(err);

      apiSpec.executables[args.executable_name].prepared = true;

      done(null, apiSpec);
    });
  });
};

var persistExecutable = function(args, done) {
  args = args || {};
  callback = _.once(callback);

  var executable = args.executable;

  if (!executable) return done(new Error('Executable missing'));
  else if (!executable.files) return done(new Error('Executable has no files'));

  //TODO: support executable.tarball.url|base64

  debug('persisting executable', executable);

  temp.mkdir('tmp-executable-' + executable.name, function(err, execPath) {
    executable.path = execPath;

    async.eachSeries(executable.files, function(file, callback) {
      if (!file.path) return callback();

      fs.mkdirs(path.join(execPath, path.dirname(file.path)), function(err) {
        if (err) return callback(err);

        debug('persisting file', file);

        if (file.text) {
          fs.writeFile(path.join(execPath, file.path), file.text, 'utf8', callback);
        } else if (file.object) {
          fs.writeFile(path.join(execPath, file.path), JSON.stringify(file.object), 'utf8', callback);
        } else if (file.base64) {
          fs.writeFile(path.join(execPath, file.path), file.base64, 'base64', callback);
        } else if (file.url) {
          request = require('request');

          request(file.url).pipe(fs.createWriteStream(path.join(execPath, file.path)))
            .on('finish', callback)
            .on('error', callback);
        } else {
          callback();
        }
      });
    }, done);
  });
};

var runInstance = function(args, done) {
  debug('run instance', args);

  args = args || {};
  args.npmLoglevel = /*args.npmLoglevel ||*/ 'silent'; // error, warn

  var apiSpec = args.apiSpec;
  if (!apiSpec) return done(new Error('API spec missing'));

  var preparedInvokers = args.preparedInvokers || {};

  var instance = args.instance || {};
  instance.created = instance.created || new Date().toString();

  var apiSpecCopy = null;
  var executable = null;
  var invoker = null;

  var paramsSchema = null;
  var paramsRequired = null;
  var resultsSchema = null;

  var instanceId = instance._id || instance.id || 'instance-' + shortId.generate();

  var paramsStream = args.parametersStream;
  var resultsStream = args.resultsStream;

  var resultFilesProcessed = [];

  async.series([
    function(callback) {
      specUtil.cloneSpec({ apiSpec: apiSpec }, function(err, cloned) {
        if (err) return callback(err);

        specUtil.enrichSpec({ apiSpec: cloned }, function(err, enriched) {
          apiSpecCopy = enriched;

          callback(err);
        });
      });
    },
    function(callback) {
      if (args.executable_name) {
        executable = apiSpecCopy.executables[args.executable_name];

        invoker = apiSpecCopy.invokers[executable.invoker_name];
      } else if (args.invoker_name) {
        invoker = apiSpecCopy.invokers[args.invoker_name];

        if (instance.executable) {
          executable = instance.executable;

          executable.name = executable.name || 'embedded-' + shortId.generate();

          executable.invoker_name = args.invoker_name;

          apiSpecCopy.executables[executable.name] = executable;
        }
      }

      if (executable) executable.name = executable.name || args.executable_name;

      invoker.name = invoker.name || args.invoker_name;

      paramsSchema = executable.parameters_schema || invoker.parameters_schema || {};
      paramsRequired = executable.parameters_required || invoker.parameters_required || {};
      resultsSchema = executable.results_schema || invoker.results_schema || {};

      callback();
    },
    function(callback) {
      if (!executable || !executable.files) return callback();

      persistExecutable({ executable: executable }, callback);
    },
    function(callback) {
      if (!executable || executable.prepared) return callback();

      debug('preparing buildtime');

      prepareBuildtime({
        apiSpec: apiSpecCopy,
        preparedInvokers: preparedInvokers,
        executable_name: executable.name
      }, callback);
    },
    function(callback) {
      if (!executable || executable.prepared) return callback();

      debug('preparing executable');

      var updateSpecCallback = function(err, updApiSpec) {
        if (err) return callback(err);

        if (updApiSpec) apiSpecCopy = updApiSpec;

        callback();
      };

      prepareExecutable({
        apiSpec: apiSpecCopy,
        executable_name: executable.name
      }, updateSpecCallback);
    },
    function(callback) {
      debug('running executable');

      //TODO kill spawned child after timeout
      var options = {
        cwd: invoker.path,
        env: {
          APISPEC_INVOKER: JSON.stringify(invoker),
          APISPEC_PATH: apiSpecCopy.apispec_path,
          //INSTANCE_PATH: instancePath,
          INSTANCE_ID: instanceId,
          INSTANCE_STORE_RESULTS: instance.store_results
          //INSTANCE_DEBUG: instance.debug
          //APISPEC: JSON.stringify(apiSpecCopy)
        },
        //timeout: instance.timeout || args.timeout || childProcTimeout,
        //killSignal: childProcKillSignal,
        //maxBuffer: childProcMaxBuffer
      };

      if (executable) options.env.APISPEC_EXECUTABLE = JSON.stringify(executable);
      if (apiSpecCopy.enriched) options.env.APISPEC_ENRICHED = 'true';

      options.env = _.extend(_.clone(process.env || {}), options.env);

      var child = childProc.spawn('npm', [ 'start', '--loglevel', args.npmLoglevel ], options);
      var stderrOutput = null;

      var concatStderrStream = concat(function(stderr) {
        stderrOutput = stderr || ' ';
      });

      if (paramsStream) paramsStream.pipe(child.stdin);
      else child.stdin.end();

      child.stderr.setEncoding('utf8');
      child.stderr.pipe(concatStderrStream);

      if (resultsStream) {
        child.stdout
          .pipe(split(function(chunk) {
            if (_.isEmpty(chunk)) return;
            else return JSON.parse(chunk);
          }))
          .pipe(resultsStream);
      }

      child.on('close', function(code) {
        concatStderrStream.end();
        //child.stderr.resume();
        //child.stdout.resume();

        var err = null;

        if (code !== 0) err = new Error('npm child process failed with exit code ' + code);

        if (err && stderrOutput) {
          err.stderr = stderrOutput;

          callback(err);
        } else if (err) {
          concatStderrStream.on('finish', function() {
            err.stderr = stderrOutput;

            callback(err);
          });
        } else {
          callback();
        }

        if (err && resultsStream) resultsStream.end();
      });
    },
  ], function(err) {
    if (err) {
      debug('error', err);

      instance.status = 'error';
      instance.failed = new Date().toString();

      instance.error = err.message;
    } else {
      instance.status = 'finished';
      instance.finished = new Date().toString();
    }

    async.parallel([
      function(callback) {
        fs.remove(apiSpecCopy.apispec_path, callback);
      }
    ], function(err2) {
      if (err2) console.error(err2);

      done(err, instance);
    });
  });
};

var generateExampleSync = function(args) {
  args = args || {};

  //TODO process results and results schema if given

  var example = {
    parameters: {
      invoker_config: {
        some_config_option: 'some_value'
      },
      some_param: 'some_value'
    }
  };

  var limit = args.limit || 6;
  var count = 0;

  _.each(args.parameters_schema, function(param, name) {
    if (count > limit && args.parameters_required && !_.contains(args.parameters_required, name)) return;

    if (param.default) {
      example.parameters[name] = param.default;
    } else if (param.type === 'object' && param.properties) {
      var p = example.parameters[name] = {};

      _.each(param.properties, function(props, name) {
        if (props.default) p[name] = props.default;
      });
    }

    count++;
  });

  return example;
};



module.exports = {
  download: download,
  extract: extract,
  checkoutGit: checkoutGit,
  checkoutBzr: checkoutBzr,

  updateInvokers: updateInvokers,
  prepareBuildtime: prepareBuildtime,
  prepareRuntime: prepareRuntime,
  prepareExecutable: prepareExecutable,
  persistExecutable: persistExecutable,
  runInstance: runInstance,
  generateExampleSync: generateExampleSync,

  embeddedExecutableSchema: require('../executable_schema.json'),
  embeddedExecutableSchemaXml: fs.readFileSync(path.resolve(__dirname, '../executable_schema.xsd'), { encoding: 'utf8' }),
  instanceSchema: require('../instance_schema.json'),
  instanceSchemaXml: fs.readFileSync(path.resolve(__dirname, '../instance_schema.xsd'), { encoding: 'utf8' }),
  instanceWritableSchema: require('../instance_writable_schema.json'),
  instanceWritableSchemaXml: fs.readFileSync(path.resolve(__dirname, '../instance_writable_schema.xsd'), { encoding: 'utf8' })
};
