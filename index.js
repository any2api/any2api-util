var debug = require('debug')(require('./package.json').name);
var path = require('path');
var _ = require('lodash');
var fs = require('fs-extra');
var S = require('string');
var Decompress = null;
var Download = null;
var async = require('async');
var shortId = require('shortid');
var temp = null;
var request = null;
var childProc = require('child_process');



var specFile = 'apispec.json';
var childProcTimeout = 2 * 60 * 1000; // 2mins
var childProcKillSignal = 'SIGKILL';



var validTypes = [ 'boolean', 'number', 'string', 'binary', 'json_object', 'json_array', 'xml_object' ];

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
      params._ = params._ || {};

      var executable = apiSpec.executables[params._.executable_name];
      var invoker = apiSpec.invokers[params._.invoker_name];

      var paramsSchema;

      if (executable) {
        paramsSchema = executable.parameters_schema;
      } else if (invoker) {
        paramsSchema = invoker.parameters_schema;
      }

      if (paramsSchema) {
        _.each(params, function(value, name) {
          if (paramsSchema[name] && paramsSchema[name].type === 'binary') {
            if (_.isArray(value)) params[name] = new Buffer(value);
            else if (_.isString(value)) params[name] = new Buffer(value, 'base64');
          }
        });
      }
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
  args = args || {};
  
  if (!args.apiSpec) return callback(new Error('API spec missing'));

  var apiSpec = args.apiSpec;
  var specPath = args.specPath || apiSpec.apispec_path;

  delete apiSpec.apispec_path;

  var writeFile;

  if (args.access) {
    writeFile = function(callback) {
      args.access.writeFile({
        path: specPath,
        content: JSON.stringify(apiSpec, null, 2),
        encoding: 'utf8'
      }, callback);
    }
  } else {
    writeFile = function(callback) {
      fs.writeFile(specPath, JSON.stringify(apiSpec, null, 2), { encoding: 'utf8' }, callback);
    }
  }

  writeFile(function(err) {
    if (err) return callback(err);

    if (specPath) apiSpec.apispec_path = specPath;

    callback(null, apiSpec);
  });
};

var cloneSpec = function(args, callback) {
  args = args || {};

  if (!args.apiSpec) return callback(new Error('API spec missing'));

  var apiSpec = args.apiSpec;

  var clonedSpec = _.cloneDeep(apiSpec);

  clonedSpec.apispec_path = path.join(path.dirname(apiSpec.apispec_path), 'tmp-apispec-' + shortId.generate() + '.json');

  writeSpec({ apiSpec: clonedSpec }, function(err, writtenSpec) {
    if (err) return callback(err);

    callback(null, writtenSpec);
  });
};

var enrichSpec = function(args, done) {
  args = args || {};

  var apiSpec = args.apiSpec;
  if (!apiSpec) return done(new Error('API spec missing'));

  if (apiSpec.enriched) return done(null, apiSpec);

  var basePath = args.basePath || path.dirname(apiSpec.apispec_path);

  async.series([
    function(callback) {
      async.each(_.keys(apiSpec.invokers), function(name, callback) {
        var invoker = apiSpec.invokers[name];

        getInvokerJson({ apiSpec: apiSpec, invokerName: name, basePath: basePath }, function(err, invokerJson) {
          if (err) return callback(err);

          invoker.parameters_schema = invokerJson.parameters_schema || {};
          invoker.parameters_required = invokerJson.parameters_required || [];
          invoker.results_schema = invokerJson.results_schema || {};

          _.each(invoker.parameters_schema, resolveTypeSync);
          _.each(invoker.results_schema, resolveTypeSync);

          callback();
        });
      }, callback);
    },
    function(callback) {
      _.each(apiSpec.executables, function(executable, name) {
        var invoker = apiSpec.invokers[executable.invoker_name];

        if (!invoker) return;

        var paramsSchema = _.clone(invoker.parameters_schema);
        executable.parameters_schema = _.extend(paramsSchema, executable.parameters_schema);

        var paramsRequired = invoker.parameters_required || [];
        executable.parameters_required = _.uniq(paramsRequired.concat(executable.parameters_required || []));

        var resultsSchema = _.clone(invoker.results_schema);
        executable.results_schema = _.extend(resultsSchema, executable.results_schema);

        _.each(executable.parameters_schema, resolveTypeSync);
        _.each(executable.results_schema, resolveTypeSync);
      });

      callback();
    }
  ], function(err) {
    if (err) return done(err);

    apiSpec.enriched = true;

    done(null, apiSpec);
  });
};

var getInvokerJson = function(args, done) {
  args = args || {};

  var apiSpec = args.apiSpec;
  if (!apiSpec) return done(new Error('API spec missing'));

  var basePath = args.basePath || path.dirname(apiSpec.apispec_path);

  var invokerPath;

  if (args.executableName) {
    var executable = apiSpec.executables[args.executableName];

    invokerPath = apiSpec.invokers[executable.invoker_name].path;
  } else if (args.invokerName) {
    invokerPath = apiSpec.invokers[args.invokerName].path;
  }

  if (!invokerPath) return done(new Error('invoker path cannot be determined'));
  else invokerPath = path.resolve(basePath, invokerPath);

  fs.readFile(path.join(invokerPath, 'invoker.json'), function(err, content) {
    if (err) return done(err);

    var invokerJson = JSON.parse(content);

    invokerJson.parameters_required = invokerJson.parameters_required || [];
    invokerJson.parameters_schema = invokerJson.parameters_schema || {};
    invokerJson.results_schema = invokerJson.results_schema || {};

    invokerJson.path = invokerPath;

    done(null, invokerJson);
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
    killSignal: childProcKillSignal
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

  if (apiSpec.executables[args.executable_name].prepared) return done();

  var invoker = null;

  if (apiSpec.executables[args.executable_name]) {
    invoker = apiSpec.invokers[apiSpec.executables[args.executable_name].invoker_name];
  }

  if (!invoker) return done(new Error('valid executable with invoker assigned must be specified'));

  debug('preparing executable', apiSpec.executables[args.executable_name]);

  var options = {
    cwd: path.resolve(apiSpec.apispec_path, '..', invoker.path),
    env: {
      APISPEC: JSON.stringify(apiSpec),
      PARAMETERS: JSON.stringify({ _: { executable_name: args.executable_name } })
    },
    timeout: args.timeout || childProcTimeout,
    killSignal: childProcKillSignal
  };

  options.env = _.extend(_.clone(process.env || {}), options.env);

  childProc.exec('npm run prepare-executable', options, function(err, stdout, stderr) {
    if (err) {
      err.stderr = stderr;
      err.stdout = stdout;

      return done(err);
    }

    readInput(apiSpec, function(err, apiSpec) {
      if (err) return done(err);

      apiSpec.executables[args.executable_name].prepared = true;

      done(null, apiSpec);
    });
  });
};

var persistEmbeddedExecutable = function(args, done) {
  if (!temp) temp = require('temp').track();

  args = args || {};

  var executable = args.executable;

  if (!executable) return done(new Error('Executable missing'));
  else if (!executable.files) return done(new Error('Executable has no files'));

  //TODO: support executable.tarball_url

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

var invokeExecutable = function(args, done) {
  if (!temp) temp = require('temp').track();

  debug('invocation triggered', args);

  args = args || {};
  args.npmLoglevel = args.npmLoglevel || 'silent'; // error, warn

  var apiSpec = args.apiSpec;
  if (!apiSpec) return done(new Error('API spec missing'));

  var preparedInvokers = args.preparedInvokers || {};

  var instance = args.instance || args.run || {};
  var apiSpecCopy;
  var executable = null;

  var invokerPath;

  var instanceParams;
  var enrichedParams;

  async.series([
    function(callback) {
      cloneSpec({ apiSpec: apiSpec }, function(err, cloned) {
        if (err) return callback(err);

        enrichSpec({ apiSpec: cloned }, function(err, enriched) {
          apiSpecCopy = enriched;

          callback(err);
        });
      });
    },
    function(callback) {
      if (args.executable_name) {
        executable = apiSpecCopy.executables[args.executable_name];

        invokerPath = apiSpecCopy.invokers[executable.invoker_name].path;
      } else if (args.invoker_name) {
        invokerPath = apiSpecCopy.invokers[args.invoker_name].path;

        if (instance.executable) {
          executable = instance.executable;

          executable.name = executable.name || 'embedded-' + shortId.generate();

          executable.invoker_name = args.invoker_name;

          apiSpecCopy.executables[executable.name] = executable;
        }
      }

      if (_.isEmpty(instance.parameters)) instance.parameters = {};

      instanceParams = { instance_id: instance._id || instance.id || 'instance-' + shortId.generate(),
                         instance_path: args.instance_path || temp.path({ prefix: 'tmp-instance-' }) };
      enrichedParams = _.clone(instance.parameters);
      enrichedParams._ = instanceParams;

      if (executable) {
        executable.name = args.executable_name || executable.name;

        instanceParams.executable_name = executable.name;
      }

      _.each(executable.parameters_schema, function(p, name) {
        if (!enrichedParams[name] && _.contains(executable.parameters_required, name) && p.default) {
          enrichedParams[name] = p.default;
        }

        if (enrichedParams[name] && Buffer.isBuffer(enrichedParams[name])) {
          enrichedParams[name] = enrichedParams[name].toString('base64');
        }
      });

      debug('enriched params', enrichedParams);

      callback();
    },
    function(callback) {
      if (!executable || !executable.files) return callback();

      persistEmbeddedExecutable({ executable: executable }, callback);
    },
    function(callback) {
      if (executable && !executable.prepared) {
        debug('preparing buildtime');

        prepareBuildtime({ apiSpec: apiSpecCopy,
                           preparedInvokers: preparedInvokers,
                           executable_name: args.executable_name || executable.name },
                         callback);
      } else {
        callback();
      }
    },
    function(callback) {
      if (executable && !executable.prepared) {
        debug('preparing executable');

        var updateSpecCallback = function(err, updApiSpec) {
          if (err) return callback(err);

          if (updApiSpec) apiSpecCopy = updApiSpec;

          callback();
        };

        prepareExecutable({ apiSpec: apiSpecCopy,
                            executable_name: args.executable_name || executable.name },
                          updateSpecCallback);
      } else {
        callback();
      }
    },
    function(callback) {
      debug('running executable');

      var options = {
        cwd: invokerPath,
        env: {
          APISPEC: JSON.stringify(apiSpecCopy),
          PARAMETERS: JSON.stringify(enrichedParams)
        },
        timeout: instance.timeout || args.timeout || childProcTimeout,
        killSignal: childProcKillSignal
      };

      options.env = _.extend(_.clone(process.env || {}), options.env);

      childProc.exec('npm start --loglevel ' + args.npmLoglevel, options, function(err, stdout, stderr) {
        debug('instance finished');

        instance.results = instance.results || {};

        instance.results.stdout = stdout;
        instance.results.stderr = stderr;

        callback(err);
      });
    },
    function(callback) {
      var filesDir = instanceParams.instance_path;// || invokerPath;

      async.eachSeries(_.keys(executable.results_schema), function(name, callback) {
        var r = executable.results_schema[name] || {};

        if (r.mapping === 'stdout') {
          instance.results[name] = instance.results.stdout;

          delete instance.results.stdout;
        } else if (r.mapping === 'stderr') {
          instance.results[name] = instance.results.stderr;

          delete instance.results.stderr;
        } else if (r.mapping === 'file' && r.file_path) {
          var filePath = path.resolve(filesDir, r.file_path);

          if (!fs.existsSync(filePath)) {
            return callback(new Error('results file missing: ' + filePath));
          }

          var options = {};

          if (r.type !== 'binary') options.encoding = r.file_encoding || 'utf8';

          instance.results[name] = fs.readFileSync(filePath, options);
        }

        if (r.type === 'json_object' || r.type === 'json_array' || r.type === 'boolean' || r.type === 'number') {
          instance.results[name] = JSON.parse(instance.results[name]);
        }

        callback();
      }, callback);
    }
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
        fs.remove(instanceParams.instance_path, callback);
      },
      function(callback) {
        fs.remove(apiSpecCopy.apispec_path, callback);
      }
    ], function(err2) {
      if (err2) console.error(err2);

      done(err, instance);
    });
  });
};

var collectResults = function(args, done) {
  args = args || {};

  var apiSpec = args.apiSpec;
  if (!apiSpec) return done(new Error('API spec missing'));

  var access = args.access;
  if (!access) return done(new Error('access missing'));

  var localPath = args.localPath;
  if (!localPath) return done(new Error('localPath missing'));

  var remotePath = args.remotePath;
  if (!remotePath) return done(new Error('remotePath missing'));

  var executable = apiSpec.executables[args.executable_name];
  var invoker = apiSpec.invokers[args.invoker_name];

  if (!executable && !invoker) return done(new Error('either executable_name or invoker_name must be valid'));

  var resultsSchema;

  async.series([
    function(callback) {
      resultsSchema = executable.results_schema;

      if (apiSpec.enriched) return callback();

      getInvokerJson({ apiSpec: apiSpec,
                       executableName:args.executable_name,
                       invokerName: args.invoker_name }, function(err, invokerJson) {
        if (err) return callback(err);

        resultsSchema = _.extend(invokerJson.results_schema, executable.results_schema);

        callback();
      });
    },
    function(callback) {
      if (_.isEmpty(resultsSchema)) return callback();

      async.eachSeries(_.keys(resultsSchema), function(name, callback) {
        var r = resultsSchema[name];

        //TODO support for r.mapping = 'dir'
        if (r.mapping === 'file' && r.file_path) {
          var local = path.resolve(localPath, r.file_path);
          var remote = path.join(remotePath, r.file_path);

          access.exists({ path: remote }, function(err, exists) {
            if (err) return callback(err);

            if (!exists) {
              debug('file does not exist remotely: ' + remote);

              return callback();
            }

            var content = null;

            var readWriteArgs = { path: remote };
            if (r.type !== 'binary') readWriteArgs.encoding = r.encoding || r.file_encoding || 'utf8';

            async.series([
              async.apply(fs.mkdirs, path.dirname(local)),
              function(callback) {
                access.readFile(readWriteArgs, function(err, c) {
                  if (r.type === 'binary' && _.isString(c)) c = new Buffer(c, 'base64');

                  content = c;

                  callback(err);
                });
              },
              function(callback) {
                fs.writeFile(local, content, readWriteArgs, callback);
              }
            ], callback);
          });
        } else {
          callback();
        }
      }, callback); 
    }
  ], done);
};

var writeParameters = function(args, done) {
  args = args || {};

  var apiSpec = args.apiSpec;
  if (!apiSpec) return done(new Error('API spec missing'));

  var params = args.parameters;
  if (!params) return done(new Error('parameters missing'));

  var access = args.access;
  if (!access) return done(new Error('access missing'));

  var remotePath = args.remotePath;
  if (!remotePath) return done(new Error('remotePath missing'));

  var executable = apiSpec.executables[args.executable_name];
  var invoker = apiSpec.invokers[args.invoker_name];

  if (!executable && !invoker) return done(new Error('either executable_name or invoker_name must be valid'));

  var paramsSchema;

  async.series([
    function(callback) {
      paramsSchema = executable.parameters_schema;

      if (apiSpec.enriched) return callback();

      getInvokerJson({ apiSpec: apiSpec,
                       executableName:args.executable_name,
                       invokerName: args.invoker_name }, function(err, invokerJson) {
        if (err) return callback(err);

        paramsSchema = _.extend(invokerJson.parameters_schema, executable.parameters_schema);

        callback();
      });
    },
    function(callback) {
      if (_.isEmpty(paramsSchema)) return callback();

      async.eachSeries(_.keys(executable.parameters_schema), function(name, callback) {
        var def = executable.parameters_schema[name];
        var val = params[name];

        if (def.mapping !== 'file' || !def.file_path || !val) {
          return callback();
        }

        var absFilePath = path.join(remotePath, def.file_path);

        var writeArgs = { path: absFilePath, content: val };
        if (def.type === 'binary' && _.isString(val)) writeArgs.content = new Buffer(val, 'base64');
        else if (def.type !== 'binary') writeArgs.encoding = def.encoding || def.file_encoding || 'utf8';

        async.series([
          async.apply(access.mkdir, { path: path.dirname(absFilePath) }),
          async.apply(access.writeFile, writeArgs)
        ], callback);
      }, callback);
    }
  ], done);
};

var getMappedParametersSync = function(args) {
//TODO: if apiSpec.enriched = false -> call and consider getInvokerJson
  args = args || {};

  var mappingType = args.mappingType;
  if (!mappingType) throw new Error('mapping type missing');

  var apiSpec = args.apiSpec;
  if (!apiSpec) throw new Error('API spec missing');

  var executable = apiSpec.executables[args.executable_name];
  if (!executable) throw new Error('executable_name missing or invalid');

  if (!executable.parameters_schema) return {};

  var params = args.parameters || {};

  var mapped = {};

  _.each(executable.parameters_schema, function(def, name) {
    if (def && def.mapping && def.mapping === mappingType) {
      mapped[name] = def;

      if (params[name]) mapped[name].value = params[name];
    }
  });

  return mapped;
};

var getMappedResultsSync = function(args) {
//TODO: if apiSpec.enriched = false -> call and consider getInvokerJson
  args = args || {};

  var mappingType = args.mappingType;
  if (!mappingType) throw new Error('mapping type missing');

  var apiSpec = args.apiSpec;
  if (!apiSpec) throw new Error('API spec missing');

  var executable = apiSpec.executables[args.executable_name];
  if (!executable) throw new Error('executable_name missing or invalid');

  if (!executable.results_schema) return {};

  var results = args.results || {};

  var mapped = {};

  _.each(executable.parameters_schema, function(def, name) {
    if (def && def.mapping && def.mapping === mappingType) {
      mapped[name] = def;

      if (results[name]) mapped[name].value = results[name];
    }
  });

  return mapped;
};

var generateExampleSync = function(args) {
  args = args || {};

  var parameters_schema = args.parameters_schema;
  var parameters_required = args.parameters_required;

  //TODO process results and results schema if given

  var example = {
    parameters: {
      invoker_config: {
        some_config_param: 'some_value'
      },
      some_param: 'some_value'
    }
  };

  var limit = args.limit || 6;
  var count = 0;

  _.each(parameters_schema, function(param, name) {
    if (count > limit && !_.contains(parameters_required, name)) return;

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

var resolveTypeSync = function(def) {
  if (_.isEmpty(def.content_type)) delete def.content_type;

  if (!_.contains(validTypes, def.type)) {
    def.type = def.type || '';
    def.content_type = def.content_type || '';
    var normalized = def.type.toLowerCase() + def.content_type.toLowerCase();

    if (S(normalized).contains('image') ||
        S(normalized).contains('img') ||
        S(normalized).contains('video') ||
        S(normalized).contains('audio') ||
        S(normalized).contains('bin') ||
        S(normalized).contains('byte')) {
      def.type = 'binary';
    } else if (S(normalized).contains('string') ||
               S(normalized).contains('text') ||
               S(normalized).contains('txt') ||
               S(normalized).contains('html') ||
               S(normalized).contains('md') ||
               S(normalized).contains('markdown')) {
      def.type = 'string';
    } else if (S(normalized).contains('yaml') ||
               S(normalized).contains('yml')) {
      def.type = 'string'; //TODO: support yaml_object

      if (_.isEmpty(def.content_type)) def.content_type = 'text/yaml; charset=utf-8';
    } else if (S(normalized).contains('json')) {
      def.type = 'json_object';
    } else if (S(normalized).contains('xml')) {
      def.type = 'xml_object';
    }
  }

  if (_.isEmpty(def.content_type)) {
    if (def.type === 'json_object') {
      def.content_type = 'application/json; charset=utf-8';
    } else if (def.type === 'xml_object') {
      def.content_type = 'application/xml; charset=utf-8';
    } else {
      delete def.content_type;
    }
  }

  if (_.isEmpty(def.type)) def.type = 'string';

  return def;
};



module.exports = {
  download: download,
  extract: extract,
  checkoutGit: checkoutGit,
  checkoutBzr: checkoutBzr,

  readInput: readInput,
  writeSpec: writeSpec,
  cloneSpec: cloneSpec,
  enrichSpec: enrichSpec,

  getInvokerJson: getInvokerJson,
  updateInvokers: updateInvokers,
  prepareBuildtime: prepareBuildtime,
  prepareRuntime: prepareRuntime,
  prepareExecutable: prepareExecutable,
  persistEmbeddedExecutable: persistEmbeddedExecutable,
  invokeExecutable: invokeExecutable,
  collectResults: collectResults,
  writeParameters: writeParameters,
  getMappedParametersSync: getMappedParametersSync,
  getMappedResultsSync: getMappedResultsSync,
  generateExampleSync: generateExampleSync,

  validTypes: validTypes,
  embeddedExecutableSchema: require('./executable_schema.json'),
  embeddedExecutableSchemaXml: fs.readFileSync(path.resolve(__dirname, 'executable_schema.xsd'), { encoding: 'utf8' }),
  instanceSchema: require('./instance_schema.json')
};
