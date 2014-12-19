var debug = require('debug')(require('./package.json').name);
var path = require('path');
var _ = require('lodash');
var fs = require('fs-extra');
var Decompress = null;
var Download = null;
var async = require('async');
var shortId = require('shortid');
var temp = null;
var request = null;
var childProc = require('child_process');



var specFile = 'apispec.json';



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
  
  if (!args.apiSpec) {
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

var cloneSpec = function(args, callback) {
  args = args || {};

  if (!args.apiSpec) {
    return callback(new Error('API spec missing'));
  }

  var apiSpec = args.apiSpec;

  var clonedSpec = _.cloneDeep(apiSpec);

  clonedSpec.apispec_path = path.join(path.dirname(apiSpec.apispec_path), 'tmp-apispec-' + shortId.generate() + '.json');

  writeSpec({ apiSpec: clonedSpec }, function(err, writtenSpec) {
    if (err) return callback(err);

    callback(null, writtenSpec);
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

  childProc.exec('npm run ' + args.command,
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

  var apiSpec = args.apiSpec;
  if (!apiSpec) return done(new Error('API spec missing'));

  var preparedInvokers = args.preparedInvokers || {};

  var run = args.run || {};
  var apiSpecCopy;
  var executable = null;

  var invokerPath;
  var invokerJson;

  var runParams;
  var enrichedParams;

  async.series([
    function(callback) {
      cloneSpec({ apiSpec: apiSpec }, function(err, as) {
        apiSpecCopy = as;

        callback(err);
      });
    },
    function(callback) {
      // Executable
      if (args.executable_name) {
        executable = apiSpecCopy.executables[args.executable_name];

        invokerPath = apiSpecCopy.invokers[executable.invoker_name].path;
      } else if (args.invoker_name) {
        invokerPath = apiSpecCopy.invokers[args.invoker_name].path;

        if (run.executable) {
          executable = run.executable;

          executable.name = executable.name || 'embedded-' + shortId.generate();

          executable.invoker_name = args.invoker_name;

          apiSpecCopy.executables[executable.name] = executable;
        }
      }

      if (!invokerPath) return callback(new Error('invoker path cannot be determined'));
      else invokerPath = path.resolve(apiSpecCopy.apispec_path, '..', invokerPath);

      if (executable) executable.name = args.executable_name || executable.name;

      // Read invoker.json
      invokerJson = JSON.parse(fs.readFileSync(path.join(invokerPath, 'invoker.json')));

      // Process parameters
      var paramsRequired = invokerJson.parameters_required || [];
      var paramsSchema = invokerJson.parameters_schema;

      if (_.isEmpty(run.parameters)) run.parameters = {};

      runParams = { run_id: run._id || run.id || 'run-' + shortId.generate(),
                    run_path: args.run_path || temp.path({ prefix: 'tmp-run-' }) };
      enrichedParams = _.clone(run.parameters);
      enrichedParams._ = runParams;

      if (executable) {
        runParams.executable_name = executable.name;
        paramsRequired = _.uniq(paramsRequired.concat(executable.parameters_required || []));
        paramsSchema = _.extend(paramsSchema, executable.parameters_schema)
      }

      _.each(paramsSchema, function(p, name) {
        if (_.contains(paramsRequired, name) && !enrichedParams[name] && p.default) {
          enrichedParams[name] = p.default;
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
          PARAMETERS: JSON.stringify(enrichedParams),
          PATH: process.env.PATH
        }
      };

      childProc.exec('npm start', options, function(err, stdout, stderr) {
        debug('run complete');

        run.results = run.results || {};

        run.results.stdout = stdout;
        run.results.stderr = stderr;

        callback(err);
      });
    },
    function(callback) {
      var results_schema = invokerJson.results_schema || {};

      if (executable) _.extend(results_schema, executable.results_schema);

      var filesDir = runParams.run_path;// || invokerPath;

      async.eachSeries(_.keys(results_schema), function(name, callback) {
        var r = results_schema[name];

        if (r.mapping === 'stdout') {
          run.results[name] = run.results.stdout;

          delete run.results.stdout;
        } else if (r.mapping === 'stderr') {
          run.results[name] = run.results.stderr;

          delete run.results.stderr;
        } else if (r.mapping === 'file' && r.file_path) {
          var filePath = path.resolve(filesDir, r.file_path);

          if (!fs.existsSync(filePath)) {
            return callback(new Error('results file missing: ' + filePath));
          }

          run.results[name] = fs.readFileSync(filePath, 'utf8');
        }

        if (r.type === 'object') {
          run.results[name] = JSON.parse(run.results[name]);
        }

        callback();
      }, callback);
    }
  ], function(err) {
    if (err) {
      debug('error', err);

      run.status = 'error';
      run.failed = new Date().toString();

      run.error = err.message;
    } else {
      run.status = 'finished';
      run.finished = new Date().toString();
    }

    async.parallel([
      function(callback) {
        fs.remove(runParams.run_path, callback);
      },
      function(callback) {
        fs.remove(apiSpecCopy.apispec_path, callback);
      }
    ], function(err2) {
      if (err2) console.error(err2);

      done(err, run);
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
  if (!executable) return done(new Error('executable_name missing or invalid'));

  if (!executable.results_schema) return done();

  async.eachSeries(_.keys(executable.results_schema), function(name, callback) {
    var r = executable.results_schema[name];

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

        async.series([
          async.apply(fs.mkdirs, path.dirname(local)),
          function(callback) {
            access.readFile({ path: remote, options: { encoding: 'utf8' } }, function(err, c) {
              content = c;

              callback(err);
            });
          },
          function(callback) {
            fs.writeFile(local, content, { encoding: 'utf8' }, callback);
          }
        ], callback);
      });
    } else {
      callback();
    }
  }, function(err) {
    done();
  });
};

var writeParameters = function(args, done) {
  args = args || {};

  var schema = args.parametersSchema;
  if (!schema) return done(new Error('parametersSchema missing'));

  var params = args.parameters;
  if (!params) return done(new Error('parameters missing'));

  var access = args.access;
  if (!access) return done(new Error('access missing'));

  var remotePath = args.remotePath;
  if (!remotePath) return done(new Error('remotePath missing'));

  async.eachSeries(_.keys(schema), function(name, callback) {
    var def = schema[name];
    var val = params[name];

    if (def.mapping !== 'file' || !def.file_path || !val) {
      return callback();
    }

    var absFilePath = path.join(remotePath, def.file_path);

    async.series([
      async.apply(access.mkdir, { path: path.dirname(absFilePath) }),
      async.apply(access.writeFile, { path: absFilePath, content: val })
    ], callback);
  }, done);
};

var generateExampleSync = function(args) {
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

var embeddedExecutableSchema = {
  type: 'object',
  oneOf: [
    {
      properties: {
        files: {
          type: 'array',
          items: {
            type: 'object',
            oneOf: [
              {
                properties: {
                  path: { type: 'string' },
                  text: { type: 'string' }
                }
              },
              {
                properties: {
                  path: { type: 'string' },
                  object: { type: 'object' }
                }
              },
              {
                properties: {
                  path: { type: 'string' },
                  base64: { type: 'string' }
                }
              },
              {
                properties: {
                  path: { type: 'string' },
                  url: { type: 'string' }
                }
              }
            ]
          }
        } 
      }
    },
    {
      properties: {
        tarball_url: { type: 'string' }
      }
    }
  ]
};



module.exports = {
  download: download,
  extract: extract,
  checkoutGit: checkoutGit,
  checkoutBzr: checkoutBzr,
  readInput: readInput,
  writeSpec: writeSpec,
  cloneSpec: cloneSpec,
  updateInvokers: updateInvokers,
  prepareBuildtime: prepareBuildtime,
  prepareRuntime: prepareRuntime,
  prepareExecutable: prepareExecutable,
  persistEmbeddedExecutable: persistEmbeddedExecutable,
  invokeExecutable: invokeExecutable,
  collectResults: collectResults,
  generateExampleSync: generateExampleSync,
  embeddedExecutableSchema: embeddedExecutableSchema
};
