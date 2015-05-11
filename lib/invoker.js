var _ = require('lodash');
var through2 = require('through2');
var split = require('split');
var path = require('path');
var async = require('async');
var shortId = require('shortid');
var streamUtil = require('./stream');



var readInput = function(input, callback) {
  input = input || {};

  callback = _.once(callback);

  if (!input.invoker && !process.env.APISPEC_INVOKER) return callback(new Error('invoker must be specified'));

  if (!input.invoker) {
    try {
      input.invoker = JSON.parse(process.env.APISPEC_INVOKER);
    } catch (err) {
      return callback(err);
    }
  }

  if (!input.executable && process.env.APISPEC_EXECUTABLE) {
    try {
      input.executable = JSON.parse(process.env.APISPEC_EXECUTABLE);
    } catch (err) {
      return callback(err);
    }
  }

  if (!input.specEnriched && process.env.APISPEC_ENRICHED) {
    try {
      input.specEnriched = JSON.parse(process.env.APISPEC_ENRICHED);
    } catch (err) {
      return callback(err);
    }
  }

  input.invokerName = input.invokerName || process.env.APISPEC_INVOKER_NAME;
  input.executableName = input.executableName || process.env.APISPEC_EXECUTABLE_NAME;

  input.specPath = input.specPath || input.apispec_path || process.env.APISPEC_PATH;
  input.instanceId = input.instanceId || process.env.INSTANCE_ID;
  input.storeResults = input.storeResults || process.env.INSTANCE_STORE_RESULTS || 'all';

  if (!input.parametersStream) {
    input.parametersStream = process.stdin
      //.on('error', handleError)
      .pipe(split(function(chunk) {
        if (_.isEmpty(chunk)) return;
        else return JSON.parse(chunk);
      }));
      //.on('error', handleError)
  }

  callback(null, input);
};

var writeOutput = function(args, callback) {
  args = args || {};

  if (_.isEmpty(args.resultsStream)) return callback(new Error('results stream missing'));

  args.outputStream = args.outputStream || process.stdout;

  if (!_.isBoolean(args.stringify)) args.stringify = true;

  var transformStream = through2.obj(function(data, encoding, callback) {
    if (args.stringify) {
      if (data.chunk && Buffer.isBuffer(data.chunk)) data.chunk = data.chunk.toString('base64');

      callback(null, JSON.stringify(data) + '\n');      
    } else {
      callback(null, data);
    }
  });

  args.outputStream.on('finish', callback);

  args.resultsStream.pipe(transformStream).pipe(args.outputStream);
};

var prepareInvoke = function(args, done) {
  args = args || {};

  if (!args.accessModule) return done(new Error('access module missing'));

  if (!args.invoker) return done(new Error('invoker missing'));

  //args.executable = args.executable || null;

  if (!args.specPath) return done(new Error('spec path missing'));

  if (!args.parametersStream) return done(new Error('parameters stream missing'));

  args.parametersSchema = args.invoker.parameters_schema;
  args.resultsSchema = args.invoker.results_schema;
  args.executablePath = null;

  if (args.executable) {
    args.parametersSchema = args.executable.parameters_schema;
    args.resultsSchema = args.executable.results_schema;
    args.executablePath = path.resolve(args.specPath, '..', args.executable.path);
  }

  args.stdoutName = 'stdout';
  args.stderrName = 'stderr';

  _.each(args.resultsSchema, function(def, name) {
    if (def.mapping === 'stdout') args.stdoutName = name;
    else if (def.mapping === 'stderr') args.stderrName = name;
  });

  args.instanceId = args.instanceId || uuid.v4();
  args.instancePath = null;

  args.invokerConfig = {};
  args.access = null;

  args.contextPrepared = true;

  done(null, args);
};

var invoke = function(args, done) {
  var ctx = args || {};

  // debug
  ctx.debugStarted = new Date().getTime();

  async.series([
    function(callback) {
      if (!ctx.contextPrepared) prepareInvoke(ctx, callback);
      else callback();
    },
    function(callback) {
      var getAccess = function(data, callback) {
        if (ctx.access) return callback(null, ctx.access);
        else if (data.event !== 'gatheredParameter' || !data.parameter.name === 'invoker_config') return callback();

        ctx.invokerConfig = data.parameter.value || {};
        ctx.invokerConfig.access = ctx.invokerConfig.access || 'local';

        if (ctx.accessModule[ctx.invokerConfig.access]) {
          ctx.access = ctx.accessModule[ctx.invokerConfig.access](ctx.invokerConfig);

          callback(null, ctx.access);
        } else {
          callback(new Error('access \'' + ctx.invokerConfig.access + '\' not supported'));
        }
      };

      var getBasePath = function(data, callback) {
        if (ctx.instancePath) return callback(null, ctx.instancePath);
        else if (data.event !== 'gatheredParameter' || !data.parameter.name === 'invoker_config') return callback();

        ctx.invokerConfig = data.parameter.value || {};
        ctx.invokerConfig.access = ctx.invokerConfig.access || 'local';
        ctx.instancePath = ctx.invokerConfig.instance_path || path.join('/', 'tmp', shortId.generate());

        callback(null, ctx.instancePath);
      };



      ctx.resultsStream = streamUtil.readResultFilesStream({
        resultsSchema: ctx.resultsSchema,
        basePath: getBasePath,
        access: getAccess
      });

      var lastStream = ctx.parametersStream
        .pipe(streamUtil.gatherParametersStream({
          parametersSchema: ctx.parametersSchema,
          name: 'invoker_config'
        }))
        .pipe(streamUtil.rearrangeStream(function(data, state, callback) {
          if (data.event === 'gatheredParameter' && data.parameter.name === 'invoker_config') {
            callback(null, { prioritized: true, finished: true });
          } else {
            callback(null);
          }
        }))
        .pipe(streamUtil.writeParameterFilesStream({
          parametersSchema: ctx.parametersSchema,
          basePath: getBasePath,
          access: getAccess
        }));

      _.each(ctx.pipeline, function(stream) {
        lastStream.pipe(stream);

        lastStream = stream;
      });

      lastStream
        .pipe(through2.obj(function(data, encoding, callback) {
          // filter
          if (data.name || data.event === 'instanceFinished') {
            callback(null, data);

            // debug
            ctx.debugFinished = new Date().getTime();

            if (ctx.invokerConfig.debug && ctx.access) {
              ctx.access.writeFile({
                path: path.join(ctx.invokerConfig.cwd || ctx.instancePath, 'debug_id-' + ctx.instanceId + '_time-' + ctx.debugFinished + '.json'),
                content: JSON.stringify({
                  instanceId: ctx.instanceId,
                  started: ctx.debugStarted,
                  finished: ctx.debugFinished,
                  duration: ctx.debugFinished - ctx.debugStarted
                }, null, 2)
              });
            }
          } else {
            callback();
          }
        }))
        .pipe(ctx.resultsStream);

      callback();
    }
  ], function(err) {
    done(err, ctx.resultsStream);
  });
};

var createInvoker = function(args) {
  args = args || {};

  var invokeFunction = args.invokeFunction || args.invoke;
  if (!invokeFunction) throw new Error('invoke function missing');

  var accessModule = args.accessModule || args.access;
  if (!accessModule) throw new Error('access module missing');

  var gatherParameters = args.gatherParameters;

  return function(spec) {
    var obj = {};

    obj.invoke = function(args, done) {
      args = args || {};

      args.accessModule = accessModule;

      prepareInvoke(args, function(err, ctx) {
        if (err) return done(err);

        ctx.mappedParameters = {};
        ctx.unmappedParameters = {};

        ctx.accessExecCallback = function(callback) {
          return function(err, stdout, stderr) {
            if (stdout) {
              ctx.resultsStream.write({ name: ctx.stdoutName, chunk: stdout/*, complete: true*/ });
            }

            if (stderr) {
              ctx.resultsStream.write({ name: ctx.stderrName, chunk: stderr/*, complete: true*/ });
            }

            if (err) {
              err.stderr = stderr;
              err.stdout = stdout;
            }

            callback(err);
          };
        };

        ctx.invokeStream = streamUtil.throughStream({ objectMode: true }, function(data, encoding, callback) {
          if (data.event === 'writeParameterFilesFinished') {
            ctx.instancePrepared = true;
          } else if (data.event === 'gatheredParameter' && data.mapping) {
            ctx.mappedParameters[data.mapping] = ctx.mappedParameters[data.mapping] || {};
            ctx.mappedParameters[data.mapping][data.parameter.name] = data.parameter.value;
          } else if (data.event === 'gatheredParameter') {
            ctx.unmappedParameters[data.parameter.name] = data.parameter.value;
          }

          if (ctx.instancePrepared && !ctx.instanceRunning && !ctx.instanceFinished) {
            ctx.instanceRunning = true;

            ctx.invokerConfig.cwd = ctx.invokerConfig.cwd || ctx.instancePath;

            ctx.invokerConfig.env = ctx.invokerConfig.env || {};
            if (ctx.mappedParameters.env) ctx.invokerConfig.env = _.extend(ctx.mappedParameters.env, ctx.invokerConfig.env);
            ctx.invokerConfig.env.INSTANCE_PATH = ctx.instancePath;

            ctx.invokerConfig.args = ctx.invokerConfig.args || {};
            if (ctx.mappedParameters.arg) ctx.invokerConfig.args = _.extend(ctx.mappedParameters.arg, ctx.invokerConfig.args);

            if (ctx.mappedParameters.stdin && !ctx.invokerConfig.stdin) {
              ctx.invokerConfig.stdin = _.values(ctx.mappedParameters.stdin)[0];
            }

            invokeFunction(ctx, function(err) {
              ctx.instanceRunning = false;
              ctx.instanceFinished = true;

              ctx.access.terminate(function(err2) {
                if (err2) console.error(err2);

                callback(err, { event: 'instanceFinished', error: err });
              });
            });
          } else {
            callback(null, data);
          }
        });

        ctx.pipeline = [];

        _.each(gatherParameters, function(gp) {
          gp.parametersSchema = ctx.parametersSchema;

          ctx.pipeline.push(streamUtil.gatherParametersStream(gp));
        });

        ctx.pipeline.push(streamUtil.gatherParametersStream({
          parametersSchema: ctx.parametersSchema,
          mapping: 'env'
        }));
        ctx.pipeline.push(streamUtil.gatherParametersStream({
          parametersSchema: ctx.parametersSchema,
          mapping: 'arg'
        }));
        ctx.pipeline.push(streamUtil.gatherParametersStream({
          parametersSchema: ctx.parametersSchema,
          mapping: 'stdin'
        }));
        ctx.pipeline.push(ctx.invokeStream);

        invoke(ctx, done);
      });
    };

    return obj;
  };
};



module.exports = {
  readInput: readInput,
  writeOutput: writeOutput,
  prepareInvoke: prepareInvoke,
  invoke: invoke,
  createInvoker: createInvoker
};
