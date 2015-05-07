var path = require('path');
var _ = require('lodash');
var fs = require('fs');
var S = require('string');
var async = require('async');
var shortId = require('shortid');



var specFile = 'apispec.json';

var validTypes = [ 'boolean', 'number', 'string', 'binary', 'json_object', 'json_array', 'xml_object' ];



var readSpec = function(args, callback) {
  args = args || {};
  args.apiSpec = args.apiSpec || args.spec;
  args.specPath = args.specPath || args.apispec_path || args.path;

  if (!_.isEmpty(args.apiSpec)) return callback(null, apiSpec);

  if (_.isEmpty(args.specPath)) return callback(new Error('API spec path must be specified'));

  var apiSpec;

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
  } catch (err) {
    return callback(err);
  }

  apiSpec.executables = apiSpec.executables || {};
  apiSpec.invokers = apiSpec.invokers || {};
  apiSpec.implementation = apiSpec.implementation || {};

  callback(null, apiSpec);
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

        getInvokerJson({ invoker: invoker, basePath: basePath }, function(err, invokerJson) {
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

        var paramsSchema = _.cloneDeep(invoker.parameters_schema);
        executable.parameters_schema = _.extend(paramsSchema, executable.parameters_schema);

        var paramsRequired = invoker.parameters_required || [];
        executable.parameters_required = _.uniq(paramsRequired.concat(executable.parameters_required || []));

        var resultsSchema = _.cloneDeep(invoker.results_schema);
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

  var invoker = args.invoker;
  if (!invoker) return done(new Error('invoker missing'));

  var basePath = args.basePath || '.'; // || path.dirname(apiSpec.apispec_path);

  var invokerPath = invoker.path;
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

var resolveTypeSync = function(def) {
  if (_.isEmpty(def.content_type)) delete def.content_type;

  if (!_.contains(validTypes, def.type)) {
    def.type = def.type || '';
    def.content_type = def.content_type || '';
    var normalized = S(def.type.toLowerCase() + def.content_type.toLowerCase());

    if (normalized.contains('image') ||
        normalized.contains('img') ||
        normalized.contains('video') ||
        normalized.contains('audio') ||
        normalized.contains('bin') ||
        normalized.contains('byte')) {
      def.type = 'binary';
    } else if (def.mapping && S(def.mapping.toLowerCase()).contains('dir')) {
      def.type = 'binary';
    } else if (normalized.contains('string') ||
               normalized.contains('text') ||
               normalized.contains('txt') ||
               normalized.contains('html') ||
               normalized.contains('md') ||
               normalized.contains('markdown')) {
      def.type = 'string';
    } else if (normalized.contains('yaml') ||
               normalized.contains('yml')) {
      def.type = 'string'; //TODO: support yaml_object

      if (_.isEmpty(def.content_type)) def.content_type = 'text/yaml; charset=utf-8';
    } else if (normalized.contains('json')) {
      def.type = 'json_object';
    } else if (normalized.contains('xml')) {
      def.type = 'xml_object';
    }
  }

  if (_.isEmpty(def.content_type)) {
    if (def.type === 'json_object' || def.type === 'json_array') {
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
  validTypes: validTypes,
  readSpec: readSpec,
  writeSpec: writeSpec,
  cloneSpec: cloneSpec,
  enrichSpec: enrichSpec,
  resolveTypeSync: resolveTypeSync
};
