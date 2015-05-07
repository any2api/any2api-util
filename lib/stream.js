var path = require('path');
var _ = require('lodash');
var fs = require('fs');
var S = require('string');
var async = require('async');
var through2 = require('through2');
var split = require('split');
//var combine = require('stream-combiner2'); // ,"stream-combiner2": "~1.0.2"
var concat = require('concat-stream');
var tar = require('tar-stream');
var unzip = require('unzip');
var temp = require('temp').track();



var streamifyParameters = function(args, done) {
  args = args || {};

  if (!args.parameters) return done(new Error('parameters missing'));

  if (!args.parametersSchema) return done(new Error('parameters schema missing'));

  args.parametersRequired = args.parametersRequired || [];

  if (!_.isBoolean(args.objectMode)) args.objectMode = false;

  args.parametersStream = through2({ objectMode: args.objectMode });

  var defaults = {};

  _.each(args.parametersSchema, function(def, name) {
    if (!args.parameters[name] && _.contains(args.parametersRequired, name) && def.default) {
      defaults[name] = def.default;
    }
  });

  _.each(_.keys(args.parameters).concat(_.keys(defaults)), function(name) {
    var p = { name: name, chunk: args.parameters[name] || defaults[name], complete: true };

    if (args.parametersSchema[name] && args.parametersSchema[name].type) {
      p.type = args.parametersSchema[name].type;
    }

    if (!args.objectMode) {
      if (Buffer.isBuffer(p.chunk)) p.chunk = p.chunk.toString('base64');

      p = JSON.stringify(p) + '\n';
    }

    args.parametersStream.write(p);
  });

  args.parametersStream.on('finish', function() {
    done(null, args.parametersStream);
  });

  args.parametersStream.end();
};

var unstreamifyResults = function(args, done) {
  args = args || {};

  var results = {};
  var concatStreams = {};

  if (!args.resultsSchema) return done(new Error('results schema missing'));

  if (!args.resultsStream) return done(new Error('results stream missing'));

  args.resultsStream
    .pipe(through2.obj(function(data, encoding, callback) {
      if (!data || !data.name) return callback();

      var def = args.resultsSchema[data.name] || {};
      var type = def.type || data.type;

      if (!concatStreams[data.name] && _.isObject(data.chunk) && data.complete) {
        results[data.name] = data.chunk;

        return callback();
      }

      concatStreams[data.name] = concatStreams[data.name] || concat(function(content) {
        if (type === 'binary' && _.isString(content)) {
          results[data.name] = new Buffer(content, 'base64');
        } else if (type === 'json_object' || type === 'json_array' || type === 'boolean' || type === 'number') {
          results[data.name] = JSON.parse(content);
        } else {
          results[data.name] = content;
        }
      });

      if (data.chunk) concatStreams[data.name].write(data.chunk);

      if (data.complete) {
        concatStreams[data.name].end();

        delete concatStreams[data.name];
      }

      callback();
    }, function(callback) {
      async.each(_.values(concatStreams), function(stream, callback) {
        stream.on('finish', callback);

        stream.end();
      }, function(err) {
        callback(err);

        done(err, results);
      });
    }));
};

var getAccessAndBasePath = function(data, args, callback) {
  async.parallel([
    function(callback) {
      if (!_.isFunction(args.access)) return callback();

      args.access(data, function(err, a) {
        if (err) return callback(err);

        args.access = a || args.access;

        callback();
      });
    },
    function(callback) {
      if (!_.isFunction(args.basePath)) return callback();

      args.basePath(data, function(err, b) {
        if (err) return callback(err);

        args.basePath = b || args.basePath;

        callback();
      });
    }
  ], function(err) {
    callback(err, args);
  });
};

var writeParameterFilesStream = function(args) {
  args = args || {};

  if (!args.access) throw new Error('access missing');

  if (!args.parametersSchema) throw new Error('parameters schema missing');

  var writeStreams = {};

  var throughStream = through2.obj(function(data, encoding, callback) {
    // wait for access and basePath
    if (_.isFunction(args.access) || _.isFunction(args.basePath)) {
      getAccessAndBasePath(data, args, function(err, args) {
        callback(err, data);
      });

      return;
    }

    if (!data.name || !data.chunk) return callback(null, data);

    var def = args.parametersSchema[data.name] || {};
    var type = def.type || data.type;

    var targetPath = def.file_path || def.dir_path;
    
    if (args.basePath && targetPath && !S(targetPath).startsWith('/')) {
      targetPath = path.join(args.basePath, targetPath); //TODO path.isAbsolute()
    }

    var writeChunk = function(callback) {
      if ((def.mapping === 'dir' || type === 'binary') && _.isString(data.chunk)) {
        data.chunk = new Buffer(data.chunk, 'base64');
      } else if (_.isObject(data.chunk)) {
        data.chunk = JSON.stringify(data.chunk);
      }
      //else if (type !== 'binary') writeArgs.encoding = def.encoding || def.file_encoding || 'utf8';

      writeStreams[data.name].write(data.chunk);

      if (data.complete) {
        writeStreams[data.name].on('finish', function() {
          delete writeStreams[data.name];
        });

        writeStreams[data.name].end();
      }

      callback();
    };

    if (def.mapping === 'file' && def.file_path && data.chunk) {
      //filePath = path.normalize(filePath);

      if (!writeStreams[data.name]) {
        args.access.mkdir({ path: path.dirname(targetPath) }, function(err) {
          if (err) return callback(err, data);

          writeStreams[data.name] = args.access.fileWriteStream({ path: targetPath });

          writeChunk(callback);
        });
      } else {
        writeChunk(callback);
      }
    } else if (def.mapping === 'dir' && def.dir_path && data.chunk) {
      if (!writeStreams[data.name] && def.dir_pack === 'zip') {
        writeStreams[data.name] = zipToTarStream({ tarExtractStream: args.access.tarExtractWriteStream({ path: targetPath }) });
      } else if (!writeStreams[data.name]) {
        writeStreams[data.name] = args.access.tarExtractWriteStream({ path: targetPath });
      }

      writeChunk(callback);
    } else {
      callback(null, data);
    }
  }, function(callback) {
    var self = this;

    async.each(_.values(writeStreams), function(stream, callback) {
      stream.on('finish', callback);

      stream.end();
    }, function(err) {
      self.push({ event: 'writeParameterFilesFinished' });

      callback(err);
    });
  });

  return throughStream;
};

var gatherParametersStream = function(args) {
  args = args || {};

  if (!args.parametersSchema) throw new Error('parameters schema missing');

  args.mapping = args.mapping || args.mappings || args.mappingKind;
  args.name = args.name || args.names;

  var gatheredParams = [];

  var concatStreams = {};

  var throughStream = through2.obj(function(data, encoding, callback) {
    var name = data.name;
    var def = args.parametersSchema[name] || {};
    var type = def.type || data.type;
    var mapping = def.mapping;

    if ( (!data || !name) ||
         (_.isArray(args.name) && !_.contains(args.name, name)) ||
         (_.isString(args.name) && args.name !== name) ||
         (_.isArray(args.mapping) && !_.contains(args.mapping, mapping)) ||
         (_.isString(args.mapping) && args.mapping !== mapping) ) {
      return callback(null, data);
    }

    if (!concatStreams[name] && _.isObject(data.chunk) && data.complete) {
      //gatheredParams[name] = data.chunk;

      gatheredParams.push(name);

      return callback(null, {
        event: 'gatheredParameter',
        mapping: args.mapping,
        parameter: { name: name, value: data.chunk }
      });
    }

    concatStreams[name] = concatStreams[name] || concat(function(value) {
      if (type === 'binary' && _.isString(value)) {
        value = new Buffer(value, 'base64');
      } else if (type === 'json_object' || type === 'json_array' || type === 'boolean' || type === 'number') {
        value = JSON.parse(value);
      }

      gatheredParams.push(name);

      callback(null, {
        event: 'gatheredParameter',
        mapping: args.mapping,
        parameter: { name: name, value: value }
      });
    });

    if (data.chunk) concatStreams[name].write(data.chunk);

    if (data.complete) {
      concatStreams[name].on('finish', function() {
        delete concatStreams[name];
      });

      concatStreams[name].end();
    } else {
      callback();
    }
  }, function(callback) {
    var self = this;

    async.each(_.values(concatStreams), function(stream, callback) {
      stream.on('finish', callback);

      stream.end();
    }, function(err) {
      self.push({
        event: 'gatherParametersFinished',
        mapping: args.mapping,
        parameters: gatheredParams //_.keys(concatStreams)
      });

      callback(err);
    });
  });

  return throughStream;
};

var readResultFilesStream = function(args) {
  args = args || {};

  if (!args.access) throw new Error('access missing');

  if (!args.resultsSchema) throw new Error('results schema missing');

  var readStreams = {};

  var instanceFinished = false;

  var resultFilesRead = false;

  var throughStream = through2.obj(function(data, encoding, callback) {
    var self = this;

    // wait for access and basePath
    if (_.isFunction(args.access) || _.isFunction(args.basePath)) {
      getAccessAndBasePath(data, args, function(err, args) {
        callback(err, data);
      });

      return;
    }

    if (data.event === 'instanceFinished') {
      instanceFinished = true;

      self.push(data);
    } else if (!instanceFinished || resultFilesRead) {
      return callback(null, data);
    }

    async.eachSeries(_.keys(args.resultsSchema), function(name, callback) {
      var def = args.resultsSchema[name] || {};

      if ((def.mapping === 'file' && def.file_path) || (def.mapping === 'dir' && def.dir_path)) {
        var targetPath = def.file_path || def.dir_path;
        //targetPath = path.normalize(targetPath);

        if (args.basePath && targetPath && !S(targetPath).startsWith('/')) {
          targetPath = path.join(args.basePath, targetPath); //TODO path.isAbsolute()
        }

        args.access.exists({ path: targetPath }, function(err, exists) {
          if (err || !exists) return callback(err);

          var createStream = args.access.fileReadStream;
          if (def.mapping === 'dir') createStream = args.access.tarPackReadStream;

          //TODO support zip files: transparently convert tar stream to zip stream

          readStreams[name] = createStream({ path: targetPath });

          readStreams[name].pipe(through2(function(chunk, encoding, callback) {
            if (Buffer.isBuffer(chunk) && def.type !== 'binary') chunk = chunk.toString('utf8');

            self.push({ name: name, chunk: chunk });

            callback();
          }, function(flushCallback) {
            self.push({ name: name, complete: true });

            delete readStreams[name];

            flushCallback();
            callback();
          }));
        });
      } else {
        callback();
      }
    }, function(err) {
      resultFilesRead = true;

      callback(err);
    });
  }, function(callback) {
    var self = this;

    async.each(_.values(readStreams), function(stream, callback) {
      stream.on('end', callback);

      //stream.resume();
    }, function(err) {
      self.push({ event: 'readResultFilesFinished' });

      callback(err);
    });
  });

  return throughStream;
};

var rearrangeStream = function(prioritize) {
  if (!_.isFunction(prioritize)) throw new Error('prioritize must be a function');

  var tempFile = null;

  var state = {};

  var write = function(data, callback) {
    fs.write(tempFile.fd, JSON.stringify(data, null, 2) + '\n');

    callback();
  };

  var throughStream = through2.obj(function(data, encoding, callback) {
    if (state.finished) return callback(null, data);

    prioritize(data, state, function(err, st) {
      if (err) return callback(err);

      state = st || {};

      if (state.prioritized) {
        callback(null, data);
      } else if (!state.finished && tempFile) {
        write(data, callback);
      } else if (!state.finished && !tempFile) {
        temp.open('temp-stream', function(err, info) {
          if (err) return callback(err);

          tempFile = info;

          write(data, callback);
        });
      } else if (state.finished && tempFile) {
        fs.close(tempFile.fd, function(err) {
          if (err) return callback(err);

          fs.createReadStream(tempFile.path).pipe(split(JSON.parse)).pipe(throughStream);
        });
      }
    });
  });

  return throughStream;
};

var zipToTarStream = function(args) {
  args = args || {};

  if (!args.tarExtractStream) throw new Error('tar extract stream missing');

  var tarPackStream = tar.pack();

  var zipExtractStream = unzip.Parse();

  zipExtractStream.pipe(tarPackStream).pipe(tarExtractStream);

  zipExtractStream.on('entry', function(zipEntry) {
    //zipEntry.type; // 'Directory' or 'File'

    var tarEntry = tarPackStream.tarEntry({ name: zipEntry.path, size: zipEntry.size }, function(err) {
      //tarPackStream.finalize();
    });

    zipEntry.pipe(tarEntry);
  }).on('finish', tarPackStream.finalize);

  return zipExtractStream;
};

var singleParameterStream = function(args) {
  //TODO single param as stream, not gathered, e.g., stdin
};

var singleResultStream = function(args) {
  //TODO single result as stream, not gathered, e.g., stdout
};



module.exports = {
  streamifyParameters: streamifyParameters,
  unstreamifyResults: unstreamifyResults,
  throughStream: through2,
  writeParameterFilesStream: writeParameterFilesStream,
  gatherParametersStream: gatherParametersStream,
  readResultFilesStream: readResultFilesStream,
  rearrangeStream: rearrangeStream
};
