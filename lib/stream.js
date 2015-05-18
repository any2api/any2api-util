var path = require('path');
var _ = require('lodash');
var fs = require('fs');
var S = require('string');
var async = require('async');
var through2 = require('through2');
var split = require('split');
//var combine = require('stream-combiner2'); // ,"stream-combiner2": "~1.0.2"
var concat = require('concat-stream');
var tar = require('tar-fs');
var temp = require('temp').track();
var baseUtil = require('./base');



var streamifyParameters = function(args, done) {
  args = args || {};
  args.chunkMaxSize = args.chunkMaxSize || 2000;

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

  var writeChunks = function(name, type, chunks, callback) {
    async.eachSeries(chunks, function(chunk, callback) {
      var data = { name: name, chunk: chunk };

      if (type) data.type = type;

      if (!args.objectMode) {
        if (Buffer.isBuffer(data.chunk)) data.chunk = data.chunk.toString('base64');

        data = JSON.stringify(data) + '\n';
      }

      args.parametersStream.write(data, callback);
    }, function(err) {
      if (err) return callback(err);

      var data = { name: name, complete: true };

      if (!args.objectMode) data = JSON.stringify(data) + '\n';

      args.parametersStream.write(data, callback);
    });
  };

  async.eachSeries(_.keys(args.parameters).concat(_.keys(defaults)), function(name, callback) {
    var value = args.parameters[name] || defaults[name];
    var type = null;

    if (args.parametersSchema[name] && args.parametersSchema[name].type) {
      type = args.parametersSchema[name].type;
    } else if (args.parametersSchema[name] && args.parametersSchema[name].mapping === 'dir') {
      type = 'binary';
    }

    if (_.isPlainObject(value) || _.isArray(value)) {
      var stringifiedValue = JSON.stringify(value);

      if (stringifiedValue.length <= args.chunkMaxSize) {
        var data = { name: name, chunk: value, type: type, complete: true };

        if (!args.objectMode) data = JSON.stringify(data) + '\n';

        return args.parametersStream.write(data, callback);
      } else {
        value = stringifiedValue;
      }
    }

    if (_.isString(value) && value.length > args.chunkMaxSize) {
      var re = new RegExp('.{1,' + args.chunkMaxSize + '}', 'g');

      writeChunks(name, type, value.match(re), callback);
    } else if (Buffer.isBuffer(value) && value.length > args.chunkMaxSize) {
      var chunks = [];

      for (var i = 0 ; i < value.length ; i = i + args.chunkMaxSize) {
        var end = i + args.chunkMaxSize;

        if (end > value.length) end = value.length;

        chunks.push(value.slice(i, end));
      }

      writeChunks(name, type, chunks, callback);
    } else {
      writeChunks(name, type, [ value ], callback);
    }
  }, function(err) {
    args.parametersStream.end();

    if (err) throw err;
  });

  /*args.parametersStream.on('finish', function() {
    done(null, args.parametersStream);
  });*/

  done(null, args.parametersStream);
};

var unstreamifyResults = function(args, done) {
  args = args || {};

  var results = {};
  var concatStreams = {};

  if (!args.resultsSchema) return done(new Error('results schema missing'));

  if (!args.resultsStream) return done(new Error('results stream missing'));

  args.resultsStream
    .pipe(through2.obj(function(data, encoding, callback) {
      if ( !data ||
           !data.name ||
          (!data.chunk && data.complete && !concatStreams[data.name]) ||
          (!data.chunk && !data.complete) ) {
        return callback();
      }

      var name = data.name;
      var def = args.resultsSchema[name] || {};
      var type = def.type || data.type;

      if (!concatStreams[name] && (_.isPlainObject(data.chunk) || _.isArray(data.chunk)) && data.complete) {
        results[name] = data.chunk;

        return callback();
      }

      concatStreams[name] = concatStreams[name] || concat(function(content) {
        if (type === 'binary' && _.isString(content)) {
          results[name] = new Buffer(content, 'base64');
        } else if (type === 'json_object' || type === 'json_array' || type === 'boolean' || type === 'number') {
          results[name] = JSON.parse(content);
        } else {
          results[name] = content;
        }
      });

      if (!_.isString(data.chunk) && !Buffer.isBuffer(data.chunk)) data.chunk = JSON.stringify(data.chunk);

      if (data.chunk && !data.complete) concatStreams[name].write(data.chunk, callback);
      else if (data.complete) concatStreams[name].end(data.chunk, function() {
        delete concatStreams[name];
        callback();
      });
      else callback();
    }, function(callback) {
      async.each(_.values(concatStreams), function(stream, callback) {
        stream.end(callback);
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
  var helperStreams = {};

  var writeChunk = function(data, def, callback) {
    var type = def.type || data.type;

    if ((def.mapping === 'dir' || type === 'binary') && _.isString(data.chunk)) {
      data.chunk = new Buffer(data.chunk, 'base64');
    } else if (_.isPlainObject(data.chunk) || _.isArray(data.chunk)) {
      data.chunk = JSON.stringify(data.chunk);
    }
    //else if (type !== 'binary') writeArgs.encoding = def.encoding || def.file_encoding || 'utf8';

    if (data.chunk && !data.complete) writeStreams[data.name].write(data.chunk, callback);
    else if (data.complete) writeStreams[data.name].end(data.chunk, function() {
      delete writeStreams[data.name];
      callback();
    });
    else callback();
  };

  var processData = function(data, callback) {
    if ( !data ||
         !data.name ||
        (!data.chunk && data.complete && !writeStreams[data.name]) ||
        (!data.chunk && !data.complete) ) {
      return callback(null, data);
    }

    var def = args.parametersSchema[data.name] || {};

    var targetPath = def.file_path || def.dir_path;
    
    if (args.basePath && targetPath && !S(targetPath).startsWith('/')) {
      targetPath = path.join(args.basePath, targetPath); //TODO path.isAbsolute()
    }

    if (def.mapping === 'file' && def.file_path && data.chunk) {
      //filePath = path.normalize(filePath);

      if (!writeStreams[data.name]) {
        args.access.mkdir({ path: path.dirname(targetPath) }, function(err) {
          if (err) return callback(err, data);

          writeStreams[data.name] = args.access.fileWriteStream({ path: targetPath });

          writeChunk(data, def, callback);
        });
      } else {
        writeChunk(data, def, callback);
      }
    } else if (def.mapping === 'dir' && def.dir_path && data.chunk) {
      if (!writeStreams[data.name] && def.dir_pack !== 'tar') {
        helperStreams[data.name + 'tar'] = args.access.tarExtractWriteStream({ path: targetPath }).on('finish', function() {
          delete helperStreams[data.name + 'tar'];
        });

        writeStreams[data.name] = toTarStream({ tarWriteStream: helperStreams[data.name + 'tar'] });
      } else if (!writeStreams[data.name]) {
        writeStreams[data.name] = args.access.tarExtractWriteStream({ path: targetPath });
      }

      writeChunk(data, def, callback);
    } else {
      callback(null, data);
    }
  };

  var throughStream = through2.obj(function(data, encoding, callback) {
    // wait for access and basePath
    if (_.isFunction(args.access) || _.isFunction(args.basePath)) {
      getAccessAndBasePath(data, args, function(err, args) {
        if (!err && !_.isFunction(args.access) && !_.isFunction(args.basePath)) {
          processData(data, callback);
        } else {
          callback(err, data);
        }        
      });
    } else {
      processData(data, callback);
    }
  }, function(callback) {
    var self = this;

    async.parallel([
      function(callback) {
        async.each(_.values(writeStreams), function(stream, callback) {
          stream.end(callback);
        }, callback);
      },
      function(callback) {
        async.each(_.values(helperStreams), function(stream, callback) {
          stream.on('finish', callback);
        }, callback);
      }
    ], function(err) {
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
    var self = this;
    var name = data.name;
    var def = args.parametersSchema[name] || {};
    var type = def.type || data.type;
    var mapping = def.mapping;

    if ( !data ||
         !name ||
        (!data.chunk && data.complete && !concatStreams[data.name]) ||
        (!data.chunk && !data.complete) ||
        (_.isArray(args.name) && !_.contains(args.name, name)) ||
        (_.isString(args.name) && args.name !== name) ||
        (_.isArray(args.mapping) && !_.contains(args.mapping, mapping)) ||
        (_.isString(args.mapping) && args.mapping !== mapping) ) {
      return callback(null, data);
    }

    if (!concatStreams[name] && (_.isPlainObject(data.chunk) || _.isArray(data.chunk)) && data.complete) {
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

      //delete concatStreams[name];

      self.push({
        event: 'gatheredParameter',
        mapping: args.mapping,
        parameter: { name: name, value: value }
      });
    });

    if (!_.isString(data.chunk) && !Buffer.isBuffer(data.chunk)) data.chunk = JSON.stringify(data.chunk);

    if (data.chunk && !data.complete) concatStreams[name].write(data.chunk, callback);
    else if (data.complete) concatStreams[name].end(data.chunk, function() {
      delete concatStreams[name];
      callback();
    });
    else callback();
  }, function(callback) {
    var self = this;

    async.each(_.values(concatStreams), function(stream, callback) {
      stream.end(callback);
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

  var processData = function(data, stream, callback) {
    if (data.event === 'instanceFinished') {
      instanceFinished = true;

      stream.push(data);
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

            stream.push({ name: name, chunk: chunk });

            callback();
          }, function(flushCallback) {
            stream.push({ name: name, complete: true });

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
  }

  var throughStream = through2.obj(function(data, encoding, callback) {
    var self = this;

    // wait for access and basePath
    if (_.isFunction(args.access) || _.isFunction(args.basePath)) {
      getAccessAndBasePath(data, args, function(err, args) {
        if (!err && !_.isFunction(args.access) && !_.isFunction(args.basePath)) {
          processData(data, self, callback);
        } else {
          callback(err, data);
        }        
      });
    } else {
      processData(data, self, callback);
    }
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
    if (data.chunk && Buffer.isBuffer(data.chunk)) {
      data.chunk = data.chunk.toString('base64');

      data._buffer = true;
    }

    fs.write(tempFile.fd, JSON.stringify(data, null, 2) + '\n', callback);
  };

  var restoreBufferStream = through2.obj(function(data, encoding, callback) {
    if (data._buffer && _.isString(data.chunk)) data.chunk = new Buffer(data.chunk, 'base64');

    callback(null, data);
  });

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

          fs.createReadStream(tempFile.path)
            .pipe(split(JSON.parse))
            .pipe(restoreBufferStream)
            .pipe(throughStream);

          callback();
        });
      }
    });
  }, function(callback) {
    var self = this;

    if (!state.finished && tempFile) {
      fs.close(tempFile.fd, function(err) {
        if (err) return callback(err);

        fs.createReadStream(tempFile.path)
          .pipe(split(JSON.parse))
          .pipe(restoreBufferStream)
          .pipe(through2.obj(function(data, encoding, callback) {
            self.push(data); //TODO respect 'false' return value, do not push too much data

            callback();
          }, callback));

        //callback();
      });
    } else {
      callback();
    }
  });

  return throughStream;
};

var toTarStream = function(args) {
  args = args || {};

  if (!args.tarWriteStream) throw new Error('tar write stream missing');

  var concatStream = concat(function(buf) {
    temp.mkdir('unzipped', function(err, tempDir) {
      if (err) throw err;
       
      baseUtil.extract({ dir: tempDir, file: buf }, function(err) {
        if (err) throw err;

        tar.pack(tempDir).pipe(args.tarWriteStream);
      });
    });
  });

  return concatStream;
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
