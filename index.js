var baseUtil = require('./lib/base');
var specUtil = require('./lib/spec');
var streamUtil = require('./lib/stream');
var invokerUtil = require('./lib/invoker');



module.exports = {
  download: baseUtil.download,
  extract: baseUtil.extract,
  checkoutGit: baseUtil.checkoutGit,
  checkoutBzr: baseUtil.checkoutBzr,

  readInput: invokerUtil.readInput,
  writeOutput: invokerUtil.writeOutput,
  prepareInvoke: invokerUtil.prepareInvoke,
  invoke: invokerUtil.invoke,
  createInvoker: invokerUtil.createInvoker,

  validTypes: specUtil.validTypes,
  readSpec: specUtil.readSpec,
  writeSpec: specUtil.writeSpec,
  cloneSpec: specUtil.cloneSpec,
  enrichSpec: specUtil.enrichSpec,

  updateInvokers: baseUtil.updateInvokers,
  prepareBuildtime: baseUtil.prepareBuildtime,
  prepareRuntime: baseUtil.prepareRuntime,
  prepareExecutable: baseUtil.prepareExecutable,
  persistExecutable: baseUtil.persistExecutable,
  runInstance: baseUtil.runInstance,
  generateExampleSync: baseUtil.generateExampleSync,

  streamifyParameters: streamUtil.streamifyParameters,
  unstreamifyResults: streamUtil.unstreamifyResults,
  throughStream: streamUtil.throughStream,
  writeParameterFilesStream: streamUtil.writeParameterFilesStream,
  gatherParametersStream: streamUtil.gatherParametersStream,
  readResultFilesStream: streamUtil.readResultFilesStream,
  rearrangeStream: streamUtil.rearrangeStream,

  embeddedExecutableSchema: baseUtil.embeddedExecutableSchema,
  embeddedExecutableSchemaXml: baseUtil.embeddedExecutableSchemaXml,
  instanceSchema: baseUtil.instanceSchema,
  instanceSchemaXml: baseUtil.instanceSchemaXml,
  instanceWritableSchema: baseUtil.instanceWritableSchema,
  instanceWritableSchemaXml: baseUtil.instanceWritableSchemaXml
};
