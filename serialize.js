const { isBuffer, castToBuffer } = require('./lib/utils');

module.exports = function serialize (value, asBuffer) {
  
  if (value === null ||
      value === undefined ||
      value === '' ||
      (Buffer.isBuffer(value) && value.length === 0)) {
    return {NULL: true}
  }

  let type = value.constructor.name;

  if(isBuffer(value)) value = castToBuffer(value);

  const reduce = function (value) {
    return Object.keys(value).reduce((acc, key) => {
      acc[key] = serialize(value[key], asBuffer)
      return acc
    }, {})
  }

  switch (type) {
    case 'String': return {S: value}
    case 'Buffer' : return {B: Buffer.from(value)}
    case 'Boolean' : return {BOOL: value}
    case 'Number' : return {N: String(value)}
    case 'Array' : return {L: value.map(serialize, asBuffer)}
    case 'Object' : return {M: reduce(value)}
    default : throw new Error(`cannot serialize ${type}`)
  }
};
