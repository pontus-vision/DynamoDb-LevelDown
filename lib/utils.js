const fileType = require('file-type');

function isBuffer(object){
  if(!object || typeof object !== 'object') return false;
  return Buffer.isBuffer(object) || (object.type === 'Buffer' && Array.isArray(object.data));
};

exports.isBuffer = isBuffer;

function castToBuffer(object){
  if(isBuffer(object))            return Buffer.from(object);
  if(Array.isArray(object))       return Buffer.from(object);
  if(typeof object === 'string')  return Buffer.from(object);
  if(typeof object === 'boolean') return Buffer.from([object]);
  if(typeof object === 'number')  return Buffer.from([object]); // not accurate at all
  if(object === null || object === undefined) return Buffer.alloc(0);
  return object;
};

exports.castToBuffer = castToBuffer;

function isPlainObject(object){
  return typeof object === 'object' && object !== null && !Array.isArray(object) && !Buffer.isBuffer(object);
};

exports.isPlainObject = isPlainObject;

function parseJSON(string, safe=false){
  try{
    return JSON.parse(string, (key, value) => {
      if(isBuffer(value)) return castToBuffer(string);
      return value;
    });
  }catch(err){
    if(!safe) throw err;
  }
}

exports.parseJSON = parseJSON;

function stringifyJSON(object){
  // when undefined gets passed, we want that to return
  // the string 'null' so we get valid JSON in that case
  return JSON.stringify(object, null, 2) || 'null';
}

exports.stringifyJSON = stringifyJSON;

function toFileType(object){
  if(isBuffer(object)){
    const buffer = castToBuffer(object),
          type   = fileType(buffer);
    if(type) return type;
    return {
      ext: 'bin',
      mime: 'application/octet-stream'
    };
  }
  if(typeof object === 'string') return {
    ext: 'txt',
    mime: 'text/plain'
  };
  return {
    ext:  'json',
    mime: 'application/json'
  };
}

exports.toFileType = toFileType;

exports.marshalize = function marshalize(object){
  if(isBuffer(object)) {
    const { mime } = toFileType(object);
    return {
      mime:   mime || 'application/octet-stream',
      buffer: object
    };
  }
  if(typeof object === 'string') return {
    mime:   'text/plain',
    buffer: Buffer.from(object)
  };
  return {
    mime:   'application/json',
    buffer: Buffer.from(stringifyJSON(object))
  }
};

exports.demarshalize = function demarshalize({ buffer, mime }){
  if(mime  === 'text/plain')        return buffer.toString();
  if(mime  === 'application/json')  return parseJSON(buffer.toString());
  return buffer;
}

exports.serialize = function serialize (value, asBuffer) {
  
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

exports.deserialize = function deserialize (val, asBuffer) {
  const type = Object.keys(val || {})[0] || 'NULL';
  const value = (val || {})[type];

  const reduce = function (value) {
    return Object.keys(value).reduce((acc, key) => {
      acc[key] = deserialize(value[key], asBuffer)
      return acc
    }, {})
  }
  switch (type) {
    case 'NULL' : return null;
    case 'S' : return Buffer.from(value).toString()
    case 'B': return Buffer.from(value)
    case 'BOOL' : return value
    case 'N' : return parseFloat(value, 10)
    case 'L' : return value.map(deserialize, asBuffer)
    case 'M' : return reduce(value)
    default : throw new Error(`cannot parse ${type}.`)
  }
}
