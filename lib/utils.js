const fileType     = require('file-type'),
      blobToBuffer = require('blob-to-buffer');

function isBuffer(object){
  if(!object || typeof object !== 'object') return false;
  return Buffer.isBuffer(object) || (object.type === 'Buffer' && Array.isArray(object.data));
};

exports.isBuffer = isBuffer;

function isBlob(object){
  return object.constructor.name === "Blob";
}

exports.isBlob = isBlob;

async function castToBuffer(object){
  if(isBlob(object)) return await new Promise((resolve, reject) => {
    blobToBuffer(blob, function (err, buffer) {
      if (err) throw reject(err);
      resolve(buffer);
    });
  });
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

async function reviveObject(object){
  const keys = Object.keys(object);
  let i = 0;
  while(i < keys.length){
    const key   = keys[i],
          value = object[key];
    i++;
    if(await isBuffer(value)) {
      object[key] = await castToBuffer(value);
    } else if (isPlainObject(value)) {
      await reviveObject(value);
    }
  };
  return object;
}

async function parseJSON(string, safe=false){
  try{
    const object = JSON.parse(string);
    await reviveObject(object);
    return object;
  }catch(err){
    if(!safe) throw err;
  }
}

exports.parseJSON = parseJSON;

async function stringifyJSON(object){
  // when undefined gets passed, we want that to return
  // the string 'null' so we get valid JSON in that case
  return JSON.stringify(object, null, 2) || 'null';
}

exports.stringifyJSON = stringifyJSON;

async function toFileType(object){
  if(isBuffer(object)){
    const buffer = await castToBuffer(object),
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

exports.marshalize = async function marshalize(object){
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
    buffer: Buffer.from(await stringifyJSON(object))
  }
};

exports.demarshalize = async function demarshalize({ buffer, mime }){
  if(mime  === 'text/plain')        return buffer.toString();
  if(mime  === 'application/json')  return await parseJSON(buffer.toString());
  return buffer;
}

exports.serialize = async function serialize (value, asBuffer) {
  
  if (value === null ||
      value === undefined ||
      value === '' ||
      (Buffer.isBuffer(value) && value.length === 0)) {
    return {NULL: true}
  }

  let type = value.constructor.name;

  if(isBuffer(value)) value = await castToBuffer(value);

  const reduce = async function (value) {
    const acc = {};
    for(let key of Object.keys(value)){
      acc[key] = await serialize(value[key], asBuffer);
    }
    return acc;
  };

  switch (type) {
    case 'String': return {S: value}
    case 'Buffer' : return {B: Buffer.from(value)}
    case 'Boolean' : return {BOOL: value}
    case 'Number' : return {N: String(value)}
    case 'Array' : return {L: await (async () => {
      const output = [];
      for(let item of value){
        output.push(await serialize(item));
      }
      return output;
    })()}
    case 'Object' : return {M: await reduce(value)}
    default : throw new Error(`cannot serialize ${type}`)
  }
};

exports.deserialize = async function deserialize (val, asBuffer) {
  const type = Object.keys(val || {})[0] || 'NULL';
  const value = (val || {})[type];

  const reduce = async function (value) {
    const acc = {};
    Object.keys(value).forEach(async (key) => {
      acc[key] = await deserialize(value[key], asBuffer)
    });
    return acc;
  };

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
