function isBuffer(object){
  if(!object || typeof object !== 'object') return false;
  return Buffer.isBuffer(object) || (object.type === 'Buffer' && Array.isArray(object.data));
};

exports.isBuffer = isBuffer;

exports.castToBuffer = function castToBuffer(object){
  if(isBuffer(object))            return Buffer.from(object);
  if(Array.isArray(object))       return Buffer.from(object);
  if(typeof object === 'string')  return Buffer.from(object);
  if(typeof object === 'boolean') return Buffer.from([object]);
  if(typeof object === 'number')  return Buffer.from([object]); // not accurate at all
  if(object === null || object === undefined) return Buffer.alloc(0);
  return object;
};

exports.isPlainObject = function isPlainObject(object){
  return typeof object === 'object' && object !== null && !Array.isArray(object) && !Buffer.isBuffer(object);
};

