'use strict'

const ToBase64 = value => Buffer.from(String(value)).toString('base64')

function isBuffer (object) {
  if (!object || typeof object !== 'object') return false
  return Buffer.isBuffer(object) || (object.type === 'Buffer' && Array.isArray(object.data))
}

function hexEncodeString (str) {
  var hex = ''
  for (var pos = 0; pos < str.length; pos++) hex += String(str.charCodeAt(pos).toString(16))
  return hex
}

function castToBuffer (object) {
  if (isBuffer(object)) return object
  if (Array.isArray(object)) return Buffer.from(object)
  if (typeof object === 'string') return Buffer.from(object)
  if (typeof object === 'boolean') return Buffer.from(String(object))
  if (typeof object === 'number') return Buffer.from(String(object))
  if (object === null || object === undefined) return Buffer.alloc(0)
  return object
}

function isPlainObject (object) {
  return typeof object === 'object' && object !== null && !Array.isArray(object) && !Buffer.isBuffer(object)
}

function serialize (value) {
  return TRANSFORMERS.find(transformer => transformer.for(value)).toDb(value)
}

function deserialize (value) {
  return TRANSFORMERS.find(transformer => transformer.for(value)).fromDb(value)
}

const dbotName = value => Object.keys(value || {}).shift()
const ctorName = value => value.constructor.name
const transformReduceFrom = value => {
  const acc = {}
  for (const key in value) {
    if (Reflect.has(value, key)) {
      acc[key] = deserialize(value[key])
    }
  }
  return acc
}
const transformReduceTo = value => {
  const acc = {}
  for (const key of Object.keys(value)) {
    acc[key] = serialize(value[key])
  }
  return acc
}
const transformMapFrom = value => {
  const result = []
  for (const typedItem of value) {
    const item = deserialize(typedItem)
    result.push(item)
  }
  return result
}
const transformMapTo = value => {
  return value.map(item => serialize(item))
}
const TRANSFORMER_VALUES = {
  NaN: ToBase64(Number.NaN),
  EmptyString: ToBase64('EMPTY_STRING'),
  EmptyBuffer: ToBase64('EMPTY_BUFFER')
}
const TRANSFORMERS = [
  {
    for: value => value === null || value === undefined || dbotName(value) === 'NULL',
    toDb: () => ({ NULL: true }),
    fromDb: () => undefined
  },
  { for: value => Number.isNaN(value), toDb: () => ({ B: TRANSFORMER_VALUES.NaN }), fromDb: () => Number.NaN },
  { for: value => String(value).trim() === '', toDb: () => ({ B: TRANSFORMER_VALUES.EmptyString }), fromDb: () => '' },
  {
    for: value => !!value && isBuffer(value) && value.length === 0,
    toDb: () => ({ B: TRANSFORMER_VALUES.EmptyBuffer }),
    fromDb: () => Buffer.alloc(0)
  },
  {
    for: value => ctorName(value) === 'String' || dbotName(value) === 'S',
    toDb: value => ({ S: value }),
    fromDb: value => value.S
  },
  {
    for: value => ctorName(value) === 'Boolean' || dbotName(value) === 'BOOL',
    toDb: value => ({ BOOL: value }),
    fromDb: value => value.BOOL
  },
  {
    for: value => ctorName(value) === 'Number' || dbotName(value) === 'N',
    toDb: value => ({ N: String(value) }),
    fromDb: value => Number(value.N)
  },
  {
    for: value => isBuffer(value) || ctorName(value) === 'Buffer' || dbotName(value) === 'B',
    toDb: value => ({ B: value }),
    fromDb: value => {
      const buffer = Buffer.from(value.B)
      const buferString = String(buffer)
      const specialKey = (
        Object.entries(TRANSFORMER_VALUES).find(([key, value]) => buferString === value) || [undefined]
      ).shift()
      switch (specialKey) {
        case 'NaN':
          return Number.NaN
        case 'EmptyString':
          return ''
        case 'EmptyBuffer':
          return Buffer.alloc(0)
        default:
          return buffer
      }
    }
  },
  {
    for: value => ctorName(value) === 'Array' || dbotName(value) === 'L',
    toDb: value => ({ L: transformMapTo(value) }),
    fromDb: value => transformMapFrom(value.L)
  },
  {
    for: value => ctorName(value) === 'Object' || dbotName(value) === 'M',
    toDb: value => ({ M: transformReduceTo(value) }),
    fromDb: value => transformReduceFrom(value.M)
  },
  {
    for: () => true,
    toDb: value => {
      throw new Error(`Cannot serialize ${typeof value} value`)
    },
    fromDb: value => {
      throw new Error(`Cannot deserialize ${typeof value} value`)
    }
  }
]

module.exports = {
  isBuffer,
  castToBuffer,
  isPlainObject,
  hexEncodeString,
  serialize,
  deserialize
}
