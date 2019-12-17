import { DynamoDB } from 'aws-sdk';
import { IteratorOptions, Keys, ValueTransformer } from './types';
import fs from 'fs';

export function cloneObject<T>(obj: T): T {
  if (obj === null || typeof obj !== 'object') {
    return obj;
  }

  const temp = Object.create(<Object>obj);
  for (const key in obj) {
    temp[key] = cloneObject(obj[key]);
  }
  return <T>temp;
}

export async function maybeDelay(ms?: number): Promise<void> {
  if (!!ms && ms > 0) {
    await new Promise(resolve => setTimeout(resolve, ms));
  }
}

export function withoutKeys<T extends DynamoDB.ItemCollectionKeyAttributeMap>(item: T): T {
  if (!item) return item;

  const newItem = <any>cloneObject(item);
  const delProps = [Keys.HASH_KEY, Keys.RANGE_KEY];
  if (isPlainObject(newItem)) {
    Reflect.setPrototypeOf(newItem, null);
    delProps.forEach(k => {
      Reflect.set(newItem, k, undefined);
      Reflect.deleteProperty(newItem, k);
    });
  }
  return <T>newItem;
}

export function keyConditionsFor(hashKey: string, rangeCondition: DynamoDB.Condition): DynamoDB.KeyConditions {
  return {
    [Keys.HASH_KEY]: {
      ComparisonOperator: 'EQ',
      AttributeValueList: [{ S: hashKey }]
    },
    [Keys.RANGE_KEY]: rangeCondition
  };
}

export function dataFromItem<T = any>(item: DynamoDB.ItemCollectionKeyAttributeMap): T {
  const deserialized = deserialize({ M: item });
  return deserialized[Keys.DATA_KEY];
}

export function rangeKeyFrom(item: any): string {
  if (!item) throw new Error('No keys are available from undefined');

  if ('Key' in item) return item.Key[Keys.RANGE_KEY].S;
  if (Keys.RANGE_KEY in item) return item[Keys.RANGE_KEY].S;
  if ('PutRequest' in item && 'Item' in item.PutRequest) return item.PutRequest.Item[Keys.RANGE_KEY].S;
  if ('DeleteRequest' in item && 'Key' in item.DeleteRequest) return item.DeleteRequest.Key[Keys.RANGE_KEY].S;

  throw new Error(`No range key available from ${typeof item}`);
}

export function createRangeKeyCondition(opts: IteratorOptions): DynamoDB.Types.Condition {
  const defaultStart = '\u0000';
  const defaultEnd = '\xff\xff\xff\xff\xff\xff\xff\xff';
  let result: DynamoDB.Types.Condition;

  if (opts.gt && opts.lt) {
    result = {
      ComparisonOperator: 'BETWEEN',
      AttributeValueList: [{ S: opts.gt }, { S: opts.lt }]
    };
  } else if (opts.lt) {
    result = {
      ComparisonOperator: 'LT',
      AttributeValueList: [{ S: opts.lt }]
    };
  } else if (opts.gt) {
    result = {
      ComparisonOperator: 'GT',
      AttributeValueList: [{ S: opts.gt }]
    };
  } else if (!opts.start && !opts.end) {
    result = {
      ComparisonOperator: 'BETWEEN',
      AttributeValueList: [{ S: defaultStart }, { S: defaultEnd }]
    };
  } else if (!opts.end) {
    const op = opts.reverse ? 'LE' : 'GE';
    result = {
      ComparisonOperator: op,
      AttributeValueList: [{ S: opts.start }]
    };
  } else if (!opts.start) {
    const op = opts.reverse ? 'GE' : 'LE';
    result = {
      ComparisonOperator: op,
      AttributeValueList: [{ S: opts.end }]
    };
  } else if (opts.reverse) {
    result = {
      ComparisonOperator: 'BETWEEN',
      AttributeValueList: [{ S: opts.end }, { S: opts.start }]
    };
  } else {
    result = {
      ComparisonOperator: 'BETWEEN',
      AttributeValueList: [{ S: opts.start }, { S: opts.end }]
    };
  }

  return result;
}

export function isBuffer(object: any): boolean {
  if (!object || typeof object !== 'object') return false;
  return Buffer.isBuffer(object) || (object.type === 'Buffer' && Array.isArray(object.data));
}

export function hexEncodeString(str: string): string {
  var hex = '';
  for (var pos = 0; pos < str.length; pos++) hex += String(str.charCodeAt(pos).toString(16));
  return hex;
}

export function castToBuffer(
  object: Buffer | Array<any> | string | boolean | number | null | undefined | object
): Buffer {
  let result: Buffer;
  if (object instanceof Buffer) result = object;
  else if (object instanceof Array) result = Buffer.from(object);
  else if (typeof object === 'string') result = Buffer.from(object);
  else if (typeof object === 'boolean') {
    const b = Buffer.alloc(1);
    b.writeUInt8(object === true ? 1 : 0, 0);
    result = b;
  } else if (typeof object === 'number') {
    const b = Buffer.alloc(8);
    b.writeFloatBE(object, 0);
    result = b;
  } else if (isPlainObject(object)) result = Buffer.from(JSON.stringify(object));
  else if (object === null || object === undefined) result = Buffer.alloc(0);
  else throw new Error('The object is not supported for conversion to buffer');

  return result;
}

export function isPlainObject(object: any): boolean {
  return typeof object === 'object' && object !== null && !Array.isArray(object) && !Buffer.isBuffer(object);
}

export const serialize = (value: any) => getTransformerOrThrow(value).toDb(value);
export const deserialize = (value: any) => getTransformerOrThrow(value).fromDb(value);

const getTransformerOrThrow = (value: any): ValueTransformer => {
  const transformer = TRANSFORMERS.find(transformer => transformer.for(value));
  if (!transformer) throw new Error(`Transformer not available for '${typeof value}'`);
  return transformer;
};
const toBase64 = (value: string) => Buffer.from(value).toString('base64');
const dbotName = (value: any) => Object.keys(value || {}).shift();
const ctorName = (value: Object) => value.constructor.name;
const transformReduce = (value: any, transformer: (value: any) => any) => {
  const acc: any = {};
  for (const key in value) {
    acc[key] = transformer(value[key]);
  }
  return acc;
};
const transformMapFrom = (value: any): Array<any> => {
  const result = [];
  for (const typedItem of value) {
    const item = deserialize(typedItem);
    result.push(item);
  }
  return result;
};
const transformMapTo = (value: Array<any>): Array<any> => {
  return value.map(item => serialize(item));
};
const TRANSFORMER_SPECIALS = {
  NaN: toBase64(Number.NaN.toString()),
  EmptyString: toBase64('EMPTY_STRING'),
  EmptyBuffer: toBase64('EMPTY_BUFFER')
};
const TRANSFORMERS: ValueTransformer[] = [
  {
    for: (value: any) => value === null || value === undefined || dbotName(value) === 'NULL',
    toDb: () => ({ NULL: true }),
    fromDb: () => undefined
  },
  {
    for: (value: any) =>
      Number.isNaN(value) || (dbotName(value) === 'B' && String(value.B) === TRANSFORMER_SPECIALS.NaN),
    toDb: () => ({ B: TRANSFORMER_SPECIALS.NaN }),
    fromDb: () => Number.NaN
  },
  {
    for: (value: any) =>
      (isBuffer(value) && value.length === 0) ||
      (dbotName(value) === 'B' && String(value.B) === TRANSFORMER_SPECIALS.EmptyBuffer),
    toDb: () => ({ B: TRANSFORMER_SPECIALS.EmptyBuffer }),
    fromDb: () => Buffer.alloc(0)
  },
  {
    for: (value: any) =>
      (ctorName(value) === 'String' && value.trim() === '') ||
      (dbotName(value) === 'B' && String(value.B) === TRANSFORMER_SPECIALS.EmptyString),
    toDb: () => ({ B: TRANSFORMER_SPECIALS.EmptyString }),
    fromDb: () => ''
  },
  {
    for: (value: any) => ctorName(value) === 'String' || dbotName(value) === 'S',
    toDb: (value: any) => ({ S: value }),
    fromDb: (value: any) => value.S
  },
  {
    for: (value: any) => ctorName(value) === 'Boolean' || dbotName(value) === 'BOOL',
    toDb: (value: any) => ({ BOOL: value }),
    fromDb: (value: any) => value.BOOL
  },
  {
    for: (value: any) => ctorName(value) === 'Number' || dbotName(value) === 'N',
    toDb: (value: any) => ({ N: String(value) }),
    fromDb: (value: any) => Number(value.N)
  },
  {
    for: (value: any) => isBuffer(value) || ctorName(value) === 'Buffer' || dbotName(value) === 'B',
    toDb: (value: any) => ({ B: value }),
    fromDb: (value: any) => Buffer.from(value.B)
  },
  {
    for: (value: any) => ctorName(value) === 'Array' || dbotName(value) === 'L',
    toDb: (value: any) => ({ L: transformMapTo(value) }),
    fromDb: (value: any) => transformMapFrom(value.L)
  },
  {
    for: (value: any) => ctorName(value) === 'Object' || dbotName(value) === 'M',
    toDb: (value: any) => ({ M: transformReduce(value, serialize) }),
    fromDb: (value: any) => transformReduce(value.M, deserialize)
  }
];
