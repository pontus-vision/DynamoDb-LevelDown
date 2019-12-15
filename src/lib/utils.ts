import { DynamoDB } from 'aws-sdk';
import { IteratorOptions, Keys } from './types';

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

  if (opts.gt && opts.lt) {
    return {
      ComparisonOperator: 'BETWEEN',
      AttributeValueList: [{ S: opts.gt }, { S: opts.lt }]
    };
  }

  if (opts.lt) {
    return {
      ComparisonOperator: 'LT',
      AttributeValueList: [{ S: opts.lt }]
    };
  }

  if (opts.gt) {
    return {
      ComparisonOperator: 'GT',
      AttributeValueList: [{ S: opts.gt }]
    };
  }

  if (!opts.start && !opts.end) {
    return {
      ComparisonOperator: 'BETWEEN',
      AttributeValueList: [{ S: defaultStart }, { S: defaultEnd }]
    };
  }

  if (!opts.end) {
    const op = opts.reverse ? 'LE' : 'GE';
    return {
      ComparisonOperator: op,
      AttributeValueList: [{ S: opts.start }]
    };
  }

  if (!opts.start) {
    const op = opts.reverse ? 'GE' : 'LE';
    return {
      ComparisonOperator: op,
      AttributeValueList: [{ S: opts.end }]
    };
  }

  if (opts.reverse) {
    return {
      ComparisonOperator: 'BETWEEN',
      AttributeValueList: [{ S: opts.end }, { S: opts.start }]
    };
  }

  return {
    ComparisonOperator: 'BETWEEN',
    AttributeValueList: [{ S: opts.start }, { S: opts.end }]
  };
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

export function castToBuffer(object: Buffer | Array<any> | string | boolean | number | null | undefined): Buffer {
  if (object instanceof Buffer) return object;
  if (object instanceof Array) return Buffer.from(object);
  if (typeof object === 'string') return Buffer.from(object);
  if (typeof object === 'boolean') {
    const b = Buffer.alloc(1);
    b.writeUInt8(object === true ? 1 : 0, 0);
    return b;
  }
  if (typeof object === 'number') {
    const b = Buffer.alloc(8);
    b.writeFloatBE(object, 0);
    return b;
  }
  if (object === null || object === undefined) return Buffer.alloc(0);

  throw new Error('The object is not supported for conversion to buffer');
}

export function isPlainObject(object: any): boolean {
  return typeof object === 'object' && object !== null && !Array.isArray(object) && !Buffer.isBuffer(object);
}

export function serialize(value: any) {
  const transformer = TRANSFORMERS.find(transformer => transformer.for(value));
  if (!!transformer) return transformer.toDb(value);
  throw new Error(`Serialization not available for ${typeof value}`);
}

export function deserialize(value: any) {
  const transformer = TRANSFORMERS.find(transformer => transformer.for(value));
  if (!!transformer) return transformer.fromDb(value);
  throw new Error(`Deserialization not available for ${typeof value}`);
}

const toBase64 = (value: string) => Buffer.from(value).toString('base64');
const dbotName = (value: any) => Object.keys(value || {}).shift();
const ctorName = (value: Object) => value.constructor.name;
const transformReduceFrom = (value: any) => {
  const acc: any = {};
  for (const key in value) {
    if (Reflect.has(value, key)) {
      acc[key] = deserialize(value[key]);
    }
  }
  return acc;
};
const transformReduceTo = (value: any) => {
  const acc: any = {};
  for (const key in value) {
    if (Reflect.has(value, key)) {
      acc[key] = serialize(value[key]);
    }
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
const TRANSFORMER_SPECIALS_VALUES = Object.values(TRANSFORMER_SPECIALS);
const TRANSFORMERS = [
  {
    for: (value: any) => value === null || value === undefined || dbotName(value) === 'NULL',
    toDb: () => ({ NULL: true }),
    fromDb: () => undefined
  },
  { for: (value: any) => Number.isNaN(value), toDb: () => ({ B: TRANSFORMER_SPECIALS.NaN }), fromDb: () => Number.NaN },
  {
    for: (value: any) => String(value).trim() === '',
    toDb: () => ({ B: TRANSFORMER_SPECIALS.EmptyString }),
    fromDb: () => ''
  },
  {
    for: (value: any) => !!value && isBuffer(value) && value.length === 0,
    toDb: () => ({ B: TRANSFORMER_SPECIALS.EmptyBuffer }),
    fromDb: () => Buffer.alloc(0)
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
    fromDb: (value: any) => {
      const buffer = Buffer.from(value.B);
      const bufferString = String(buffer);
      const specialKey = TRANSFORMER_SPECIALS_VALUES.find(v => bufferString === v) || undefined;
      switch (specialKey) {
        case TRANSFORMER_SPECIALS.NaN:
          return Number.NaN;
        case TRANSFORMER_SPECIALS.EmptyString:
          return '';
        case TRANSFORMER_SPECIALS.EmptyBuffer:
          return Buffer.alloc(0);
        default:
          return buffer;
      }
    }
  },
  {
    for: (value: any) => ctorName(value) === 'Array' || dbotName(value) === 'L',
    toDb: (value: any) => ({ L: transformMapTo(value) }),
    fromDb: (value: any) => transformMapFrom(value.L)
  },
  {
    for: (value: any) => ctorName(value) === 'Object' || dbotName(value) === 'M',
    toDb: (value: any) => ({ M: transformReduceTo(value) }),
    fromDb: (value: any) => transformReduceFrom(value.M)
  },
  {
    for: () => true,
    toDb: (value: any) => {
      throw new Error(`Cannot serialize ${typeof value} value`);
    },
    fromDb: (value: any) => {
      throw new Error(`Cannot deserialize ${typeof value} value`);
    }
  }
];
