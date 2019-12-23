import { DynamoDB, S3 } from 'aws-sdk';
import {
  IteratorOptions,
  Keys,
  ValueTransformer,
  Attachment,
  AttachmentDefinition,
  S3Pointer,
  AttachmentResult,
  S3Pointers,
  S3ObjectBatch
} from './types';
function createS3Pointer(key: string): S3Pointer {
  return {
    _s3key: key
  };
}

type ExtractionItem = { key: string; keyPath: string; value: any; parent?: any };

/* @internal */
export function promiseS3Body(input: { Body?: S3.Body }): S3.Body {
  return input.Body || Buffer.alloc(0);
}

/* @internal */
export function extractAttachments(key: any, value: any, definitions: AttachmentDefinition[]): AttachmentResult {
  if (!!value && isPlainObject(value) && definitions.length > 0) {
    const clone = cloneObject(value);
    const result: Attachment[] = [];
    const flattened: ExtractionItem[] = [{ key, keyPath: key, value: clone }];
    do {
      const entry = flattened.shift() as ExtractionItem;
      const element = entry.value;
      const fullKey = entry.keyPath as string;
      const defMatch = definitions.find(d => d.match.test(fullKey));
      if (!!defMatch) {
        if (entry.parent) {
          entry.parent[entry.key] = createS3Pointer(fullKey);
        }
        result.push({
          key: fullKey,
          data: castToBuffer(element[defMatch.dataKey], defMatch.dataEncoding),
          contentType: element[defMatch.contentTypeKey]
        });
      }
      for (const propKey in element) {
        const current = element[propKey];
        if (!!current && isPlainObject(current)) {
          const keyPath = [entry.keyPath, propKey].filter(v => !!v && v.length > 0).join('/');
          flattened.push({ key: propKey, keyPath, value: current, parent: element });
        }
      }
    } while (flattened.length > 0);
    return { newValue: clone, attachments: result };
  }
  return { newValue: value, attachments: [] };
}

/* @internal */
export function extractS3Pointers(key: any, value: any): S3Pointers {
  if (!!value && isPlainObject(value)) {
    const result: S3Pointers = {};
    const flattened: ExtractionItem[] = [{ key, keyPath: key, value }];
    do {
      const entry = flattened.shift() as ExtractionItem;
      const element = entry.value;
      for (const propKey in element) {
        const current = element[propKey];
        const keyPath = [entry.keyPath, propKey].filter(v => !!v && v.length > 0).join('/');
        if (!!current && isPlainObject(current)) {
          if (current.hasOwnProperty('_s3key')) {
            result[keyPath] = current;
          } else {
            flattened.push({ key: propKey, keyPath, value: current });
          }
        }
      }
    } while (flattened.length > 0);
    return result;
  }
  return {};
}

/* @internal */
export function restoreAttachments(
  value: any,
  pointers: S3Pointers,
  attachments: S3ObjectBatch,
  definitions: AttachmentDefinition[]
): any {
  if (!!value && !!pointers && !!attachments) {
    const newValue = cloneObject(value);
    for (const keyPath in pointers) {
      const definition = definitions.find(d => d.match.test(keyPath));
      if (!definition) continue;

      const propNames = keyPath.split('/');
      const lastPropName = propNames.pop() as string;
      const target = propNames.reduce((p: any, c: string) => p[c] || p, newValue);
      const s3Object = attachments[target[lastPropName]._s3key];
      target[lastPropName] = {
        [definition.contentTypeKey]: s3Object.ContentType,
        [definition.dataKey]: promiseS3Body(s3Object).toString(definition.dataEncoding)
      };
    }
    return newValue;
  }
  return value;
}

/* @internal */
export function cloneObject<T>(obj: T): T {
  if (obj === null || typeof obj !== 'object') {
    return obj;
  } else if (isBuffer(obj)) {
    const newBuffer = Buffer.alloc(((obj as unknown) as Buffer).length);
    ((obj as unknown) as Buffer).copy(newBuffer);
    return (newBuffer as unknown) as T;
  }

  const temp = Object.create(<Object>obj);
  for (const key in obj) {
    temp[key] = cloneObject(obj[key]);
  }
  return <T>temp;
}

/* @internal */
export async function maybeDelay(ms?: number): Promise<void> {
  if (!!ms && ms > 0) {
    await new Promise(resolve => setTimeout(resolve, ms));
  }
}

/* @internal */
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

/* @internal */
export function keyConditionsFor(hashKey: string, rangeCondition: DynamoDB.Condition): DynamoDB.KeyConditions {
  return {
    [Keys.HASH_KEY]: {
      ComparisonOperator: 'EQ',
      AttributeValueList: [{ S: hashKey }]
    },
    [Keys.RANGE_KEY]: rangeCondition
  };
}

/* @internal */
export function dataFromItem<T = any>(item: DynamoDB.ItemCollectionKeyAttributeMap): T {
  const deserialized = deserialize({ M: item });
  return deserialized[Keys.DATA_KEY];
}

/* @internal */
export function rangeKeyFrom(item: any): string {
  if (!item) throw new Error('No keys are available from undefined');

  if ('Key' in item) return item.Key[Keys.RANGE_KEY].S;
  if (Keys.RANGE_KEY in item) return item[Keys.RANGE_KEY].S;
  if ('PutRequest' in item && 'Item' in item.PutRequest) return item.PutRequest.Item[Keys.RANGE_KEY].S;
  if ('DeleteRequest' in item && 'Key' in item.DeleteRequest) return item.DeleteRequest.Key[Keys.RANGE_KEY].S;

  throw new Error(`No range key available from ${typeof item}`);
}

/* @internal */
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

/* @internal */
export function isBuffer(object: any): boolean {
  if (!object || typeof object !== 'object') return false;
  return Buffer.isBuffer(object) || (object.type === 'Buffer' && Array.isArray(object.data));
}

/* @internal */
export function hexEncodeString(str: string): string {
  var hex = '';
  for (var pos = 0; pos < str.length; pos++) hex += String(str.charCodeAt(pos).toString(16));
  return hex;
}

/* @internal */
export function castToBuffer(
  object: Buffer | Array<any> | string | boolean | number | null | undefined | object,
  encoding?: BufferEncoding
): Buffer {
  let result: Buffer;
  if (object instanceof Buffer) result = object;
  else if (object instanceof Array) result = Buffer.from(object);
  else if (typeof object === 'string') result = Buffer.from(object, encoding);
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

/* @internal */
export function isPlainObject(object: any): boolean {
  return typeof object === 'object' && object !== null && !Array.isArray(object) && !Buffer.isBuffer(object);
}

/* @internal */
export const serialize = (value: any) => getTransformerOrThrow(value).toDb(value);

/* @internal */
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
