import { ValueTransformer } from './types';
import { isBuffer } from './utils';

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
