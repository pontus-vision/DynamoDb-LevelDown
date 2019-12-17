import test from 'tape';
import { Keys } from '../dist/lib/types';
import {
  cloneObject,
  maybeDelay,
  withoutKeys,
  serialize,
  rangeKeyFrom,
  hexEncodeString,
  castToBuffer,
  isBuffer,
  deserialize
} from '../dist/lib/utils';

/*
 * Basic sanity unit tests
 */
test('utility tests', t => {
  t.test('object cloning', t => {
    class MyThing {
      foo: string = 'bar';
      fizz: string = 'gig';
      random: number = 1;
      alive: boolean = false;
    }
    const obj1 = new MyThing();
    const obj2 = cloneObject(obj1);
    t.deepEquals(obj2, obj1, 'object clones');
    t.end();
  });

  t.test('async delay', t => {
    t.test('delays with ms > 0', async t => {
      const delayTimeMs = 100;
      const timestamp1 = Date.now();
      await maybeDelay(delayTimeMs);
      const timestamp2 = Date.now();
      const difference = timestamp2 - timestamp1;
      t.true(difference >= delayTimeMs, `delayed for ${difference}`);
      t.end();
    });
    t.test('does not delays with ms <= 0', async t => {
      const delayTimeMs = 0;
      const timestamp1 = Date.now();
      await maybeDelay(delayTimeMs);
      const timestamp2 = Date.now();
      const difference = timestamp2 - timestamp1;
      t.true(difference <= 5, `delayed for ${difference}`);
      t.end();
    });
  });

  t.test('underlying key removal', t => {
    t.test('success with undefined', t => {
      const keyless = withoutKeys(<any>undefined);
      t.notOk(keyless, 'give undefined, get undefined');

      t.end();
    });

    t.test('success with basic value', t => {
      const keyless = withoutKeys(<any>1);
      t.ok(keyless, 'get what we give');

      t.end();
    });

    t.test('success with defined', t => {
      const dbItem = {
        [Keys.HASH_KEY]: serialize('fake'),
        [Keys.RANGE_KEY]: serialize('news'),
        [Keys.DATA_KEY]: serialize('everywhere')
      };
      const keyless = withoutKeys(dbItem);
      t.notOk(keyless[Keys.HASH_KEY], 'has no hash key');
      t.notOk(keyless[Keys.RANGE_KEY], 'has no range key');
      t.ok(keyless[Keys.DATA_KEY], 'has data key');

      t.end();
    });

    t.end();
  });

  t.test('range key acquisition', t => {
    t.test('error with undefined', t => {
      try {
        const key = rangeKeyFrom(undefined);
        t.notOk(key, 'got key from undefined');
      } catch (e) {
        t.ok(e, 'has error');
        t.ok(e.message, 'has error message');
        t.true(/.*no keys.*undefined.*/i.test(e.message), 'correct error message');
      }

      t.end();
    });

    t.test('error with unsupported type', t => {
      try {
        const key = rangeKeyFrom({ foo: 'bar' });
        t.notOk(key, 'got key from unsupported');
      } catch (e) {
        t.ok(e, 'has error');
        t.ok(e.message, 'has error message');
        t.true(/.*no range key available.*/i.test(e.message), 'correct error message');
      }

      t.end();
    });

    t.test('key from item directly', t => {
      const key = rangeKeyFrom({ [Keys.RANGE_KEY]: { S: 'foo' } });
      t.ok(key, 'any key found');
      t.equal(key, 'foo', 'correct key found');

      t.end();
    });

    t.test('key from item with `Key`', t => {
      const key = rangeKeyFrom({ Key: { [Keys.RANGE_KEY]: { S: 'foo' } } });
      t.ok(key, 'any key found');
      t.equal(key, 'foo', 'correct key found');

      t.end();
    });

    t.test('key from item with `PutRequest.Item`', t => {
      const key = rangeKeyFrom({ PutRequest: { Item: { [Keys.RANGE_KEY]: { S: 'foo' } } } });
      t.ok(key, 'any key found');
      t.equal(key, 'foo', 'correct key found');

      t.end();
    });

    t.test('key from item with `DeleteRequest.Key`', t => {
      const key = rangeKeyFrom({ DeleteRequest: { Key: { [Keys.RANGE_KEY]: { S: 'foo' } } } });
      t.ok(key, 'any key found');
      t.equal(key, 'foo', 'correct key found');

      t.end();
    });

    t.end();
  });

  t.test('hex encode strings', t => {
    const encoded = hexEncodeString('foobase');
    t.equal(encoded, '666f6f62617365', 'correctly encodes string');

    t.end();
  });

  t.test('buffer casting', t => {
    t.throws(() => castToBuffer(<any>(() => {})), /.*not supported.*/, 'throws correct error when unsupported');

    t.test('null || undefined => Buffer(empty)', t => {
      let output: Buffer | null = null;

      output = castToBuffer(null);
      t.ok(output, 'got buffer from null');
      t.equal(output.length, 0, 'is empty buffer from null');

      output = castToBuffer(undefined);
      t.ok(output, 'got buffer from undefined');
      t.equal(output.length, 0, 'is empty buffer from undefined');

      t.end();
    });

    t.test('Buffer => Buffer', t => {
      const input = Buffer.from('foo');
      const output = castToBuffer(input);
      t.equal(output, input, 'input and output buffers are equal');

      t.end();
    });

    t.test('Array => Buffer', t => {
      const input = [10, 20, 30, 40, 55];
      const output = castToBuffer(input);
      t.ok(output, 'got buffer');

      const actual = [];
      const iterator = output.values();
      while (true) {
        const next = iterator.next();
        if (next.value) actual.push(next.value);
        if (next.done === true) break;
      }
      t.deepEqual(actual, input, 'input and output buffers are equal');

      t.end();
    });

    t.test('String => Buffer', t => {
      const input = 'foo';
      const output = castToBuffer(input);
      t.ok(output, 'got buffer from string');
      t.equal(output.toString(), input, 'buffer is expected string');

      t.end();
    });

    t.test('Number => Buffer', t => {
      const input = 50;
      const output = castToBuffer(input);
      t.ok(output, 'got buffer from number');
      t.looseEqual(output.readFloatBE(0), input, 'buffer is expected number');

      t.end();
    });

    t.test('Boolean => Buffer', t => {
      [true, false].forEach(v => {
        const input = v;
        const output = castToBuffer(input);
        t.ok(output, 'got buffer from boolean');
        t.looseEqual(output.readUInt8(0), input, 'buffer is expected boolean');
      });
      t.end();
    });

    t.test('Object => Buffer', t => {
      const input = { iam: 'a teapot', short: 'and stout', here: 'is my handle', stamped: 42, alive: true };
      const output = castToBuffer(input);
      t.ok(output, 'got buffer from object');
      t.deepEqual(JSON.parse(String(output)), input, 'buffer is expected number');

      t.end();
    });

    t.end();
  });

  t.test('buffer detection', t => {
    t.test('primitives are not buffers', t => {
      [666, true, 'foo', new Date()].forEach(v => t.notOk(isBuffer(v), `'${typeof v}' is not a buffer`));
      t.end();
    });

    t.ok(isBuffer(Buffer.alloc(0)), 'empty buffer is a buffer');
    t.ok(isBuffer({ type: 'Buffer', data: [0] }), 'buffer-like is a buffer');

    t.end();
  });

  t.test('edge serialization', t => {
    const func = function(foo: any) {
      return foo;
    };

    t.test('cannot serialize function', t => {
      try {
        const result = serialize(func);
        t.notOk(result, 'function serialized');
      } catch (e) {
        t.ok(e, 'function not serialized');
        t.true(/.*transformer not available.*/i.test(e.message), 'correct error message');
      } finally {
        t.end();
      }
    });

    t.test('cannot deserialize function', t => {
      try {
        const result = deserialize(func);
        t.notOk(result, 'function serialized');
      } catch (e) {
        t.ok(e, 'function not serialized');
        t.true(/.*transformer not available.*/i.test(e.message), 'correct error message');
      } finally {
        t.end();
      }
    });

    t.test('handles empty buffers', t => {
      const original = Buffer.alloc(0);
      const serializedBuffer = serialize(original);
      const deserializedBuffer = deserialize(serializedBuffer);
      t.comment(`Buffer(0) => ${JSON.stringify(serializedBuffer)}`);
      t.looseEqual(deserializedBuffer, original, 'deserialized empty buffer');

      t.end();
    });

    t.test('handles null', t => {
      const input = null;
      const serialized = serialize(input);
      const deserialized = deserialize(serialized);
      t.notOk(deserialized, 'give null, get undefined');

      t.end();
    });

    t.test('handles undefined', t => {
      const input = undefined;
      const serialized = serialize(input);
      const deserialized = deserialize(serialized);
      t.notOk(deserialized, 'give undefined, get undefined');

      t.end();
    });

    t.end();
  });

  t.end();
});
