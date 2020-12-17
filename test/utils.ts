import test from 'tape';
import { Keys, AttachmentDefinition } from '../src/lib/types';
import { serialize, deserialize } from '../src/lib/serialize';
import {
  cloneObject,
  withoutKeys,
  rangeKeyFrom,
  castToBuffer,
  isBuffer,
  extractAttachments,
  restoreAttachments,
  extractS3Pointers,
  promiseS3Body,
} from '../src/lib/utils';

/*
 * Basic sanity unit tests
 */
test('utility tests', (t) => {
  t.test('object cloning', (t) => {
    class MyThing {
      foo: string = 'bar';
      fizz: string = 'gig';
      random: number = 1;
      alive: boolean = false;
    }
    const obj1 = new MyThing();
    const obj2 = cloneObject(obj1);
    t.deepEqual(obj2, obj1, 'object clones');
    t.end();
  });

  t.test('extractAttachments with top-level/parent-less match', (t) => {
    const attachmentDefinitions: AttachmentDefinition[] = [
      {
        match: /.*/,
        dataKey: 'data',
        contentTypeKey: 'content_type',
        dataEncoding: 'base64',
      },
    ];
    const input = { data: Buffer.from('bar'), content_type: 'text/plain' };
    const result = extractAttachments('foo', input, attachmentDefinitions);

    t.ok(result);
    t.ok(result.newValue);
    t.ok(result.attachments);

    t.end();
  });

  t.test('restoreAttachments', (t) => {
    t.test('promiseS3Body', (t) => {
      const inputWith = { Body: Buffer.from('foo') };
      const inputWithout = { Body: undefined };

      const resultWith = promiseS3Body(inputWith);
      t.ok(resultWith, 'got value from `Buffer`');
      t.equal(resultWith, inputWith.Body, 'got expected value from `Buffer`');

      const resultWithout = promiseS3Body(inputWithout);
      t.ok(resultWithout, 'got value from `undefined`');
      t.equal(Buffer.alloc(0).compare(resultWithout as Buffer), 0, 'got expected value from `undefined`');

      t.end();
    });

    t.test('restoreAttachments with `definitions` mismatch', (t) => {
      const input = { _id: 'foo', _attachments: { 'myImage.png': { _s3key: 'foo/_attachments/myImage.png' } } };
      const pointers = extractS3Pointers(input._id, input);
      const result = restoreAttachments(input, pointers, {}, []);

      t.deepEqual(result, input, 'no changes');

      t.end();
    });

    t.test('restoreAttachments with `undefined` value', (t) => {
      const input = undefined;
      const result = restoreAttachments(input, {}, {}, []);

      t.end(result);
    });

    t.end();
  });

  t.test('underlying key removal', (t) => {
    t.test('success with undefined', (t) => {
      const keyless = withoutKeys(<any>undefined);
      t.notOk(keyless, 'give undefined, get undefined');

      t.end();
    });

    t.test('success with basic value', (t) => {
      const keyless = withoutKeys(<any>1);
      t.ok(keyless, 'get what we give');

      t.end();
    });

    t.test('success with defined', (t) => {
      const dbItem = {
        [Keys.HASH_KEY]: serialize('fake'),
        [Keys.RANGE_KEY]: serialize('news'),
        [Keys.DATA_KEY]: serialize('everywhere'),
      };
      const keyless = withoutKeys(dbItem);
      t.notOk(keyless[Keys.HASH_KEY], 'has no hash key');
      t.notOk(keyless[Keys.RANGE_KEY], 'has no range key');
      t.ok(keyless[Keys.DATA_KEY], 'has data key');

      t.end();
    });

    t.end();
  });

  t.test('range key acquisition', (t) => {
    t.test('error with undefined', (t) => {
      try {
        const key = rangeKeyFrom(undefined);
        t.notOk(key, 'got key from undefined');
      } catch (e) {
        t.ok(e, 'has error');
        t.ok(e.message, 'has error message');
        t.true(/.*no range key.*/i.test(e.message), 'correct error message');
      }

      t.end();
    });

    t.test('error with unsupported type', (t) => {
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

    t.test('key from item directly', (t) => {
      const key = rangeKeyFrom({ [Keys.RANGE_KEY]: { S: 'foo' } });
      t.ok(key, 'any key found');
      t.equal(key, 'foo', 'correct key found');

      t.end();
    });

    t.test('key from item with `Key`', (t) => {
      const key = rangeKeyFrom({ Key: { [Keys.RANGE_KEY]: { S: 'foo' } } });
      t.ok(key, 'any key found');
      t.equal(key, 'foo', 'correct key found');

      t.end();
    });

    t.test('key from item with `PutRequest.Item`', (t) => {
      const key = rangeKeyFrom({ PutRequest: { Item: { [Keys.RANGE_KEY]: { S: 'foo' } } } });
      t.ok(key, 'any key found');
      t.equal(key, 'foo', 'correct key found');

      t.end();
    });

    t.test('key from item with `DeleteRequest.Key`', (t) => {
      const key = rangeKeyFrom({ DeleteRequest: { Key: { [Keys.RANGE_KEY]: { S: 'foo' } } } });
      t.ok(key, 'any key found');
      t.equal(key, 'foo', 'correct key found');

      t.end();
    });

    t.end();
  });

  t.test('buffer casting', (t) => {
    t.throws(() => castToBuffer(<any>(() => {})), /.*not supported.*/, 'throws correct error when unsupported');

    t.test('null || undefined => Buffer(empty)', (t) => {
      let output: Buffer | null = null;

      output = castToBuffer(null);
      t.ok(output, 'got buffer from null');
      t.equal(output.length, 0, 'is empty buffer from null');

      output = castToBuffer(undefined);
      t.ok(output, 'got buffer from undefined');
      t.equal(output.length, 0, 'is empty buffer from undefined');

      t.end();
    });

    t.test('Buffer => Buffer', (t) => {
      const input = Buffer.from('foo');
      const output = castToBuffer(input);
      t.equal(output, input, 'input and output buffers are equal');

      t.end();
    });

    t.test('Array => Buffer', (t) => {
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

    t.test('String => Buffer', (t) => {
      const input = 'foo';
      const output = castToBuffer(input);
      t.ok(output, 'got buffer from string');
      t.equal(output.toString(), input, 'buffer is expected string');

      t.end();
    });

    t.test('Number => Buffer', (t) => {
      const input = 50;
      const output = castToBuffer(input);
      t.ok(output, 'got buffer from number');
      t.looseEqual(output.readFloatBE(0), input, 'buffer is expected number');

      t.end();
    });

    t.test('Boolean => Buffer', (t) => {
      [true, false].forEach((v) => {
        const input = v;
        const output = castToBuffer(input);
        t.ok(output, 'got buffer from boolean');
        t.looseEqual(output.readUInt8(0), input, 'buffer is expected boolean');
      });
      t.end();
    });

    t.test('Object => Buffer', (t) => {
      const input = { iam: 'a teapot', short: 'and stout', here: 'is my handle', stamped: 42, alive: true };
      const output = castToBuffer(input);
      t.ok(output, 'got buffer from object');
      t.deepEqual(JSON.parse(String(output)), input, 'buffer is expected number');

      t.end();
    });

    t.end();
  });

  t.test('buffer detection', (t) => {
    t.test('primitives are not buffers', (t) => {
      [666, true, 'foo', new Date()].forEach((v) => t.notOk(isBuffer(v), `'${typeof v}' is not a buffer`));
      t.end();
    });

    t.ok(isBuffer(Buffer.alloc(0)), 'empty buffer is a buffer');
    t.ok(isBuffer({ type: 'Buffer', data: [0] }), 'buffer-like is a buffer');

    t.end();
  });

  t.test('edge serialization', (t) => {
    const func = function (foo: any) {
      return foo;
    };

    t.test('cannot serialize function', (t) => {
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

    t.test('cannot deserialize function', (t) => {
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

    t.test('handles empty buffers', (t) => {
      const original = Buffer.alloc(0);
      const serializedBuffer = serialize(original);
      const deserializedBuffer = deserialize(serializedBuffer);
      t.equal(deserializedBuffer.compare(original), 0, 'deserialized empty buffer');

      t.end();
    });

    t.test('handles null', (t) => {
      const input = null;
      const serialized = serialize(input);
      const deserialized = deserialize(serialized);
      t.notOk(deserialized, 'give null, get undefined');

      t.end();
    });

    t.test('handles undefined', (t) => {
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
