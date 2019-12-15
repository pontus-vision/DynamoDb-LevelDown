import test, { Test } from 'tape';
import levelup, { LevelUp } from 'levelup';
import { ErrorCallback } from 'abstract-leveldown';

import { DynamoDB } from 'aws-sdk';
import { DynamoDbDownFactory } from '../dist/index';
import { DynamoDbDown } from '../dist/lib/dynamoDbDown';

const suite = require('abstract-leveldown/test');

const DynamoDbOptions: DynamoDB.ClientConfiguration = {
  region: 'us-east-1',
  accessKeyId: 'abc',
  secretAccessKey: '123',
  paramValidation: false,
  endpoint: 'http://localhost:4567'
};
const dynamoDb = new DynamoDB(DynamoDbOptions);
const dynamoDownFactory = DynamoDbDownFactory(dynamoDb);

const leveldown = (location: string) => {
  const dynamoDown = dynamoDownFactory(location);
  return dynamoDown;
};

const destroyer = (location: string, cb: ErrorCallback) => {
  dynamoDownFactory.destroy(location, cb);
};

const createTestOptions = () => {
  var dbIdx = 0;
  const factory = () => leveldown(location());
  const location = () => `test-table${dbIdx++}`;
  const lastLocation = () => `test-table${dbIdx}`;
  const setUp = (t: Test) => t.end();
  const tearDown = (t: Test) => t.end();
  const dbSupportTestOptions = {
    bufferKeys: true,
    clear: false,
    createIfMissing: true,
    errorIfExists: true,
    seek: true,
    snapshots: true
  };
  return {
    location,
    lastLocation,
    /* REQUIRED BELOW, OPTIONAL ABOVE */
    setUp,
    tearDown,
    factory,
    test,
    ...dbSupportTestOptions
  };
};

/*
 * Run select `leveldown` tests
 */
test('destroyer', t => {
  t.test('setup', t => {
    leveldown('tempbase');
    t.end();
  });

  t.test('destroy without opening', t => {
    destroyer('tempbase', e => {
      t.notOk(e, 'no error');
      t.end();
    });
  });

  t.test('destroy without existing', t => {
    destroyer('tempbase2', e => {
      t.ok(e, 'got error');
      t.equals((e || {}).message, 'NotFound', 'got NotFound error');
      t.end();
    });
  });

  t.test('destroy offline', t => {
    const dbl = 'offlineBase';
    const ddb = new DynamoDB({ ...DynamoDbOptions, endpoint: 'http://invalid:666' });
    const ddf = DynamoDbDownFactory(ddb);
    ddf(dbl);
    ddf.destroy(dbl, e => {
      t.ok(e, 'got error');
      t.ok(/Inaccessible host/.test((e || {}).message || ''), 'got connection error');
      t.end();
    });
  });

  t.end();
});

test('leveldown', t => {
  let db: DynamoDbDown;

  t.test('setup', t => {
    db = leveldown('foobase');
    t.end();
  });

  t.test('alternate ProvisionedThroughput', t => {
    const ProvisionedThroughput = {
      ReadCapacityUnits: 5,
      WriteCapacityUnits: 5
    };
    db.open({ dynamoOptions: { ProvisionedThroughput } }, function(err) {
      t.notOk(err);
      db.close(function(err) {
        t.notOk(err);
        t.end();
      });
    });
  });

  t.test('tearDown', t => destroyer('foobase', e => t.end(e)));

  t.end();
});

/*
 *   Run select `levelup` tests
 */
test('levelup', t => {
  let db: LevelUp<DynamoDbDown>;

  t.test('setup', t => {
    db = levelup(dynamoDownFactory('foobase'));
    t.end();
  });

  t.test('put string', t => {
    db.put('name', 'LevelUP string', function(err) {
      t.notOk(err);
      db.get('name', { asBuffer: false }, function(err, value) {
        t.notOk(err);
        t.equal(value, 'LevelUP string');
        t.end();
      });
    });
  });

  t.test('put binary', t => {
    const buffer = Buffer.from('testbuffer');
    db.put('binary', buffer, function(err) {
      t.notOk(err);
      db.get('binary', { encoding: 'binary' }, function(err, value) {
        t.notOk(err);
        t.deepEqual(value, buffer);
        t.end();
      });
    });
  });

  t.test('tearDown', t => destroyer('foobase', e => t.end(e)));

  t.test('setup', t => {
    db = levelup(dynamoDownFactory('foobase'), { valueEncoding: 'json' });
    t.end();
  });

  t.test('put object', t => {
    const object = {
      foo: 'bar',
      baz: 123,
      qux: true,
      corge: [1, 2, 3, 4, 5],
      grault: {
        foo: 'bar',
        baz: 123,
        qux: true,
        corge: [1, 2, 3, 4, 5]
      }
    };
    db.put('object', object, { valueEncoding: 'json' }, function(err) {
      t.notOk(err);
      db.get('object', { asBuffer: false }, function(err, value) {
        t.notOk(err);
        t.deepEqual(value, object);
        t.end();
      });
    });
  });

  t.test('tearDown', t => destroyer('foobase', e => t.end(e)));

  t.end();
});

/*
 *   Run all `abstract-leveldown` tests according to `dbSupportTestOptions`
 */
const options = createTestOptions();
// require('abstract-leveldown/test/clear-test').all(options.test, options);
suite(options);
