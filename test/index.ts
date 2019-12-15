import levelup, { LevelUp } from 'levelup';
import test, { Test } from 'tape';

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

const leveldown = (location: string) => {
  const dynamoDb = new DynamoDB(DynamoDbOptions);
  const dynamoDown = DynamoDbDownFactory(dynamoDb)(location);
  return dynamoDown;
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

  t.test('tearDown', t => t.end());

  t.end();
});

/*
 *   Run select `levelup` tests
 */
test('levelup', t => {
  let db: LevelUp<DynamoDbDown>;

  t.test('setup', t => {
    const dynamoDb = new DynamoDB(DynamoDbOptions);
    const dynamoDown = DynamoDbDownFactory(dynamoDb);
    db = levelup(dynamoDown('foobase'));
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

  t.test('tearDown', t => t.end());

  t.test('setup', t => {
    const dynamoDb = new DynamoDB(DynamoDbOptions);
    const dynamoDown = DynamoDbDownFactory(dynamoDb);
    db = levelup(dynamoDown('foobase'), { valueEncoding: 'json' });
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

  t.test('tearDown', t => t.end());

  t.end();
});

/*
 *   Run all `abstract-leveldown` tests according to `dbSupportTestOptions`
 */
const options = createTestOptions();
// require('abstract-leveldown/test/clear-test').all(options.test, options);
suite(options);
