import test, { Test } from 'tape';
import { LevelUp } from 'levelup';
const levelup = require('levelup');
import { ErrorCallback } from 'abstract-leveldown';

import { DynamoDB } from 'aws-sdk';
import { DynamoDbDown } from '../src/index';

const suiteLevelSupports = require('level-supports/test');
const suiteLevelDown = require('abstract-leveldown/test');

const DynamoDbOptions: DynamoDB.ClientConfiguration = {
  region: 'us-east-1',
  accessKeyId: 'abc',
  secretAccessKey: '123',
  paramValidation: false,
  endpoint: `http://localhost:${process.env.DYNAMODB_PORT}`,
};

const dynamoDb = new DynamoDB(DynamoDbOptions);
const dynamoDownFactory = DynamoDbDown.factory(dynamoDb);

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
  return {
    location,
    lastLocation,
    /* REQUIRED BELOW, OPTIONAL ABOVE */
    setUp,
    tearDown,
    factory,
    test,
  };
};

/*
 * Long-running offline tests
 */
test('offline long-running tests', (t) => {
  t.test('destroy offline', (t) => {
    const ddc: DynamoDB.ClientConfiguration = {
      ...DynamoDbOptions,
      endpoint: 'http://invalid:666',
      maxRetries: 0,
      retryDelayOptions: { base: 0, customBackoff: () => 0 },
      httpOptions: { connectTimeout: 250, timeout: 250 },
    };
    const dbl = 'offlineBase';
    const ddb = new DynamoDB(ddc);
    const ddf = DynamoDbDown.factory(ddb);
    ddf(dbl);
    ddf.destroy(dbl, (e) => {
      t.ok(e, 'got error');
      t.ok(/Socket timed out without establishing a connection/.test((e || {}).message || ''), 'got connection error');
      t.end();
    });
  });
});

/*
 * Run select `leveldown` tests
 */
test('destroyer', (t) => {
  t.test('setup', (t) => {
    leveldown('tempbase');
    t.end();
  });

  t.test('destroy without opening', (t) => {
    destroyer('tempbase', (e) => {
      // t.equal(e,'');
      t.ok(e, 'got error');
      t.ok(/Cannot do operations on a non-existent table/.test((e || {}).message || ''), 'got connection error');

      t.end();
    });
  });

  t.test('destroy without existing', (t) => {
    destroyer('tempbase2', (e) => {
      t.ok(e, 'got error');
      t.equals((e || {}).message, 'NotFound', 'got NotFound error');
      t.end();
    });
  });

  t.end();
});

test('factory options', (t) => {
  t.test('default provisioning', (t) => {
    const dynamoDown = DynamoDbDown.factory(dynamoDb, {
      billingMode: DynamoDbDown.Types.BillingMode.PROVISIONED,
    });
    const db = dynamoDown('fake_news');
    db.open((e) => {
      t.notOk(e, 'creates/opens with default provisioning');
      t.end();
    });
  });

  t.end();
});

test('leveldown', (t) => {
  let db: DynamoDbDown;

  t.test('setup', (t) => {
    db = leveldown('foobase42');
    t.end();
  });

  t.test('alternate ProvisionedThroughput', (t) => {
    const ProvisionedThroughput = {
      ReadCapacityUnits: 5,
      WriteCapacityUnits: 5,
    };
    db.open({ dynamoOptions: { ProvisionedThroughput } }, (e) => {
      t.notOk(e);
      db.close((e) => {
        t.notOk(e);
        t.end();
      });
    });
  });

  t.test('batch and iterate objects', (t) => {
    db.open((e) => {
      t.notOk(e);

      const valueObject = { short: 'and stout' };
      db.batch(
        [
          { type: 'put', key: 'foo', value: { iam: 'a little teapot' } },
          { type: 'put', key: 'bar', value: valueObject },
        ],
        (e) => {
          t.notOk(e);

          const iterator = db.iterator({ keyAsBuffer: false, valueAsBuffer: false });
          iterator.next((e, k, v) => {
            t.notOk(e);
            t.ok(k, 'got object key');
            t.equal(k, 'bar', 'got same object key');
            t.ok(v, 'got object value');
            t.deepEqual(v, valueObject, 'got same object value');

            db.close((e) => {
              t.notOk(e);
              t.end();
            });
          });
        }
      );
    });
  });

  t.test('underlying errors', (t) => {
    db.open((e) => {
      t.notOk(e);

      let callCount = 0;
      const anyDb = db as any;
      anyDb.dynamoDbAsync.put = async () => {
        throw new Error('Forced Put Error');
      };
      anyDb.dynamoDbAsync.delete = async () => {
        throw new Error('Forced Delete Error');
      };
      anyDb.dynamoDbAsync.batch = async () => {
        throw new Error('Forced Batch Error');
      };
      const oldQuery = anyDb.dynamoDbAsync.query;
      anyDb.dynamoDbAsync.query = async (params: any) => {
        callCount++;
        switch (callCount) {
          case 1:
            return { Items: undefined };
          case 2:
            throw Object.assign(new Error('ResourceNotFoundException'), { code: 'ResourceNotFoundException' });
          case 3:
            throw new Error('Forced Query Error');
          default:
            return oldQuery(params);
        }
      };

      db.put('foo', 'bar', (e) => {
        t.ok(e, 'put handles error');
        db.del('foo', (e) => {
          t.ok(e, 'del handles error');
          db.batch([{ type: 'put', key: 'foo', value: 'bar' }], (e) => {
            t.ok(e, 'batch handles error');
            db.iterator().next((e) => {
              t.notOk(e, 'iterator handles `undefined` items');
              db.iterator().next((e) => {
                t.notOk(e, 'iterator handles `ResourceNotFoundException`');
                db.iterator().next((e) => {
                  t.ok(e, 'iterator handles general error');
                  db.iterator({ gte: 5, lte: 2, reverse: false }).next((e, k, v) => {
                    t.notOk(e, 'iterator handles general error');
                    t.notOk(k, 'does not yield a key');
                    t.notOk(k, 'does not yield a value');

                    db.close((e) => {
                      t.notOk(e);
                      t.end();
                    });
                  });
                });
              });
            });
          });
        });
      });
    });
  });

  t.test('tearDown', (t) => destroyer('foobase42', (e) => t.end(e)));

  t.end();
});

test('really deep error handling', (t) => {
  const db = leveldown('foobase42');
  db.open((e) => {
    t.notOk(e);

    let callCount = 0;
    const anyDb = db as any;
    anyDb.dynamoDbAsync.batchWriteItemAsync = async (params: any) => {
      callCount++;
      if (callCount === 1) {
        const tableName = Object.keys(params.RequestItems).shift() as string;
        return {
          UnprocessedItems: {
            [tableName]: params.RequestItems[tableName],
          },
        };
      } else return {};
    };

    db.batch(
      [
        { type: 'put', key: 'foo', value: 'bar' },
        { type: 'put', key: 'fiz', value: 'gig' },
      ],
      (e) => {
        t.notOk(e);
        db.close((e) => {
          t.notOk(e);
          t.equal(callCount, 2, 'unprocessed items retried');
          t.end();
        });
      }
    );
  });
});

/*
 *   Run select `levelup` tests
 */
test('levelup', (t) => {
  let db: LevelUp<DynamoDbDown>;

  t.test('setup', (t) => {
    db = levelup(dynamoDownFactory('foobase'));
    t.end();
  });

  t.test('put string', (t) => {
    db.put('name', 'LevelUP string', function (err) {
      t.notOk(err);
      db.get('name', { asBuffer: false }, function (err, value) {
        t.notOk(err);
        t.equal(value, 'LevelUP string');
        t.end();
      });
    });
  });

  t.test('put binary', (t) => {
    const buffer = Buffer.from('testbuffer');
    db.put('binary', buffer, function (err) {
      t.notOk(err);
      db.get('binary', { encoding: 'binary' }, function (err, value) {
        t.notOk(err);
        t.deepEqual(value, buffer);
        t.end();
      });
    });
  });

  t.test('tearDown', (t) => destroyer('foobase', (e) => t.end(e)));

  t.test('setup', (t) => {
    db = levelup(dynamoDownFactory('foobase'), { valueEncoding: 'json' });
    t.end();
  });

  t.test('put object', (t) => {
    const object = {
      foo: 'bar',
      baz: 123,
      qux: true,
      corge: [1, 2, 3, 4, 5],
      grault: {
        foo: 'bar',
        baz: 123,
        qux: true,
        corge: [1, 2, 3, 4, 5],
      },
    };
    db.put('object', object, { valueEncoding: 'json' }, function (err) {
      t.notOk(err);
      db.get('object', { asBuffer: false }, function (err, value) {
        t.notOk(err);
        t.deepEqual(value, object);
        t.end();
      });
    });
  });

  t.test('tearDown', (t) => destroyer('foobase', (e) => t.end(e)));

  t.end();
});

/*
 *   Run all `abstract-leveldown` tests according to `dbSupportTestOptions`
 */
const options = createTestOptions();
suiteLevelSupports(test, options);
suiteLevelDown(options);
