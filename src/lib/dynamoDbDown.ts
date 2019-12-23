import { DynamoDB, S3 } from 'aws-sdk';
import {
  AbstractLevelDOWN,
  AbstractOpenOptions,
  ErrorCallback,
  AbstractOptions,
  AbstractGetOptions,
  ErrorValueCallback,
  AbstractBatch,
  AbstractIteratorOptions,
  AbstractIterator,
  PutBatch,
  DelBatch
} from 'abstract-leveldown';
import supports, { SupportManifest } from 'level-supports';

import { DynamoDbIterator } from './iterator';
import { DynamoDbAsync } from './dynamoDbAsync';
import * as DynamoTypes from './types';
import { isBuffer } from './utils';
import { S3Async } from './s3Async';
import { DynamoS3 } from './dynamoS3';

const manifest: SupportManifest = {
  bufferKeys: true,
  snapshots: true,
  permanence: true,
  seek: true,
  clear: true,
  status: true,
  createIfMissing: true,
  errorIfExists: true,
  deferredOpen: true,
  openCallback: true,
  promises: true,
  streams: true,
  encodings: true
};

const globalStore: { [location: string]: DynamoDbDown } = {};

export class DynamoDbDown extends AbstractLevelDOWN {
  private hashKey: string;
  private tableName: string;
  private s3Async: S3Async;
  private dynamoDbAsync: DynamoDbAsync;
  private s3AttachmentDefs: DynamoDbDown.Types.AttachmentDefinition[];

  constructor(dynamoDb: DynamoDB, location: string, options?: DynamoDbDown.Types.Options) {
    super(location);

    const billingMode = options?.billingMode || DynamoTypes.BillingMode.PAY_PER_REQUEST;
    const useConsistency = options?.useConsistency === true;
    const tableHash = location.split('$');

    this.tableName = tableHash[0];
    this.hashKey = tableHash[1] || '!';
    this.s3AttachmentDefs = options?.s3?.attachments || [];
    this.s3Async = new S3Async(options?.s3?.client as S3, this.tableName);
    this.dynamoDbAsync = new DynamoDbAsync(dynamoDb, this.tableName, this.hashKey, useConsistency, billingMode);
  }

  static factory(dynamoDb: DynamoDB, options?: DynamoDbDown.Types.Options) {
    const func = function(location: string) {
      globalStore[location] = globalStore[location] || new DynamoDbDown(dynamoDb, location, options);
      return globalStore[location];
    };
    func.destroy = async function(location: string, cb: ErrorCallback) {
      const store = globalStore[location];
      if (!store) return cb(new Error('NotFound'));

      try {
        await store.deleteTable();
        Reflect.deleteProperty(globalStore, location);
        return cb(undefined);
      } catch (e) {
        if (e && e.code === 'ResourceNotFoundException') {
          Reflect.deleteProperty(globalStore, location);
          return cb(undefined);
        }
        return cb(e);
      }
    };
    return func;
  }

  readonly supports = supports(manifest);

  async _close(cb: ErrorCallback) {
    cb(undefined);
  }

  async _open(options: AbstractOpenOptions, cb: ErrorCallback) {
    const dynamoOptions = options.dynamoOptions || {};

    try {
      let { dynamoTableExists, s3BucketExists } = await Promise.all([
        this.dynamoDbAsync.tableExists(),
        this.s3Async.bucketExists()
      ]).then(r => ({ dynamoTableExists: r.shift(), s3BucketExists: r.shift() }));

      if (options.createIfMissing !== false) {
        const results = await Promise.all([
          dynamoTableExists
            ? Promise.resolve(true)
            : this.dynamoDbAsync.createTable(dynamoOptions.ProvisionedThroughput),
          s3BucketExists ? Promise.resolve(true) : this.s3Async.createBucket()
        ]).then(r => ({ dynamoTableExists: r.shift(), s3BucketExists: r.shift() }));
        dynamoTableExists = results.dynamoTableExists;
        s3BucketExists = results.s3BucketExists;
      }

      if ((dynamoTableExists || s3BucketExists) && options.errorIfExists === true) {
        throw new Error('Underlying storage already exists!');
      }
      if ((!dynamoTableExists || !s3BucketExists) && options.createIfMissing === false) {
        throw new Error('Underlying storage does not exist!');
      }
      cb(undefined);
    } catch (e) {
      cb(e);
    }
  }

  async _put(key: any, value: any, options: AbstractOptions, cb: ErrorCallback) {
    try {
      const newValues = await DynamoS3.syncS3(
        [{ key, value }],
        this.dynamoDbAsync,
        this.s3Async,
        this.s3AttachmentDefs
      );
      await this.dynamoDbAsync.put(key, newValues[0]);
      cb(undefined);
    } catch (e) {
      cb(e);
    }
  }

  async _get(key: any, options: AbstractGetOptions, cb: ErrorValueCallback<any>) {
    try {
      let output = await this.dynamoDbAsync.get(key);
      output = await DynamoS3.maybeRestore(key, output, this.s3Async, this.s3AttachmentDefs);
      const asBuffer = options.asBuffer !== false;
      if (asBuffer) {
        output = isBuffer(output) ? output : Buffer.from(String(output));
      }
      cb(undefined, output);
    } catch (e) {
      cb(e, undefined);
    }
  }

  async _del(key: any, options: AbstractOptions, cb: ErrorCallback) {
    try {
      await DynamoS3.maybeDelete([key], this.dynamoDbAsync, this.s3Async);
      await this.dynamoDbAsync.delete(key);
      cb(undefined);
    } catch (e) {
      cb(e);
    }
  }

  async _batch(array: ReadonlyArray<AbstractBatch<any, any>>, options: AbstractOptions, cb: ErrorCallback) {
    try {
      const ops = array.reduce(
        (p, c) => ({
          puts: c.type === 'put' ? p.puts.concat(c) : p.puts,
          dels: c.type === 'del' ? p.dels.concat(c) : p.dels
        }),
        { puts: new Array<PutBatch>(), dels: new Array<DelBatch>() }
      );

      const delKeys = ops.dels.map(d => d.key);
      await Promise.all([
        DynamoS3.maybeDelete(delKeys, this.dynamoDbAsync, this.s3Async),
        DynamoS3.syncS3(ops.puts, this.dynamoDbAsync, this.s3Async, this.s3AttachmentDefs)
      ]);
      await this.dynamoDbAsync.batch((ops.puts as DynamoTypes.BatchItem[]).concat(ops.dels));
      cb(undefined);
    } catch (e) {
      cb(e);
    }
  }

  _iterator(options: AbstractIteratorOptions<any>): AbstractIterator<any, any> {
    return new DynamoDbIterator(this, this.dynamoDbAsync, this.hashKey, options);
  }

  async deleteTable() {
    return Promise.all([this.dynamoDbAsync.deleteTable(), this.s3Async.deleteBucket()]).then(r => r[0] && r[1]);
  }
}

/* istanbul ignore next */
export namespace DynamoDbDown {
  export import Types = DynamoTypes;
}
