import { DynamoDB } from 'aws-sdk';
import {
  AbstractLevelDOWN,
  AbstractOpenOptions,
  ErrorCallback,
  AbstractOptions,
  AbstractGetOptions,
  ErrorValueCallback,
  AbstractBatch,
  AbstractIteratorOptions,
  AbstractIterator
} from 'abstract-leveldown';
import supports, { SupportManifest } from 'level-supports';

import { DynamoDbIterator } from './iterator';
import { DynamoDbAsync } from './dynamoDbAsync';
import { DynamoDbDownOptions, DynamoBillingMode } from './types';
import { isBuffer } from './utils';

const manifest: SupportManifest = {
  bufferKeys: true,
  snapshots: true,
  permanence: true,
  seek: true,
  clear: true,

  // Features of abstract-leveldown that levelup doesn't have
  status: true,

  // Features of disk-based implementations
  createIfMissing: true,
  errorIfExists: true,

  // Features of level(up) that abstract-leveldown doesn't have yet
  deferredOpen: true,
  openCallback: true,
  promises: true,
  streams: true,
  encodings: true
};

export class DynamoDbDown extends AbstractLevelDOWN {
  private hashKey: string;
  private tableName: string;
  private dynamoDbAsync: DynamoDbAsync;

  constructor(private dynamoDb: DynamoDB, location: string, options?: DynamoDbDownOptions) {
    super(location);

    const billingMode = options?.billingMode || DynamoBillingMode.PAY_PER_REQUEST;
    const useConsistency = options?.useConsistency || false;
    const tableHash = location.split('$');

    this.tableName = tableHash[0];
    this.hashKey = tableHash[1] || '!';
    this.dynamoDb = dynamoDb;
    this.dynamoDbAsync = new DynamoDbAsync(this.dynamoDb, this.tableName, this.hashKey, useConsistency, billingMode);
  }

  readonly supports = supports(manifest);

  async _close(cb: ErrorCallback) {
    cb(undefined);
  }

  async _open(options: AbstractOpenOptions, cb: ErrorCallback) {
    try {
      const dynamoOptions = options.dynamoOptions || {};
      let tableExists = await this.dynamoDbAsync.tableExists();
      if (!tableExists && options.createIfMissing !== false) {
        tableExists = await this.dynamoDbAsync.createTable(dynamoOptions.ProvisionedThroughput);
      }

      if (tableExists && options.errorIfExists === true) {
        throw new Error('Underlying storage already exists!');
      }
      if (!tableExists && options.createIfMissing === false) {
        throw new Error('Underlying storage does not exist!');
      }
      cb(undefined);
    } catch (e) {
      cb(e);
    }
  }

  async _put(key: any, value: any, options: AbstractOptions, cb: ErrorCallback) {
    try {
      await this.dynamoDbAsync.put(key, value);
      cb(undefined);
    } catch (e) {
      cb(e);
    }
  }

  async _get(key: any, options: AbstractGetOptions, cb: ErrorValueCallback<any>) {
    try {
      let output = await this.dynamoDbAsync.get(key);
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
      await this.dynamoDbAsync.delete(key);
      cb(undefined);
    } catch (e) {
      cb(e);
    }
  }

  async _batch(array: ReadonlyArray<AbstractBatch<any, any>>, options: AbstractOptions, cb: ErrorCallback) {
    try {
      await this.dynamoDbAsync.batch(array);
      cb(undefined);
    } catch (e) {
      cb(e);
    }
  }

  _iterator(options: AbstractIteratorOptions<any>): AbstractIterator<any, any> {
    return new DynamoDbIterator(this, this.dynamoDbAsync, this.hashKey, options);
  }

  async deleteTable() {
    return this.dynamoDbAsync.deleteTable();
  }
}
