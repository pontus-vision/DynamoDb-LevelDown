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

import { DynamoDbIterator } from './iterator';
import { DynamoDbAsync } from './dynamoDbAsync';
import { isBuffer } from './utils';

export class DynamoDbDown extends AbstractLevelDOWN {
  private hashKey: string;
  private tableName: string;
  private dynamoDb: DynamoDB;
  private dynamoDbAsync: DynamoDbAsync;

  constructor(dynamoDb: DynamoDB, location: string) {
    super(location);

    const tableHash = location.split('$');
    this.tableName = tableHash[0];
    this.hashKey = tableHash[1] || '!';
    this.dynamoDb = dynamoDb;
    this.dynamoDbAsync = new DynamoDbAsync(this.dynamoDb, this.tableName, this.hashKey);
  }

  async _close(cb: ErrorCallback) {
    if (cb) cb(undefined);
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

  _iterator(options?: AbstractIteratorOptions<any>): AbstractIterator<any, any> {
    return new DynamoDbIterator(this, this.dynamoDbAsync, this.hashKey, options);
  }

  async deleteTable() {
    return this.dynamoDbAsync.deleteTable();
  }
}
