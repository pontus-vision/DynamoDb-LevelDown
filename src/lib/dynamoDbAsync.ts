import { promisify } from 'util';
import { DynamoDB } from 'aws-sdk';
import { WaiterConfiguration } from 'aws-sdk/lib/service';

import { serialize, dataFromItem, rangeKeyFrom } from './utils';
import { BatchItem, Keys, DynamoBillingMode } from './types';

const MAX_BATCH_SIZE = 25;
const RESOURCE_WAITER_DELAY = 1;
const defaultProvisionedThroughput = {
  ReadCapacityUnits: 5,
  WriteCapacityUnits: 5
};

export class DynamoDbAsync {
  private queryAsync: (params: DynamoDB.Types.QueryInput) => Promise<DynamoDB.Types.QueryOutput>;
  private waitForAsync: (
    state: 'tableExists' | 'tableNotExists',
    params: DynamoDB.Types.DescribeTableInput & { $waiter?: WaiterConfiguration }
  ) => Promise<DynamoDB.Types.DescribeTableOutput>;
  private getItemAsync: (params: DynamoDB.Types.GetItemInput) => Promise<DynamoDB.Types.GetItemOutput>;
  private putItemAsync: (params: DynamoDB.Types.PutItemInput) => Promise<DynamoDB.Types.PutItemOutput>;
  private deleteItemAsync: (params: DynamoDB.Types.DeleteItemInput) => Promise<DynamoDB.Types.DeleteItemOutput>;
  private createTableAsync: (params: DynamoDB.Types.CreateTableInput) => Promise<DynamoDB.Types.CreateTableOutput>;
  private deleteTableAsync: (params: DynamoDB.Types.DeleteTableInput) => Promise<DynamoDB.Types.DeleteTableOutput>;
  private describeTableAsync: (
    params: DynamoDB.Types.DescribeTableInput
  ) => Promise<DynamoDB.Types.DescribeTableOutput>;
  private batchWriteItemAsync: (
    params: DynamoDB.Types.BatchWriteItemInput
  ) => Promise<DynamoDB.Types.BatchWriteItemOutput>;

  constructor(
    private dynamoDb: DynamoDB,
    private tableName: string,
    private hashKey: string,
    private useConsistency: boolean,
    private billingMode: DynamoBillingMode
  ) {
    this.queryAsync = promisify(this.dynamoDb.query).bind(this.dynamoDb);
    // @ts-ignore - Possible override detection issue with AWS types
    this.waitForAsync = promisify(this.dynamoDb.waitFor).bind(this.dynamoDb);
    this.getItemAsync = promisify(this.dynamoDb.getItem).bind(this.dynamoDb);
    this.putItemAsync = promisify(this.dynamoDb.putItem).bind(this.dynamoDb);
    this.deleteItemAsync = promisify(this.dynamoDb.deleteItem).bind(this.dynamoDb);
    this.createTableAsync = promisify(this.dynamoDb.createTable).bind(this.dynamoDb);
    this.deleteTableAsync = promisify(this.dynamoDb.deleteTable).bind(this.dynamoDb);
    this.describeTableAsync = promisify(this.dynamoDb.describeTable).bind(this.dynamoDb);
    this.batchWriteItemAsync = promisify(this.dynamoDb.batchWriteItem).bind(this.dynamoDb);
  }

  private itemKey(key: string): { Key: DynamoDB.Key } {
    return {
      Key: {
        [Keys.HASH_KEY]: { S: this.hashKey },
        [Keys.RANGE_KEY]: { S: String(key) }
      }
    };
  }

  private queryItem(key: string): DynamoDB.GetItemInput {
    return {
      TableName: this.tableName,
      ...this.itemKey(key),
      ConsistentRead: this.useConsistency
    };
  }

  private dataItem(key: string, value: any) {
    return {
      Item: {
        ...this.itemKey(key).Key,
        [Keys.DATA_KEY]: serialize(value)
      }
    };
  }

  private dataTableItem(key: string, value: any): DynamoDB.PutItemInput {
    return {
      TableName: this.tableName,
      ...this.dataItem(key, value)
    };
  }

  async get(key: string) {
    const record = await this.getItemAsync(this.queryItem(key));
    if (!record || !record.Item) throw new Error('NotFound');
    return dataFromItem(record.Item);
  }

  async put(key: string, value: any) {
    const item = this.dataTableItem(key, value);
    return this.putItemAsync(item);
  }

  async batch(array: ReadonlyArray<BatchItem>) {
    const ops: DynamoDB.WriteRequests = [];
    const opKeys: { [key: string]: boolean } = {};
    array.forEach(item => {
      if (opKeys[item.key]) {
        const idx = ops.findIndex(someItem => rangeKeyFrom(someItem) === item.key);
        ops.splice(idx, 1); // De-dupe
      }

      opKeys[item.key] = true;
      ops.push(
        item.type === 'del'
          ? { DeleteRequest: this.itemKey(item.key) }
          : { PutRequest: this.dataItem(item.key, item.value) }
      );
    });

    const params: DynamoDB.Types.BatchWriteItemInput = { RequestItems: {} };
    while (ops.length > 0) {
      params.RequestItems[this.tableName] = ops.splice(0, MAX_BATCH_SIZE);
      const response = await this.batchWriteItemAsync(params);
      if (response && response.UnprocessedItems && response.UnprocessedItems[this.tableName]) {
        ops.unshift(...response.UnprocessedItems[this.tableName]);
      }
    }
  }

  async query(params: Pick<DynamoDB.QueryInput, any>) {
    return this.queryAsync({
      TableName: this.tableName,
      ...params,
      ConsistentRead: this.useConsistency
    });
  }

  async delete(key: string) {
    await this.deleteItemAsync(this.queryItem(key));
  }

  async tableExists(): Promise<boolean> {
    const params = { TableName: this.tableName };
    try {
      await this.describeTableAsync(params);
    } catch (e) {
      return false;
    }
    return true;
  }

  async createTable(throughput: DynamoDB.ProvisionedThroughput = defaultProvisionedThroughput): Promise<boolean> {
    await this.createTableAsync({
      TableName: this.tableName,
      AttributeDefinitions: [
        { AttributeName: Keys.HASH_KEY, AttributeType: 'S' },
        { AttributeName: Keys.RANGE_KEY, AttributeType: 'S' }
      ],
      KeySchema: [
        { AttributeName: Keys.HASH_KEY, KeyType: 'HASH' },
        { AttributeName: Keys.RANGE_KEY, KeyType: 'RANGE' }
      ],
      BillingMode: this.billingMode,
      ProvisionedThroughput: this.billingMode == DynamoBillingMode.PROVISIONED ? throughput : undefined
    });
    await this.waitForAsync('tableExists', {
      TableName: this.tableName,
      $waiter: { delay: RESOURCE_WAITER_DELAY }
    });

    return true;
  }

  async deleteTable(): Promise<boolean> {
    await this.deleteTableAsync({ TableName: this.tableName });
    await this.waitForAsync('tableNotExists', {
      TableName: this.tableName,
      $waiter: { delay: RESOURCE_WAITER_DELAY }
    });

    return true;
  }
}
