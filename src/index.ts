import { DynamoDB } from 'aws-sdk';
import { DynamoDbDown } from './lib/dynamoDbDown';
import { DynamoDbDownOptions } from './lib/types';
import { ErrorCallback } from 'abstract-leveldown';

const globalStore: { [location: string]: DynamoDbDown } = {};

function DynamoDbDownFactory(dynamoDb: DynamoDB, options?: DynamoDbDownOptions) {
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

export default DynamoDbDownFactory;
export { DynamoDbDownOptions, DynamoDbDownFactory, DynamoDbDown };
