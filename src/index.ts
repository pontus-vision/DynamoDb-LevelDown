import { DynamoDB } from 'aws-sdk';
import { DynamoDbDown } from './lib/dynamoDbDown';
import { ErrorCallback } from 'abstract-leveldown';

const globalStore: { [location: string]: DynamoDbDown } = {};

function factoryProvider(dynamoDb: DynamoDB) {
  const func = function(location: string) {
    const instance = new DynamoDbDown(dynamoDb, location);
    globalStore[location] = instance;
    return instance;
  };

  func.destroy = async function(location: string, cb: ErrorCallback) {
    const store = globalStore[location];
    if (!store) return cb(new Error('NotFound'));

    try {
      await store.deleteTable();
    } catch (e) {
      if (e && e.code !== 'ResourceNotFoundException') {
        return cb(e);
      }
    }

    Reflect.deleteProperty(globalStore, location);
    return cb(undefined);
  };

  return func;
}

export default factoryProvider;
export { factoryProvider as DynamoDbDown };
