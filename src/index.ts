import { DynamoDB } from 'aws-sdk';
import { DynamoDbDown } from './lib/dynamoDbDown';

function factoryProvider(dynamoDb: DynamoDB) {
  return function(location: string) {
    return new DynamoDbDown(dynamoDb, location);
  };
}

export default factoryProvider;
export { factoryProvider as DynamoDbDown };
