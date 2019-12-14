import { DynamoDB } from 'aws-sdk';
import { DynamoDbDown } from './lib/dynamoDbDown';
import { DynamoDbDownOptions } from './lib/types';

function DynamoDbDownFactory(dynamoDb: DynamoDB, options?: DynamoDbDownOptions) {
  return function(location: string) {
    return new DynamoDbDown(dynamoDb, location, options);
  };
}

export default DynamoDbDownFactory;
export { DynamoDbDownOptions, DynamoDbDownFactory, DynamoDbDown };
