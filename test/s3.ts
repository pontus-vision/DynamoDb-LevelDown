import test from 'tape';
import { DynamoDB, S3 } from 'aws-sdk';
import { DynamoDbDown } from '../src/index';
import { extractAttachments, extractS3Pointers } from '../src/lib/utils';

const AWSOptions = { region: 'us-east-1', accessKeyId: 'abc', secretAccessKey: '123', paramValidation: false };
const DynamoDbOptions: DynamoDB.ClientConfiguration = {
  ...AWSOptions,
  endpoint: `http://localhost:${process.env.DYNAMODB_PORT}`
};
const S3Options: S3.ClientConfiguration = {
  ...AWSOptions,
  endpoint: `http://localhost:${process.env.S3_PORT}`,
  s3ForcePathStyle: true
};

const attachmentDefinitions: DynamoDbDown.Types.AttachmentDefinition[] = [
  {
    match: /.*\/_attachments\/.*/,
    dataKey: 'data',
    contentTypeKey: 'content_type',
    dataEncoding: 'base64'
  }
];

const dynamoDownOptions: DynamoDbDown.Types.Options = {
  s3: {
    client: new S3(S3Options),
    attachments: attachmentDefinitions
  }
};

const dynamoDb = new DynamoDB(DynamoDbOptions);
const dynamoDownFactory = DynamoDbDown.factory(dynamoDb, dynamoDownOptions);

const leveldown = (location: string) => {
  const dynamoDown = dynamoDownFactory(location);
  return dynamoDown;
};

/*
 * Run select `S3` tests
 */
test('extract PouchDB attachments', t => {
  const input = {
    _id: 'meowth',
    _rev: '1-e8a84187bb4e671f27ec11bdf7320aaa',
    _attachments: {
      'meowth.png': {
        content_type: 'image/png',
        data:
          'iVBORw0KGgoAAAANSUhEUgAAACgAAAAkCAIAAAB0Xu9BAAAABGdBTUEAALGPC/xhBQAAAuNJREFUWEetmD1WHDEQhDdxRMYlnBFyBIccgdQhKVcgJeQMpE5JSTd2uqnvIGpVUqmm9TPrffD0eLMzUn+qVnXPwiFd/PP6eLh47v7EaazbmxsOxjhTT88z9hV7GoNF1cUCvN7TTPv/gf/+uQPm862MWTL6fff4HfDx4S79/oVAlAUwqOmYR0rnazuFnhfOy/ErMKkcBFOr1vOjUi2MFn4nuMil6OPh5eGANLhW3y6u3aH7ijEDCxgCvzFmimvc95TekZLyMSeJC68Bkw0kqUy1K87FlpGZqsGFCyqEtQNDdFUtFctTiuhnPKNysid/WFEFLE2O102XJdEE+8IgeuGsjeJyGHm/xHvQ3JtKVsGGp85g9rK6xMHtvHO9+WACYjk5vkVM6XQ6OZubCJvTfPicYPeHO2AKFl5NuF5UK1VDUbeLxh2BcRGKTQE3irHm3+vPj6cfCod50Eqv5QxtwBQUGhZhbrGVuRia1B4MNp6edwBxld2sl1splfHCwfsvCZfrCQyWmX10djjOlWJSSy3VQlS6LmfrgNvaieRWx1LZ6s9co+P0DLsy3OdLU3lWRclQsVcHJBcUQ0k9/WVVrmpRzYQzpgAdQcAXxZzUnFX3proannrYH+Vq6KkLi+UkarH09mC8YPr2RMWOlEqFkQClsykGEv7CqCUbXcG8+SaGvJ4a8d4y6epND+pEhxoN0vWUu5ntXlFb5/JT7JfJJqoTdy9u9qc7ax3xJRHqJLADWEl23cFWl4K9fvoaCJ2BHpmJ3s3z+O0U/DmzdMjB9alWZtg4e3yxzPa7lUR7nkvxLHO9+tvJX3mtSDpwX8GajB283I8R8a7D2MhUZr1iNWdny256yYLd52DwRYBtRMvE7rsmtxIUE+zLKQCDO4jlxB6CZ8M17GhuY+XTE8vNhQiIiSE82ZsGwk1pht4ZSpT0YVpon6EvevOXXH8JxVR78QzNuamupW/7UB7wO/+7sG5V4ekXb4cL5Lyv+4IAAAAASUVORK5CYII='
      }
    }
  };
  const result = extractAttachments(input._id, input, attachmentDefinitions);

  t.ok(result, 'got result');
  t.ok(result.newValue, 'got dynamo value');
  t.ok(result.attachments, 'found attachments');
  t.equal(result.attachments.length, 1, 'found right number of attachments');
  t.ok(result.attachments[0], 'attachment is defined');
  t.equal(result.attachments[0].key, `${input._id}/_attachments/meowth.png`, 'correct attachment key');
  t.equal(typeof result.attachments[0].data, 'object', 'correct attachment data type');
  t.true(result.attachments[0].data.length > 0, 'attachment data > 0');
  t.equal(result.attachments[0].contentType, 'image/png', 'correct attachment mime type');

  const s3Pointers = extractS3Pointers(input._id, result.newValue);

  t.ok(s3Pointers, 'found s3 pointers');

  t.end();
});

test('s3', t => {
  let db: DynamoDbDown;
  const object = {
    _id: 'meowth',
    _rev: '1-e8a84187bb4e671f27ec11bdf7320aaa',
    _attachments: {
      'meowth.png': {
        content_type: 'image/png',
        data:
          'iVBORw0KGgoAAAANSUhEUgAAACgAAAAkCAIAAAB0Xu9BAAAABGdBTUEAALGPC/xhBQAAAuNJREFUWEetmD1WHDEQhDdxRMYlnBFyBIccgdQhKVcgJeQMpE5JSTd2uqnvIGpVUqmm9TPrffD0eLMzUn+qVnXPwiFd/PP6eLh47v7EaazbmxsOxjhTT88z9hV7GoNF1cUCvN7TTPv/gf/+uQPm862MWTL6fff4HfDx4S79/oVAlAUwqOmYR0rnazuFnhfOy/ErMKkcBFOr1vOjUi2MFn4nuMil6OPh5eGANLhW3y6u3aH7ijEDCxgCvzFmimvc95TekZLyMSeJC68Bkw0kqUy1K87FlpGZqsGFCyqEtQNDdFUtFctTiuhnPKNysid/WFEFLE2O102XJdEE+8IgeuGsjeJyGHm/xHvQ3JtKVsGGp85g9rK6xMHtvHO9+WACYjk5vkVM6XQ6OZubCJvTfPicYPeHO2AKFl5NuF5UK1VDUbeLxh2BcRGKTQE3irHm3+vPj6cfCod50Eqv5QxtwBQUGhZhbrGVuRia1B4MNp6edwBxld2sl1splfHCwfsvCZfrCQyWmX10djjOlWJSSy3VQlS6LmfrgNvaieRWx1LZ6s9co+P0DLsy3OdLU3lWRclQsVcHJBcUQ0k9/WVVrmpRzYQzpgAdQcAXxZzUnFX3proannrYH+Vq6KkLi+UkarH09mC8YPr2RMWOlEqFkQClsykGEv7CqCUbXcG8+SaGvJ4a8d4y6epND+pEhxoN0vWUu5ntXlFb5/JT7JfJJqoTdy9u9qc7ax3xJRHqJLADWEl23cFWl4K9fvoaCJ2BHpmJ3s3z+O0U/DmzdMjB9alWZtg4e3yxzPa7lUR7nkvxLHO9+tvJX3mtSDpwX8GajB283I8R8a7D2MhUZr1iNWdny256yYLd52DwRYBtRMvE7rsmtxIUE+zLKQCDO4jlxB6CZ8M17GhuY+XTE8vNhQiIiSE82ZsGwk1pht4ZSpT0YVpon6EvevOXXH8JxVR78QzNuamupW/7UB7wO/+7sG5V4ekXb4cL5Lyv+4IAAAAASUVORK5CYII='
      }
    }
  };

  t.test('setup', t => {
    db = leveldown('dynamo-s3');
    t.end();
  });

  test('attachment handling', t => {
    db.open(e => {
      t.notOk(e, 'database opened');

      db.put(object._id, object, e => {
        t.notOk(e, 'put object with data');

        db.get(object._id, { asBuffer: false }, (e, v) => {
          t.notOk(e, 'get object with data');
          t.deepEqual(v, object, 'object with restored data');

          t.end();
        });
      });
    });
  });

  t.end();
});
