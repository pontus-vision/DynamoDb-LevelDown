import { S3Async } from './s3Async';
import { DynamoDbAsync } from './dynamoDbAsync';
import { AttachmentDefinition, S3Pointers } from './types';
import { extractS3Pointers, restoreAttachments, extractAttachments } from './utils';

export class DynamoS3 {
  static async maybeSave(key: any, value: any, s3Async: S3Async, s3AttachmentDefs: AttachmentDefinition[]) {
    const savable = extractAttachments(key, value, s3AttachmentDefs);
    if (savable.attachments.length > 0) {
      await s3Async.putObjectBatch(...savable.attachments);
    }
    return savable.newValue;
  }

  static async maybeDelete(key: any, dynamoDbAsync: DynamoDbAsync, s3Async: S3Async) {
    let output: any;
    try {
      output = await dynamoDbAsync.get(key);
    } catch (e) {
      if (e.message !== 'NotFound') {
        throw e;
      }
    }
    if (!!output) {
      await DynamoS3.withAttachmentKeys(key, output, async keys => {
        if (keys.length > 0) {
          return s3Async.deleteObjectBatch(...keys);
        }
        return Promise.resolve();
      });
    }
  }

  static async maybeRestore(key: any, value: any, s3Async: S3Async, s3AttachmentDefs: AttachmentDefinition[]) {
    return DynamoS3.withAttachmentKeys(key, value, async (keys, pointers) => {
      if (keys.length > 0) {
        return await s3Async
          .getObjectBatch(...keys)
          .then(attachments => restoreAttachments(value, pointers, attachments, s3AttachmentDefs));
      }
      return Promise.resolve(value);
    });
  }

  private static async withAttachmentKeys<T>(
    key: any,
    value: any,
    cb: (keys: string[], pointers: S3Pointers) => Promise<T>
  ): Promise<T> {
    const pointers = extractS3Pointers(key, value);
    return cb(
      Object.values(extractS3Pointers(key, value)).map(p => p._s3key),
      pointers
    );
  }
}
