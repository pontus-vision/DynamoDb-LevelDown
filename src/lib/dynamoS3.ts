import { S3Async } from './s3Async';
import { DynamoDbAsync } from './dynamoDbAsync';
import { extractS3Pointers, restoreAttachments, extractAttachments, dataFromItem } from './utils';
import { AttachmentDefinition, AttachmentResult, SimpleItem, S3PointerBatch } from './types';

/* @internal */
export class DynamoS3 {
  static async syncS3(
    items: SimpleItem[],
    dynamoDbAsync: DynamoDbAsync,
    s3Async: S3Async,
    s3AttachmentDefs: AttachmentDefinition[]
  ) {
    const result = await DynamoS3.savableWithS3DelKeys(items, s3AttachmentDefs, dynamoDbAsync);
    const attachments = result.savables.map(s => s.attachments).reduce((p, c) => [...p, ...c], []);
    if (attachments.length > 0) {
      await Promise.all([s3Async.putObjectBatch(...attachments), s3Async.deleteObjectBatch(...result.delKeys)]);
    }
    return result.savables.map(s => s.newValue);
  }

  static async savableWithS3DelKeys(
    items: SimpleItem[],
    s3AttachmentDefs: AttachmentDefinition[],
    dynamoDbAsync: DynamoDbAsync
  ) {
    const allItems = await dynamoDbAsync.getBatch(items.map(i => i.key)).then(r => {
      const merged = Object.keys(r)
        .map(key => ({ key, value: dataFromItem(r[key]) }))
        .concat(...items.filter(i => !r[i.key]));
      return merged;
    });
    const result = DynamoS3.withAttachmentKeys(allItems, batch => {
      const s3Keys = Object.keys(batch)
        .map(itemKey => ({ itemKey, s3Keys: Object.values(batch[itemKey]).map(p => p._s3key) }))
        .reduce((p, c) => ({ ...p, [c.itemKey]: c.s3Keys }), {} as { [key: string]: string[] });
      return items
        .map(i => {
          const savable = extractAttachments(i.key, i.value, s3AttachmentDefs);
          const nowS3Keys = savable.attachments.map(a => a.key);
          return { savable, delKeys: s3Keys[i.key].filter(k => !nowS3Keys.includes(k)) };
        })
        .reduce((p, c) => ({ savables: p.savables.concat(c.savable), delKeys: p.delKeys.concat(...c.delKeys) }), {
          savables: new Array<AttachmentResult>(),
          delKeys: new Array<string>()
        });
    });
    return result;
  }

  static async maybeDelete(keys: string[], dynamoDbAsync: DynamoDbAsync, s3Async: S3Async) {
    if (keys.length === 0) return;
    let items: SimpleItem[] = await dynamoDbAsync
      .getBatch(keys)
      .then(batch => Object.keys(batch).map(key => ({ key, value: batch[key] })));
    if (items.length > 0) {
      await DynamoS3.withAttachmentKeys(items, async batch => {
        const s3Keys = Object.keys(batch)
          .map(itemKey => Object.values(batch[itemKey]).map(p => p._s3key))
          .reduce((p, c) => p.concat(...c), []);
        if (s3Keys.length > 0) {
          return s3Async.deleteObjectBatch(...s3Keys);
        }
        return Promise.resolve();
      });
    }
  }

  static async maybeRestore(key: any, value: any, s3Async: S3Async, s3AttachmentDefs: AttachmentDefinition[]) {
    return DynamoS3.withAttachmentKeys([{ key, value }], async batch => {
      const s3Keys = Object.values(batch[key]).map(e => e._s3key);
      if (s3Keys.length > 0) {
        return await s3Async
          .getObjectBatch(...s3Keys)
          .then(attachments => restoreAttachments(value, batch[key], attachments, s3AttachmentDefs));
      }
      return Promise.resolve(value);
    });
  }

  private static withAttachmentKeys<T>(items: SimpleItem[], cb: (batch: S3PointerBatch) => T): T {
    return cb(items.reduce((p, c) => ({ ...p, [c.key]: extractS3Pointers(c.key, c.value) }), {} as S3PointerBatch));
  }
}
