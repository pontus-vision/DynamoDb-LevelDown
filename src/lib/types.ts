import { DynamoDB, S3 } from 'aws-sdk';
import { AbstractIteratorOptions } from 'abstract-leveldown';

export type LevelKey = string;
export enum BillingMode {
  'PROVISIONED' = 'PROVISIONED',
  'PAY_PER_REQUEST' = 'PAY_PER_REQUEST'
}

export type AttachmentDefinition = {
  match: RegExp;
  contentTypeKey: string;
  dataKey: string;
  dataEncoding?: BufferEncoding;
};

export interface Attachment {
  key: string;
  contentType: string;
  data: Buffer;
}

export interface AttachmentResult {
  newValue: any;
  attachments: Attachment[];
}

export interface S3Pointer {
  _s3key: string;
}

export type S3Pointers = { [key: string]: S3Pointer };

export type S3ObjectBatch = { [key: string]: S3.GetObjectOutput };

export interface S3Options {
  client: S3;
  attachments: AttachmentDefinition[];
}

export interface Options {
  useConsistency?: boolean;
  billingMode?: BillingMode;
  s3?: S3Options;
}

export interface KeyedItem {
  readonly key: string;
}

export interface SimpleItem extends KeyedItem {
  readonly value: any;
}

export interface BatchItem extends KeyedItem {
  readonly type: 'put' | 'del';
  readonly value?: any;
}

export interface IteratorOptions extends AbstractIteratorOptions<any> {
  start?: LevelKey;
  end?: LevelKey;
  lastKey?: DynamoDB.Key;
  inclusive?: boolean;
}

export class Keys {
  static readonly DATA_KEY = 'data';
  static readonly HASH_KEY = 'hash';
  static readonly RANGE_KEY = 'range';
}

export type ValueTransformer = {
  for: (value: any) => boolean;
  toDb: (value: any) => DynamoDB.AttributeValue;
  fromDb: (value: DynamoDB.AttributeValue) => any;
};
