import { DynamoDB, S3 } from 'aws-sdk';
import { AbstractIteratorOptions } from 'abstract-leveldown';

export enum BillingMode {
  'PROVISIONED' = 'PROVISIONED',
  'PAY_PER_REQUEST' = 'PAY_PER_REQUEST'
}

export interface S3Options {
  client: S3;
  attachments: AttachmentDefinition[];
}

export interface Options {
  useConsistency?: boolean;
  billingMode?: BillingMode;
  s3?: S3Options;
}

export type AttachmentDefinition = {
  match: RegExp;
  contentTypeKey: string;
  dataKey: string;
  dataEncoding?: BufferEncoding;
};

export type LevelKey = string;

export interface IteratorOptions extends AbstractIteratorOptions<any> {
  start?: LevelKey;
  end?: LevelKey;
  lastKey?: DynamoDB.Key;
  inclusive?: boolean;
}

/* @internal */
export interface Attachment {
  key: string;
  contentType: string;
  data: Buffer;
}

/* @internal */
export interface AttachmentResult {
  newValue: any;
  attachments: Attachment[];
}

/* @internal */
export interface S3Pointer {
  _s3key: string;
}

/* @internal */
export type S3Pointers = { [key: string]: S3Pointer };

/* @internal */
export type S3PointerBatch = { [key: string]: S3Pointers };

/* @internal */
export type S3ObjectBatch = { [key: string]: S3.GetObjectOutput };

/* @internal */
export interface KeyedItem {
  readonly key: string;
}

/* @internal */
export interface SimpleItem extends KeyedItem {
  readonly value: any;
}

/* @internal */
export interface BatchItem extends KeyedItem {
  readonly type: 'put' | 'del';
  readonly value?: any;
}

/* @internal */
export type ItemBatch = { [key: string]: any };

/* @internal */
export class Keys {
  static readonly DATA_KEY = 'data';
  static readonly HASH_KEY = 'hash';
  static readonly RANGE_KEY = 'range';
}

/* @internal */
export type ValueTransformer = {
  for: (value: any) => boolean;
  toDb: (value: any) => DynamoDB.AttributeValue;
  fromDb: (value: DynamoDB.AttributeValue) => any;
};
