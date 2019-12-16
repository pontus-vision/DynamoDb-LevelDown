import { DynamoDB } from 'aws-sdk';
import { AbstractIteratorOptions } from 'abstract-leveldown';

export type LevelKey = string;
export enum DynamoBillingMode {
  'PROVISIONED' = 'PROVISIONED',
  'PAY_PER_REQUEST' = 'PAY_PER_REQUEST'
}

export interface DynamoDbDownOptions {
  useConsistency?: boolean;
  billingMode?: DynamoBillingMode;
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
