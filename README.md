AWSDOWN
=======

[![Build Status](https://travis-ci.org/KlausTrainer/dynamodbdown.svg?branch=main)](https://travis-ci.org/KlausTrainer/dynamodbdown)
[![semantic-release](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--release-e10079.svg)](https://github.com/semantic-release/semantic-release)

A [LevelDOWN](https://github.com/level/leveldown) API implementation on [Amazon DynamoDB](https://aws.amazon.com/dynamodb/) and [Amazon S3](https://aws.amazon.com/s3/).

This is a drop-in replacement for [LevelDOWN](https://github.com/level/leveldown) that uses [Amazon DynamoDB](https://aws.amazon.com/dynamodb/) for object storage and [Amazon S3](https://aws.amazon.com/s3/) for primitive value storage. It can be used as a backend for [LevelUP](https://github.com/level/levelup) rather than an actual LevelDB store.

As of version 0.7, LevelUP allows you to pass a `db` option when you create a new instance. This will override the default LevelDOWN store with a LevelDOWN API compatible object. DynamoDBDOWN conforms exactly to the LevelDOWN API, but performs operations against a DynamoDB database.

## Why?

This LevelDOWN implementation is different from others such as [DynamoDBDOWN](https://github.com/KlausTrainer/dynamodbdown) in that objects are cast to actual column types in DynamoDB, and primitive values(e.g. string, number, boolean, buffer/blob) are stored in S3; this allows objects to be natively queryable with DynamoDB, and S3 allows us to store data beyond the 400kb maximum record size that DynamoDB imposes.

The intended use case for this library is with PouchDB, where most database activity involves saving objects(i.e. documents) while a few things such as attachments and database info are better suited for a storage engine like S3.  Compatibility with PouchDB is a big win in this case since it provides a common JavaScript interface for interacting with documents as well as *full replication*, including attachments of any size.  Using this LevelDOWN implementation wtih PouchDB can be useful for regular backups as well as migrating data to CouchDB.

## Usage Example ##

```js
const levelup = require('levelup')
const DynamoDBDOWN = require('dynamodbdown')

const dynamoDBOptions = {
  region: 'eu-west-1',
  secretAccessKey: 'abc',
  accessKeyId: '123'
}

const options = {
  db: DynamoDBDOWN,
  dynamodb: dynamoDBOptions // required AWS configuration
}

const db = levelup('tableName', options)

db.put('some string', 'LevelUP string')
db.put('some binary', new Buffer('LevelUP buffer'))

const dbReadStream = db.createReadStream()

dbReadStream.on('data', console.log)
dbReadStream.on('close', () => { console.log('read stream closed') })
```

When running the above example, you should get the following console output:

```
{ key: 'some binary', value: 'LevelUP buffer' }
{ key: 'some string', value: 'LevelUP string' }
read stream closed
```

## Hash Keys ##

In DynamoDB, keys consist of two parts: a *hash key* and a *range key*. To achieve LevelDB-like behaviour, all keys in a database instance are given the same hash key. That means that you can't do range queries over keys with different hash keys.

The default hash key is `!`. You can specify it by putting a `$` in the `location` argument. The `$` separates the table name from the hash key.

### Example ###

```js
const levelup = require('levelup')
const DynamoDBDOWN = require('dynamodbdown')

const dynamoDBOptions = {
  region: 'eu-west-1',
  secretAccessKey: 'abc',
  accessKeyId: '123'
}

const options = {
  db: DynamoDBDOWN,
  dynamodb: dynamoDBOptions // required AWS configuration
}

const db = levelup('tableName$hashKey', options)

db.put('some key', 'some value', => err {
  // the DynamoDB object would now look like this:
  // {
  //   '---hkey': 'hashKey',
  //   '---rkey': 'some key',
  // }
});
```

If you are fine with sharing capacity units across multiple database instances or applications, you can reuse a table by specifying the same table name, but different hash keys.

## Table & Bucket Creation ##

If the table doesn't exist, DynamoDBDOWN will try to create a table.  You can specify the read/write throughput. If not specified, it will default to `1/1`. If the table already exists, the specified throughput will have no effect. Throughput can be changed for tables that already exist by using the DynamoDB API or the AWS Console.

DynamoDBDOWN will also attempt to create an S3 bucket if it doesn't already exist.

See [LevelUP options](https://github.com/level/levelup#options) for more information.

### Example ###

```js
const levelup = require('levelup')
const DynamoDBDOWN = require('dynamodbdown')

const dynamoDBOptions = {
  region: 'eu-west-1',
  secretAccessKey: 'abc',
  accessKeyId: '123',
  ProvisionedThroughput: { // capacity can be specified; defaults to 1/1:
    ReadCapacityUnits: 1,
    WriteCapacityUnits: 1
  }
}

const options = {
  db: DynamoDBDOWN,
  dynamodb: dynamoDBOptions // required AWS configuration
}

const db = levelup('tableName', options)
```

## Table Name Encoding ##

DynamoDBDOWN encodes table names in hexadecimal if you set the `dynamodb.hexEncodeTableName` option to `true`. This can be useful if you'd like pass `location` parameter values to `levelup` that aren't compatible with DynamoDB's restrictions on table names (see [here](docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html)).

### Example ###

```js
const levelup = require('levelup')
const DynamoDBDOWN = require('dynamodbdown')

const dynamoDBOptions = {
  region: 'eu-west-1',
  secretAccessKey: 'abc',
  accessKeyId: '123',
  hexEncodeTableName: true
}

const options = {
  db: DynamoDBDOWN,
  dynamodb: dynamoDBOptions // required AWS configuration
}

const db = levelup('tableName', options) // the DynamoDB table name will
                                         // be '7461626c654e616d65'
```

## Changelog ##

See [here](https://github.com/KlausTrainer/dynamodbdown/releases).

## Acknowledgments ##

DynamoDBDOWN has been heavily inspired by:

* [DynamoDOWN](https://github.com/davidguttman/dynamodown) by David Guttman
* [dynamo-down](https://github.com/jed/dynamo-down) by Jed Schmidt.

## LICENSE ##

Copyright 2016 Klaus Trainer

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
