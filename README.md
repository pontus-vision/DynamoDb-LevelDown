# DynamoDbDown

[![CircleCI](https://circleci.com/gh/GioCirque/DynamoDb-LevelDown.svg?style=shield)](https://circleci.com/gh/GioCirque/DynamoDb-LevelDown) [![codecov](https://codecov.io/gh/GioCirque/DynamoDb-LevelDown/graph/badge.svg)](https://codecov.io/gh/GioCirque/DynamoDb-LevelDown) [![Vulnerabilities](https://img.shields.io/snyk/vulnerabilities/github/GioCirque/dynamodb-leveldown.svg)](https://www.npmjs.com/package/dynamodb-leveldown) [![NPM version](https://img.shields.io/npm/v/dynamodb-leveldown.svg)](https://www.npmjs.com/package/dynamodb-leveldown) [![NPM license](https://img.shields.io/npm/l/dynamodb-leveldown.svg)](https://www.npmjs.com/package/dynamodb-leveldown) [![Types](https://img.shields.io/npm/types/dynamodb-leveldown.svg)](https://www.npmjs.com/package/dynamodb-leveldown) [![semantic-release](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--release-e10079.svg)](https://github.com/semantic-release/semantic-release)

A [LevelDOWN](https://github.com/level/leveldown) API implementation of [Amazon DynamoDB](https://aws.amazon.com/dynamodb/).

Originally forked from [Ten Bitcomb's AWSDOWN](https://github.com/Ravenstine/awsdown) which forked from [Klaus Trainer's DynamoDbDown](https://github.com/KlausTrainer/dynamodbdown) which was "heavily inspired by" [David Guttman's DynamoDown](https://github.com/davidguttman/dynamodown) and [Jed Schmidt's dynamo-down](https://github.com/jed/dynamo-down)

This is a drop-in replacement for [LevelDOWN](https://github.com/level/leveldown) that uses [Amazon DynamoDB](https://aws.amazon.com/dynamodb/) for persistence. It can be used as a backend for [LevelUP](https://github.com/level/levelup) rather than an actual LevelDB store.

As of version 0.7, [LevelUP](https://github.com/level/levelup) allows you to pass a `db` option when you create a new instance. This will override the default [LevelDOWN](https://github.com/level/leveldown) store with a [LevelDOWN](https://github.com/level/leveldown) API compatible object. `DynamoDbDown` conforms exactly to the [LevelDOWN](https://github.com/level/leveldown) API, but performs operations against a DynamoDB database.

## Why?

The intended use case for this library is with [PouchDB](https://github.com/pouchdb/pouchdb). Compatibility with [PouchDB](https://github.com/pouchdb/pouchdb) is a big win in this case since it provides a common JavaScript interface for interacting with documents as well as _full replication_, including attachments of any size. Using this [LevelDOWN](https://github.com/level/leveldown) implementation with [PouchDB](https://github.com/pouchdb/pouchdb) can be useful for regular backups as well as migrating data to CouchDB.

## Why the fork?

Other similar implementation have become old, stale, and don't appear to be maintained any more. This fork has updated all dependencies, and runs [LevelUP](https://github.com/level/levelup) and [LevelDOWN](https://github.com/level/leveldown) automated tests to help ensure quality.

## Usage Example

```js
const levelup = require('levelup');
const { DynamoDB } = require('aws-sdk');
const { DynamoDbDown } = require('dynamodbdown');

const factory = DynamoDbDown(
  new DynamoDB({
    region: 'us-west-1',
    secretAccessKey: 'foo',
    accessKeyId: 'bar'
  })
);

const db = levelup(factory('tableName'));

db.put('some string', 'LevelUP string');
db.put('some binary', Buffer.from('LevelUP buffer'));

const dbReadStream = db.createReadStream();

dbReadStream.on('data', console.log);
dbReadStream.on('close', () => {
  console.log('read stream closed');
});
```

When running the above example, you should get the following console output:

```sh
{ key: 'some binary', value: 'LevelUP buffer' }
{ key: 'some string', value: 'LevelUP string' }
read stream closed
```

## Hash Keys

In DynamoDB, keys consist of two parts: a _hash key_ and a _range key_. To achieve LevelDB-like behaviour, all keys in a database instance are given the same hash key. That means that you can't do range queries over keys with different hash keys.

The default hash key is `!`. You can specify it by putting a `$` in the `location` argument. The `$` separates the table name from the hash key.

### Example

```js
const levelup = require('levelup');
const { DynamoDB } = require('aws-sdk');
const { DynamoDbDown } = require('dynamodbdown');

const factory = DynamoDbDown(
  new DynamoDB({
    region: 'us-west-1',
    secretAccessKey: 'foo',
    accessKeyId: 'bar'
  })
);

const db = levelup(factory('tableName$hashKey'));

db.put('some key', 'some value', => err {
  // the DynamoDB object would now look like this:
  // {
  //   '---hkey': 'hashKey',
  //   '---rkey': 'some key',
  // }
});
```

If you are fine with sharing capacity units across multiple database instances or applications, you can reuse a table by specifying the same table name, but different hash keys.

## Table Creation

If the table doesn't exist, `DynamoDbDown` will try to create a table. You can specify the read/write throughput. If not specified, it will default to `1/1`. If the table already exists, the specified throughput will have no effect. Throughput can be changed for tables that already exist by using the DynamoDB API or the AWS Console.

See [LevelUP options](https://github.com/level/levelup#options) for more information.

### Example

```js
const levelup = require('levelup');
const { DynamoDB } = require('aws-sdk');
const { DynamoDbDown } = require('dynamodbdown');

const dynamoDBOptions = {
  region: 'eu-west-1',
  secretAccessKey: 'foo',
  accessKeyId: 'bar'
};

// capacity can be specified; defaults to 1/1:
const factoryOptions = {
  ProvisionedThroughput: {
    ReadCapacityUnits: 1,
    WriteCapacityUnits: 1
  }
};

const factory = DynamoDbDown(new DynamoDB(dynamoDBOptions));

const db = levelup(factory('tableName'), factoryOptions);
```

## Other Considerations

This library may not be suitable for multi-process database access, since there is no mechanism for locking DynamoDB tables. If you find you need to have multiple processes access your database, it will be necessary to maintain direct-access on a single thread and have other processes communicate with that instance. Using [multilevel](https://github.com/juliangruber/multilevel) is one pre-made way of achieving this.

## Changelog

See [here](https://github.com/GioCirque/DynamoDbDown/releases).

## Acknowledgments

DynamoDbDown has been heavily inspired by, and/or forked from:

- Ten Bitcomb's [AWSDOWN](https://github.com/Ravenstine/awsdown)
- Klaus Trainer's [DynamoDBDOWN](https://github.com/KlausTrainer/dynamodbdown)
- David Guttman's [DynamoDown](https://github.com/davidguttman/dynamodown)
- Jed Schmidt's [dynamo-down](https://github.com/jed/dynamo-down)

## LICENSE

Copyright 2019 Gio Palacino

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
