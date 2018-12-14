'use strict'

const { AbstractLevelDOWN }   = require('abstract-leveldown'),
        MAX_BATCH_SIZE        = 25,
        RESOURCE_WAITER_DELAY = 1,
        globalStore           = {},
        serialize             = require('./serialize'),
        deserialize           = require('./deserialize'),
        DynamoDBIterator      = require('./iterator'),
      { isPlainObject,
        isBuffer,
        castToBuffer }        = require('./lib/utils'),
      { promisify }           = require('es6-promisify');

function hexEncodeTableName (str) {
  var hex = '';
  for (var pos = 0; pos < str.length; pos++) {
    hex += String(str.charCodeAt(pos).toString(16));
  }
  return hex;
}

class DynamoDBDOWN extends AbstractLevelDOWN {
  constructor(dependencies = {}, location){
    super(location);
    const tableHash = location.split('$');

    this.tableName = tableHash[0];
    this.hashKey   = tableHash[1] || '!';

    // this.dynamoDb = dynamodb;
    
    Object.entries(dependencies).forEach(([name, dependency]) => this[name] = dependency );
    
    globalStore[location] = this;

    if(this.s3) this.s3.createBucket({ Bucket: this.tableName }, () => {});

  }

  _open(options={}, cb) {
    // if (!options.dynamodb) return cb(new Error('`open` requires `options` argument with "dynamodb" key'));
    options.dynamodb = options.dynamodb || {};
    options.s3       = options.s3       || {};
    if (typeof options.prefix === 'string') this.tableName = this.tableName.replace(options.prefix, '');
  
    if (options.dynamodb.hexEncodeTableName === true) {
      this.encodedTableName = hexEncodeTableName(this.tableName)
    } else {
      this.encodedTableName = this.tableName
    }

    if(options.createIfMissing !== true) return cb(null, this);

    this.createTable({
      ProvisionedThroughput: options.dynamodb.ProvisionedThroughput || {
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 1
      }
    }, (err, data) => {
      const exists = err && (err.code === 'ResourceInUseException');
      if ((options.errorIfExists && exists) || (err && !exists)) {
        cb(err)
      } else {
        cb(null, this)
      }
    });

    // this._openS3({ Bucket: this.tableName }, cb);
    try {
    } catch(err) {
      
    }
    
  }

  
  _put(key, value, options, cb) {
    if(typeof key === 'string' && key.match('foobatch1')) debugger;
    const params = {
      TableName: this.encodedTableName,
      Item: {
        hkey: {S: this.hashKey},
        rkey: {S: key.toString()},
      }
    };
    const shouldSpread = isPlainObject(value);
    if(shouldSpread) {
      const serialized = serialize(value).M;
      params.Item = Object.assign(serialized, params.Item);
    }
    this.dynamoDb.putItem(params, (err, data) => {
      if(err) return cb(err);
      if(shouldSpread) return cb(null, data);
      this.s3.putObject({
        Body: JSON.stringify(serialize(value)),
        Bucket: this.tableName,
        Key: `${this.hashKey}${key.toString()}`
      }, cb);
    });
  }

  // _putS3(key, value, options, cb){
  //   const serialized = JSON.stringify(serialize(value));
  //   const params = {
  //     Body: serialized,
  //     Bucket: this.tableName, 
  //     Key: key
  //   }
  //   this.s3.putObject(params, cb);
  // }

  // _getS3(key, options, cb){
  //   const params = {
  //     Body: value.toString(),
  //     Bucket: this.tableName, 
  //     Key: key
  //   }
  //   this.s3.getObject(params, function(err, data) {
  //     if(err) return cb(err);
  //     if(!(data && data.Body)) return cb(new Error('NotFound'));
  //     const body = Buffer.from(data.Body).toString();
  //     const deserialized = deserialize(JSON.parse(body));
  //     cb(null, deserialized);
  //   });
  // }
  
  _get(key, options, cb) {
    const params = {
      TableName: this.encodedTableName,
      Key: {
        hkey: {S: this.hashKey},
        rkey: {S: key.toString()}
      }
    };
    this.dynamoDb.getItem(params, (err, data) => {
      if(err) return cb(err);
      // if(!(data && data.Item)) return cb(new Error('NotFound'));
      if(!data || !data.Item) return cb(new Error('NotFound'));
      if(Object.keys(data.Item || {}).length > 2){
        const deserialized = deserialize({ M: data.Item });
        cb(null, JSON.stringify(deserialized));
      } else {
        const params = {
          Bucket: this.tableName, 
          Key: `${this.hashKey}${key.toString()}`
        };
        this.s3.getObject(params, function(err, data) {
          // if(err) return cb(err);
          if(!(data && data.Body)) return cb(null, Buffer.alloc(0));
          const body = Buffer.from(data.Body).toString();
          let deserialized = deserialize(JSON.parse(body));
          if(isBuffer(deserialized)) deserialized = Buffer.from(deserialized);
          if(isPlainObject(deserialized)) deserialized = JSON.stringify(deserialized);
          deserialized = (options.asBuffer) ? castToBuffer(deserialized) : deserialized;
          try {
            cb(null, deserialized);
          } catch(err) {

          }
        });
      }
    });
    // this.dynamoDb.getItem(params, function (err, data) {
    //   if(err) return cb(err);
    //   if(!(data && data.Item)) return cb(new Error('NotFound'));
    //   let value = isPlainObject(data.Item.value) ? data.Item.value : { M: data.Item };
    //   if(value === undefined) return cb(new Error('NotFound'));
    //   let deserialized = deserialize(value);
    //   if(isBuffer(deserialized)) deserialized = Buffer.from(deserialized);
    //   if(isPlainObject(deserialized)) deserialized = JSON.stringify(deserialized);
    //   deserialized = (options.asBuffer) ? castToBuffer(deserialized) : deserialized;
    //   cb(null, deserialized);
    // });
  }
  
  _del(key, options, cb) {
    const params = {
      TableName: this.encodedTableName,
      Key: {
        hkey: {S: this.hashKey},
        rkey: {S: key.toString()}
      }
    };
    this.dynamoDb.deleteItem(params, (err) => {
      if(err) return cb(err);
      this.s3.deleteObject({
        Bucket: this.tableName,
        Key: `${this.hashKey}${key.toString()}`
      }, () => cb());
    });
  }
  
  async _batch(array, options, cb) {
    const opKeys = {},
          ops    = [];
  
    array.forEach((item) => {
      if (opKeys[item.key]) {
        // We want to ensure that there are no duplicate keys in the same
        // batch request, as DynamoDB won't accept those. That's why we only
        // retain the last operation here.
        const idx = ops.findIndex(someItem => {
          return (someItem.DeleteRequest && someItem.DeleteRequest.Key.rkey.S === item.key) ||
            (someItem.PutRequest && someItem.PutRequest.Item.rkey.S === item.key)
        });
  
        if (idx !== -1) ops.splice(idx, 1);
      }
  
      var op;
  
      opKeys[item.key] = true
  
      if (item.type === 'del') {
        op = {
          DeleteRequest: {
            Key: {
              hkey: {S: this.hashKey},
              rkey: {S: item.key.toString()}
            }
          }
        }
      } else {
        let value;
        try {
          value = typeof item.value === 'string' ? JSON.parse(item.value) : item.value;
        } catch(err){

        }
        if(isPlainObject(value)) {
          const serialized = serialize(value, options.asBuffer).M,
                Item       = Object.assign(serialized, {
                  hkey: {S: this.hashKey},
                            rkey: {S: item.key.toString()}
                });
          op = {
            PutRequest: {
              Item
            }
          };
        } else {
          op = {
            PutRequest: {
              Item: {
                hkey: {S: this.hashKey},
                rkey: {S: item.key.toString()},
                '---s3-key': `${this.hashKey}${item.key.toString()}`,
                '---s3-body': serialize(item.value)
              }
            }
          };
        }
      }
  
      ops.push(op);
    })

    const putObject = promisify(this.s3.putObject).bind(this.s3);

    let i = 0;

    while(i < ops.length){
      const req   = ops[i] || {},
            item  = (req.PutRequest || req.DeleteRequest || {}).Item || {},
            key   = item['---s3-key'],
            body  = item['---s3-body'];
      i++;
      delete item['---s3-body'];
      delete item['---s3-key'];
      if(!key) continue;
      if(req.PutRequest && body){
        try {
          await putObject({
            Body: JSON.stringify(body),
            Bucket: this.tableName,
            Key: key
          });
        } catch(err){

        }

        // this.s3.putObject({
        //   Body: JSON.stringify(body),
        //   Bucket: this.tableName,
        //   Key: key
        // }, () => {});
      }
    }
  
    const params = {RequestItems: {}};
  
    const loop = (err, data) => {
      if (err) return cb(err);

      const reqs = [];
  
      if (data && data.UnprocessedItems && data.UnprocessedItems[this.encodedTableName])
        reqs.push.apply(reqs, data.UnprocessedItems[this.encodedTableName]);
  
      reqs.push.apply(reqs, ops.splice(0, MAX_BATCH_SIZE - reqs.length));
  
      if(reqs.length === 0) return cb();

      params.RequestItems[this.encodedTableName] = reqs;
      this.dynamoDb.batchWriteItem(params, loop);
    }
  
    loop();
  }
  
  _iterator(options) {
    return new DynamoDBIterator(this, options);
  }
  
  createTable(opts, cb) {
    const params = {
      TableName: this.encodedTableName,
      AttributeDefinitions: [
        {AttributeName: 'hkey', AttributeType: 'S'},
        {AttributeName: 'rkey', AttributeType: 'S'}
      ],
      KeySchema: [
        {AttributeName: 'hkey', KeyType: 'HASH'},
        {AttributeName: 'rkey', KeyType: 'RANGE'}
      ]
    };
  
    params.ProvisionedThroughput = opts.ProvisionedThroughput || {
      ReadCapacityUnits: 1,
      WriteCapacityUnits: 1
    };
    this.dynamoDb.createTable(params, (err, data) => {
      if(err) return cb(err);
      this.dynamoDb.waitFor(
        'tableExists',
        {TableName: this.encodedTableName, $waiter: {delay: RESOURCE_WAITER_DELAY}},
        cb);
    });
  }
  
}

module.exports = function(dependencies){
  const func = function(location){
    return new DynamoDBDOWN(dependencies, location);
  };
  
  func.destroy = function (name, cb) {
    const store = globalStore[name]

    if(!store) return cb(new Error('NotFound'));
  
    store.dynamoDb.deleteTable({TableName: store.encodedTableName}, (err, data) => {
      if (err && err.code === 'ResourceNotFoundException') {
        delete globalStore[name]
        return cb();
      }
      if (err) return cb(err);
      store.dynamoDb.waitFor(
        'tableNotExists',
        {TableName: store.encodedTableName, $waiter: {delay: RESOURCE_WAITER_DELAY}},
        (err, data) => {
          if (err) {
            cb(err)
          } else {
            delete globalStore[name]
            cb()
          }
        }
      );
    });
  };

  return func;
}

