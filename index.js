'use strict'

const { AbstractLevelDOWN }   = require('abstract-leveldown'),
        MAX_BATCH_SIZE        = 25,
        RESOURCE_WAITER_DELAY = 1,
        globalStore           = {},
        DynamoDBIterator      = require('./iterator'),
      { isPlainObject,
        isBuffer,
        marshalize,
        demarshalize,
        serialize,
        deserialize,
        castToBuffer,
        stringifyJSON,
        parseJSON }           = require('./lib/utils'),
      { promisify }           = require('util');

function hexEncodeTableName (str) {
  var hex = '';
  for (var pos = 0; pos < str.length; pos++)
    hex += String(str.charCodeAt(pos).toString(16));
  return hex;
}

class AWSDOWN extends AbstractLevelDOWN {
  constructor(dependencies = {}, location){
    super(location);
    const tableHash = location.split('$');

    this.tableName = tableHash[0];
    this.hashKey   = tableHash[1] || '!';
    this.s3Bucket  = this.tableName.split(/_/g).filter((x) => x.length).join('-');
    
    Object.entries(dependencies).forEach(([name, dependency]) => this[name] = dependency );
    
    globalStore[location] = this;
  }

  async _open(options={}, cb) {
    options.dynamodb = options.dynamodb || {};
    options.s3       = options.s3       || {};
    if (typeof options.prefix === 'string') this.tableName = this.tableName.replace(options.prefix, '');
  
    let shouldHexEncode = options.dynamodb.hexEncodeTableName === true;

    this.encodedTableName = shouldHexEncode ? hexEncodeTableName(this.tableName) : this.tableName;

    if(options.createIfMissing !== true) return cb(null, this);

    try {
      if(this.s3) await promisify(this.s3.createBucket).apply(this.s3, [{ Bucket: this.s3Bucket }]);
    } catch(err) {
      const exists = (err.message || '').match(/you already own it|bucket already exist/i);
      if(!exists) return cb(err);
    }

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
  }

  
  async _put(key, value, options, cb) {
    try {
      const params = {
        TableName: this.encodedTableName,
        Item: {
          '---hkey': {S: this.hashKey},
          '---rkey': {S: key.toString()},
        }
      };
      const shouldSpread = isPlainObject(value);
      if(shouldSpread) params.Item = Object.assign(await serialize(value).M, params.Item);
      const itemData = await promisify(this.dynamoDb.putItem).apply(this.dynamoDb, [params]);
      if(shouldSpread) return cb(null, itemData);
      const marshalized = await marshalize(value),
            objectData  = await promisify(this.s3.putObject).apply(this.s3, [{
        Body: marshalized.buffer,
        Bucket: this.s3Bucket,
        Key: `${this.hashKey}${key.toString()}`,
        ContentType: marshalized.mime || 'application/octet-stream'
      }]);
      cb(null, objectData);
    } catch(err) {
      cb(err);
    }
  }
  
  async _get(key, options, cb) {
    const getItem   = promisify(this.dynamoDb.getItem).bind(this.dynamoDb),
          getObject = promisify(this.s3.getObject).bind(this.s3);
    try {
      const record = await getItem({
        TableName: this.encodedTableName,
        Key: {
          '---hkey': {S: this.hashKey},
          '---rkey': {S: key.toString()}
        }
      });
      if(!record || !record.Item) return cb(new Error('NotFound'));
      if(Object.keys(record.Item || {}).length > 2){
        const deserialized = await deserialize({ M: record.Item });
        delete deserialized['---hkey'];
        delete deserialized['---rkey'];
        return cb(null, await stringifyJSON(deserialized));
      }
      const data = await getObject({
        Bucket: this.s3Bucket, 
        Key: `${this.hashKey}${key.toString()}`
      });
      if(!(data && data.Body)) return cb(null, Buffer.alloc(0));
      let output = await demarshalize({ mime: data.ContentType, buffer: Buffer.from(data.Body) });
      if(options.asBuffer && isBuffer(output)) return cb(null, output);
      if(data.ContentType === 'application/json') output = await stringifyJSON(output);
      if(options.asBuffer) output = await castToBuffer(output);
      cb(null, output);
    } catch(err) {
      cb(err);
    }
  }
  
  async _del(key, options, cb) {
    const params = {
      TableName: this.encodedTableName,
      Key: {
        '---hkey': {S: this.hashKey},
        '---rkey': {S: key.toString()}
      }
    };

    let error = null;
    try{
      await promisify(this.dynamoDb.deleteItem).apply(this.dynamoDb, [params]);
    }catch(err){
      error = err;
    } 
    try {
      await promisify(this.s3.deleteObject).apply(this.s3, [{
        Bucket: this.s3Bucket,
        Key: `${this.hashKey}${key.toString()}`
      }]);
    } catch(err){}
    cb(error);
  }
  
  async _batch(array, options, cb) {
    const opKeys = {},
          ops    = [];
    for(let item of array){
      if (opKeys[item.key]) {
        // We want to ensure that there are no duplicate keys in the same
        // batch request, as DynamoDB won't accept those. That's why we only
        // retain the last operation here.
        const idx = ops.findIndex(someItem => {
          return (someItem.DeleteRequest && someItem.DeleteRequest.Key['---rkey'].S === item.key) ||
                 (someItem.PutRequest && someItem.PutRequest.Item['---rkey'].S === item.key);
        });
        if (idx !== -1) ops.splice(idx, 1);
      }
    
      opKeys[item.key] = true;
  
      if (item.type === 'del') {
        ops.push({
          DeleteRequest: {
            Key: {
              '---hkey': {S: this.hashKey},
              '---rkey': {S: item.key.toString()}
            }
          }
        });
        continue;
      };

      const value = (await parseJSON(item.value, true)) || item.value;
      
      if(isPlainObject(value)) {
        const serialized = (await serialize(value, options.asBuffer)).M,
              Item       = Object.assign(serialized, {
                '---hkey': {S: this.hashKey},
                '---rkey': {S: item.key.toString()}
              });
        ops.push({
          PutRequest: {
            Item
          }
        });
      } else {
        const marshalized = await marshalize(item.value);
        ops.push({
          PutRequest: {
            Item: {
              '---hkey': {S: this.hashKey},
              '---rkey': {S: item.key.toString()},
            }
          },
          PutObjectRequest: {
            Bucket: this.s3Bucket,
            Key: `${this.hashKey}${item.key.toString()}`,
            Body: marshalized.buffer,
            ContentType: marshalized.mime
          }
        });
      }
    }
  
    const params         = {RequestItems: {}},
          batchWriteItem = promisify(this.dynamoDb.batchWriteItem).bind(this.dynamoDb),
          putObject      = promisify(this.s3.putObject).bind(this.s3);
  
    const loop = async (data) => {
      try {
        const reqs = [];
        if (data && data.UnprocessedItems && data.UnprocessedItems[this.encodedTableName])
          reqs.push.apply(reqs, data.UnprocessedItems[this.encodedTableName]);
        reqs.push.apply(reqs, ops.splice(0, MAX_BATCH_SIZE - reqs.length));
        if(reqs.length === 0) return cb();
        params.RequestItems[this.encodedTableName] = reqs.map(r => {
          const req = Object.assign({}, r);
          delete req.PutObjectRequest;
          return req;
        });
        const resp = await batchWriteItem(params);
        let i = 0;
        while(i < reqs.length){
          const req = reqs[i] || {}; i++;
          if(!req.PutObjectRequest) continue;
          await putObject(req.PutObjectRequest);
        }
        loop(resp);
      } catch(err){
        debugger
        return cb(err);
      }
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
        {AttributeName: '---hkey', AttributeType: 'S'},
        {AttributeName: '---rkey', AttributeType: 'S'}
      ],
      KeySchema: [
        {AttributeName: '---hkey', KeyType: 'HASH'},
        {AttributeName: '---rkey', KeyType: 'RANGE'}
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
    return new AWSDOWN(dependencies, location);
  };
  
  func.destroy = async function (name, cb) {
    const store = globalStore[name];

    if(!store) return cb(new Error('NotFound'));

    await promisify(store.dynamoDb.deleteTable).apply(store.dynamoDb, [{
      TableName: store.encodedTableName
    }]);
  
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
          delete globalStore[name];
          cb();
        }
      }
    );
  };

  return func;
}

