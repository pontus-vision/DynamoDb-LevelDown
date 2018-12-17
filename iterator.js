const through2           = require('through2'),
    { promisify }        = require('bluebird'),
    { AbstractIterator } = require('abstract-leveldown'),
    { isPlainObject,
      demarshalize,
      deserialize,
      castToBuffer }     = require('./lib/utils');

class DynamoDBIterator extends AbstractIterator {

  constructor(db, options){
    super(db);
    this.keyAsBuffer   = options.keyAsBuffer   !== false;
    this.valueAsBuffer = options.valueAsBuffer !== false;
    this.db = db;
    this.dynamoDb = db.dynamoDb;
    this.s3       = db.s3;
    this._results = this.createReadStream(options);
    this._results.once('end', () => {
      this._endEmitted = true
    });
  }

  async _next(cb) {
    const onEnd = () => {
      this._results.removeListener('readable', onReadable);
      cb();
    };
  
    const onReadable = () => {
      this._results.removeListener('end', onEnd);
      this._next(cb);
    };
  
    const obj = this._results.read();

    if(this._endEmitted) return cb();

    if (obj === null) {
      this._results.once('readable', onReadable);
      this._results.once('end', onEnd);
      return;
    }
    
    if (this.valueAsBuffer === false)
      obj.value = isPlainObject(obj.value) ? JSON.stringify(obj.value) : obj.value.toString();
    if (this.keyAsBuffer === false) obj.key = obj.key.toString();
    // FIXME: This could be better.
    const key   = this.keyAsBuffer   ? await castToBuffer(obj.key)   : obj.key,
          value = this.valueAsBuffer ? await castToBuffer(obj.value) : obj.value;
    cb(null, key, value);
  }
  
  createReadStream(opts) {
    let returnCount = 0
  
    if (opts.limit === -1) opts.limit = Infinity;

    const isFinished = () => {
      return opts.limit && returnCount > opts.limit;
    }
  
    const stream = through2.obj(async function (data, enc, cb) {

      const output = {
        key: await deserialize(data['---rkey']),
        value: data.value
      };

      if(isPlainObject(output.value)) {
        delete output.value['---hkey'];
        delete output.value['---rkey'];
      }

      returnCount += 1;
  
      if (!isFinished()) this.push(output);
  
      cb();

    });
  
    const onData = (err, data) => {
      this._waiting = false;
      if (err) {
        err.code === 'ResourceNotFoundException' ? stream.end() : stream.emit('error', err);
        return stream;
      }
  
      data.Items.forEach(item => {
        const filtered = (opts.gt && !(item['---rkey'].S > opts.gt)) ||
                         (opts.lt && !(item['---rkey'].S < opts.lt));
        if (!filtered) stream.write(item);
      });
  
      opts.ExclusiveStartKey = data.LastEvaluatedKey;
  
      if (opts.ExclusiveStartKey && !isFinished()) {
        this.getRange(opts, onData);
      } else {
        stream.end();
      }

    }
  
    if (opts.limit === 0) {
      stream.end();
    } else {
      this.getRange(opts, onData);
    }
  
    return stream;
  }
  
  async getRange(opts, cb) {
    if (opts.gte) {
      if (opts.reverse) {
        opts.end = opts.gte;
      } else {
        opts.start = opts.gte;
      }
    }
  
    if (opts.lte) {
      if (opts.reverse) {
        opts.start = opts.lte;
      } else {
        opts.end = opts.lte;
      }
    }
  
    if (opts.gte > opts.lte && !opts.reverse) return cb(null, {Items: []});
  
    const rkey = createRKey(opts);
  
    const params = {
      TableName: this.db.encodedTableName,
      KeyConditions: {
        '---hkey': {
          ComparisonOperator: 'EQ',
          AttributeValueList: [
            {S: this.db.hashKey}
          ]
        },
        '---rkey': rkey
      },
      Limit: opts.limit,
      ScanIndexForward: !opts.reverse,
      ExclusiveStartKey: opts.ExclusiveStartKey
    };

    try {
      const query     = promisify(this.dynamoDb.query).bind(this.dynamoDb),
            getObject = promisify(this.s3.getObject).bind(this.s3),
            records   = await query(params);
      if(!records || !records.Items) throw new Error('Items not found');
      await Promise.all(records.Items.map(async item => {
        if(Object.keys(item) > 2) {
          item.value = await deserialize(Object.assign({}, item));
          return;
        };
        const dItem  = await deserialize({M: item}),
              object = await getObject({
                Bucket: this.db.s3Bucket,
                Key: `${dItem['---hkey']}${dItem['---rkey']}`
              }).catch(err => {
                if(err.message !== 'Object does not exist') throw err;
              });
        if(!object || !object.Body) return;
        item.value = await demarshalize({
          buffer: Buffer.from(object.Body),
          mime: object.ContentType || 'application/octet-stream'
        });
      }));
      cb(null, records);
    } catch(err){
      cb(err);
    }
  }

}

module.exports = DynamoDBIterator;

function createRKey (opts) {
  const defaultStart = '\u0000',
        defaultEnd   = '\xff\xff\xff\xff\xff\xff\xff\xff';

  if (opts.gt && opts.lt) return {
    ComparisonOperator: 'BETWEEN',
    AttributeValueList: [
      {S: opts.gt},
      {S: opts.lt}
    ]
  };

  if (opts.lt) return {
    ComparisonOperator: 'LT',
    AttributeValueList: [
      {S: opts.lt}
    ]
  };

  if (opts.gt) return {
    ComparisonOperator: 'GT',
    AttributeValueList: [
      {S: opts.gt}
    ]
  };

  if (!opts.start && !opts.end) return {
    ComparisonOperator: 'BETWEEN',
    AttributeValueList: [
      {S: defaultStart},
      {S: defaultEnd}
    ]
  };

  if (!opts.end) {
    const op = opts.reverse ? 'LE' : 'GE'
    return {
      ComparisonOperator: op,
      AttributeValueList: [
        {S: opts.start}
      ]
    }
  }

  if (!opts.start) {
    const op = opts.reverse ? 'GE' : 'LE'
    return {
      ComparisonOperator: op,
      AttributeValueList: [
        {S: opts.end}
      ]
    }
  }

  if (opts.reverse) return {
    ComparisonOperator: 'BETWEEN',
    AttributeValueList: [
      {S: opts.end},
      {S: opts.start}
    ]
  };

  return {
    ComparisonOperator: 'BETWEEN',
    AttributeValueList: [
      {S: opts.start},
      {S: opts.end}
    ]
  }
}
