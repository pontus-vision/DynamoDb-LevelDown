const { AbstractLevelDOWN,
        AbstractIterator } = require('abstract-leveldown'),
        ltgt               = require('ltgt'),
        debug              = require('debug')('S3LevelDOWN');


function lt(value) {
  return ltgt.compare(value, this._end) < 0
}

function lte(value) {
  return ltgt.compare(value, this._end) <= 0
}

function getStartAfterKey(key) {
  var keyMinusOneNum = (key.charCodeAt(key.length - 1) - 1)
  var keyMinusOne = keyMinusOneNum >= 0 ? (String.fromCharCode(keyMinusOneNum) + '\uFFFF') : ''
  return key.substring(0, key.length - 1) + keyMinusOne
}

function nullEmptyUndefined(v) {
  return typeof v === 'undefined' || v === null || v === ''
}

class S3Iterator extends AbstractIterator {

  constructor(db, options){
    super(db);
    this._limit   = options.limit
  
    if (this._limit === -1)
      this._limit = Infinity;
  
    this.keyAsBuffer = options.keyAsBuffer !== false;
    this.valueAsBuffer = options.valueAsBuffer !== false;
    this.fetchValues = options.values;
    this._reverse   = options.reverse;
    this._options = options;
    this._done = 0;
    this.bucket = db.bucket;
    this.db = db;
    this.s3 = db.s3;
    this.s3ListObjectMaxKeys = options.s3ListObjectMaxKeys || 1000;
    if (!this._reverse && this._limit < this.s3ListObjectMaxKeys) {
      this.s3ListObjectMaxKeys = this._limit
    }
  
    this._start = ltgt.lowerBound(options);
    this._end = ltgt.upperBound(options);
    if (!nullEmptyUndefined(this._end)) {
      if (ltgt.upperBoundInclusive(options))
        this._test = lte
      else
        this._test = lt
    }
  
    if (!nullEmptyUndefined(this._start))
      this.startAfter = ltgt.lowerBoundInclusive(options) ? getStartAfterKey(this._start) : this._start
      
    debug('new iterator %o', this._options);
  }

  _next(callback) {  
    if (this._done++ >= this._limit || 
      (this.data && this.dataUpto == this.data.length && !this.s3nextContinuationToken))
      return setImmediate(callback)
  
    if (!this.data || this.dataUpto == this.data.length) {
      listObjects.apply(this)
    } else {
      fireCallback.apply(this);
    }
  
    function listObjects() {
      var params = {
          Bucket: this.bucket,
          MaxKeys: this.s3ListObjectMaxKeys
      }
  
      if (this.db.folderPrefix !== '') {
        params.Prefix = this.db.folderPrefix
      }
  
      if (this.s3nextContinuationToken) {
        params.ContinuationToken = this.s3nextContinuationToken
        debug('listObjectsV2 ContinuationToken %s', params.ContinuationToken)
      }
      else if (typeof this.startAfter !== 'undefined') {
        params.StartAfter = this.db.folderPrefix + this.startAfter
      }
  
      this.s3.listObjectsV2(params, function(err, data) {
        if (err) {
          debug('listObjectsV2 error %s', err.message)
          callback(err)
        } else {
          if (data.Contents.length === 0) {
            debug('listObjectsV2 empty')
            return setImmediate(callback)
          }
  
          debug('listObjectsV2 %d keys', data.Contents.length)
  
          if (this.data && this.dataUpto === 0) {
            this.data = this.data.concat(data.Contents)
          } else {
            this.data = data.Contents
          }
  
          this.dataUpto = 0
          this.s3nextContinuationToken = data.NextContinuationToken
  
          if (this._reverse && this.s3nextContinuationToken &&
            data.Contents.every(function(x) {
              return this._test(x.Key.substring(this.db.folderPrefix.length, x.Key.length)) })
            ) {
            listObjects()
          } else {
            fireCallback()
          }
        }
      })
    }
  
  
    function fireCallback() {
      var index, key
      for(;;) {
        index = (!this._reverse) ? this.dataUpto : (this.data.length - 1 - this.dataUpto)
        var awskey = this.data[index].Key
        key = awskey.substring(this.db.folderPrefix.length, awskey.length)
        debug('iterator data index %d: %s', index, key)
        this.dataUpto++
  
        if (this._test(key)) {
          break
        }
  
        if (!this._reverse || this.dataUpto === this.data.length) {
          return setImmediate(callback)
        }
      }
  
      if (this.fetchValues) {
        if (this.data[index].Size === 0)
          getCallback(null, '')
        else
          this.db._get(key, null, getCallback)
      }
      else
        getCallback()
  
      function getCallback(err, value) {
        if (err) {
          if (err.message == 'NotFound') {
            // collection changed while we were iterating, skip this key
            return setImmediate(function () {
              this._next(callback)
            })
          }
          return setImmediate(function () {
            callback(err)
          })
        }
  
        if (this.keyAsBuffer && !(key instanceof Buffer))
          key = new Buffer(key)
        if (!this.keyAsBuffer && (value instanceof Buffer))
          key = key.toString('utf8')
  
        if (this.fetchValues) {
          if (this.valueAsBuffer && !(value instanceof Buffer))
            value = new Buffer(value)
          if (!this.valueAsBuffer && (value instanceof Buffer))
            value = value.toString('utf8')
        }
  
        setImmediate(function () {
          debug('_next result %s=%s', key, value)
          callback(null, key, value)
        })
      }
    }
  }
  
  _test() { return true }
}






class S3LevelDOWN extends AbstractLevelDOWN {

  constructor(s3, location){
    super(typeof location == 'string' ? location : '');

    this.s3 = s3;

    if (!(this instanceof S3LevelDOWN))
      return new S3LevelDOWN(location);

    if (location.indexOf('/') !== -1) {
      this.folderPrefix = location.substring(location.indexOf('/') + 1, location.length) + '/';
      this.bucket = location.substring(0, location.indexOf('/'));
    } else {
      this.folderPrefix = '';
      this.bucket = location;
    }

    debug('db init %s %s', this.bucket, this.folderPrefix);
  }

  _open(options, callback) {
    setImmetiate(() => callback(null, this));
  }
  
  _put(key, value, options, callback) {
    if (nullEmptyUndefined(value))
      value = new Buffer('');
  
    if (!(value instanceof Buffer || value instanceof String))
      value = String(value);
  
    this.s3.upload({
      Bucket: this.bucket,
      Key: this.folderPrefix + key,
      Body: value
    }, function(err) {
      if (err) {
        debug('Error s3 upload: %s %s', key, err.message);
        callback(err);
      } else {
        debug('Successful s3 upload: %s', key);
        callback();
      }
    })
  }
  
  _get(key, options, callback) {
    this.s3.getObject({
      Bucket: this.bucket,
      Key: this.folderPrefix + key
    }, function (err, data) {
      if (err) {
        debug('Error s3 getObject: %s %s', key, err.message)
        if (err.code === 'NoSuchKey') {
          callback(new Error('NotFound'))
        } else {
          callback(err)
        }
      } else {
        var value = data.Body;
        if (options && options.asBuffer && !(value instanceof Buffer))
          value = new Buffer(value);
        if ((!options || !options.asBuffer) && (value instanceof Buffer))
          value = value.toString('utf8');
        debug('getObject: %s', key);
        callback(null, value);
      }
    })
  
  }
  
  _del(key, options, callback) {
    this.s3.deleteObject({
      Bucket: this.bucket,
      Key: this.folderPrefix + key
    }, function (err) {
      if (err) {
        debug('Error s3 delete: %s %s', key, err.message)
        callback(err)
      } else {
        debug('Successful s3 delete: %s', key)
        callback()
      }
    })
  }
  
  _batch(array, options, callback) {
    var i = 0
      , len = array.length;
  
      function act(action, cb) {
        if (!action) {
          return setImmediate(cb)
        }
  
        var key = (action.key instanceof Buffer) ? action.key : String(action.key)
        var value = action.value
  
        if (action.type === 'put') {
          this._put(key, value, null, cb)
        } else if (action.type === 'del') {
          this._del(key, null, cb)
        }
      }
  
      function actCallback(err) {
        if (err) {
          return setImmediate(function() { callback(err) })
        }
  
        if (++i >= len) {
          return setImmediate(callback)
        }
  
        act.apply(this, [array[i], actCallback])
      }
  
      act.apply(this, [array[i], actCallback])
  }
  
  _iterator(options) {
    return new S3Iterator(this, options)
  }

}


module.exports = function(s3){
  const func = function(location){
    return new S3LevelDOWN(s3, location);
  };
  
  func.destroy = function (name, cb) {

  };

  return func;
};

