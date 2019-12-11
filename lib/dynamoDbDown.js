'use strict'

const { AbstractLevelDOWN } = require('abstract-leveldown')

const { DynamoDbIterator } = require('./iterator')
const { DynamoDbAsync } = require('./dynamoDbAsync')
const { isBuffer, hexEncodeString } = require('./utils')

class DynamoDbDown extends AbstractLevelDOWN {
  constructor (dependencies = {}, location) {
    super(location)

    const tableHash = location.split('$')
    this.tableName = tableHash[0]
    this.hashKey = tableHash[1] || '!'

    this.dynamoDbAsync = new DynamoDbAsync()
    Object.entries(dependencies).forEach(([name, dependency]) => (this[name] = dependency))
  }

  async _close (cb) {
    if (cb) cb()
  }

  async _open (options = {}, cb) {
    options.dynamodb = options.dynamodb || {}

    if (typeof options.prefix === 'string') this.tableName = this.tableName.replace(options.prefix, '')

    const shouldHexEncode = options.dynamodb.hexEncodeTableName === true
    this.encodedTableName = shouldHexEncode ? hexEncodeString(this.tableName) : this.tableName

    this.dynamoDbAsync = new DynamoDbAsync(this.dynamoDb, this.encodedTableName, this.hashKey)

    let tableExists = await this._tableExists()
    if (!tableExists && options.createIfMissing !== false) {
      tableExists = await this._createTable(options.dynamodb.ProvisionedThroughput)
    }

    if (tableExists && options.errorIfExists === true) {
      return cb(new Error('Underlying storage already exists!'), this)
    }
    if (!tableExists && options.createIfMissing === false) {
      return cb(new Error('Underlying storage does not exist!'), this)
    }

    cb(null, this)
  }

  async _put (key, value, options, cb) {
    try {
      const result = await this.dynamoDbAsync.put(key, value)
      cb(null, result)
    } catch (e) {
      cb(e)
    }
  }

  async _get (key, options, cb) {
    try {
      let output = await this.dynamoDbAsync.get(key)
      const asBuffer = options.asBuffer !== false
      if (asBuffer) {
        output = isBuffer(output) ? output : Buffer.from(String(output))
      }
      cb(null, output)
    } catch (e) {
      cb(e)
    }
  }

  async _del (key, options, cb) {
    try {
      await this.dynamoDbAsync.delete(key)
      cb()
    } catch (e) {
      cb(e)
    }
  }

  async _batch (array, options, cb) {
    try {
      await this.dynamoDbAsync.batch(array)
      cb()
    } catch (e) {
      cb(e)
    }
  }

  _iterator (options) {
    return new DynamoDbIterator(this, options)
  }

  async _tableExists () {
    return this.dynamoDbAsync.tableExists(this.encodedTableName)
  }

  async _createTable (throughput = undefined) {
    return this.dynamoDbAsync.createTable(throughput)
  }

  async _deleteTable () {
    return this.dynamoDbAsync.deleteTable()
  }
}

module.exports = { DynamoDbDown }
