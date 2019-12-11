'use strict'

const { promisify } = require('util')
const { serialize, deserialize } = require('./utils')

const DATA_KEY = '---data'
const HASH_KEY = '---hkey'
const RANGE_KEY = '---rkey'
const MAX_BATCH_SIZE = 25
const RESOURCE_WAITER_DELAY = 1
const defaultProvisionedThroughput = {
  ReadCapacityUnits: 1,
  WriteCapacityUnits: 1
}

class DynamoDbAsync {
  constructor (dynamoDb, tableName, hashKey) {
    if (!dynamoDb) return

    this._hashKey = hashKey
    this._dynamoDb = dynamoDb
    this._tableName = tableName
    this._query = promisify(this._dynamoDb.query).bind(this._dynamoDb)
    this._waitFor = promisify(this._dynamoDb.waitFor).bind(this._dynamoDb)
    this._getItem = promisify(this._dynamoDb.getItem).bind(this._dynamoDb)
    this._putItem = promisify(this._dynamoDb.putItem).bind(this._dynamoDb)
    this._deleteItem = promisify(this._dynamoDb.deleteItem).bind(this._dynamoDb)
    this._createTable = promisify(this._dynamoDb.createTable).bind(this._dynamoDb)
    this._deleteTable = promisify(this._dynamoDb.deleteTable).bind(this._dynamoDb)
    this._describeTable = promisify(this._dynamoDb.describeTable).bind(this._dynamoDb)
    this._batchWriteItem = promisify(this._dynamoDb.batchWriteItem).bind(this._dynamoDb)
  }

  withoutKeys (item) {
    if (!item) return item

    Reflect.deleteProperty(item, HASH_KEY)
    Reflect.deleteProperty(item, RANGE_KEY)

    return item
  }

  keyConditionsFor (key) {
    return {
      [HASH_KEY]: {
        ComparisonOperator: 'EQ',
        AttributeValueList: [{ S: this._hashKey }]
      },
      [RANGE_KEY]: key
    }
  }

  rangeKeyFrom (item) {
    if (!item) return undefined

    if (item.Key) return item.Key[RANGE_KEY].S
    if (item[RANGE_KEY]) return item[RANGE_KEY].S
    if (item.PutRequest && item.PutRequest.Item) return item.PutRequest.Item[RANGE_KEY].S
    if (item.DeleteRequest && item.DeleteRequest.Key) return item.DeleteRequest.Key[RANGE_KEY].S

    return undefined
  }

  itemKey (key) {
    return {
      Key: {
        [HASH_KEY]: { S: this._hashKey },
        [RANGE_KEY]: { S: key.toString() }
      }
    }
  }

  queryItem (key) {
    return {
      TableName: this._tableName,
      ...this.itemKey(key)
    }
  }

  dataItem (key, value) {
    return {
      Item: {
        ...this.itemKey(key).Key,
        [DATA_KEY]: serialize(value)
      }
    }
  }

  dataTableItem (key, value) {
    return {
      TableName: this._tableName,
      ...this.dataItem(key, value)
    }
  }

  dataFromItem (item) {
    const deserialized = deserialize({ M: item })
    return deserialized[DATA_KEY]
  }

  async get (key) {
    const record = await this._getItem(this.queryItem(key))
    if (!record || !record.Item) throw new Error('NotFound')
    return this.dataFromItem(record.Item)
  }

  async put (key, value) {
    const params = this.dataTableItem(key, value)
    return this._putItem(params)
  }

  async batch (array) {
    const ops = []
    const opKeys = {}
    for (const item of array) {
      if (opKeys[item.key]) {
        // We want to ensure that there are no duplicate keys in the same
        // batch request, as DynamoDB won't accept those. That's why we only
        // retain the last operation here.
        const idx = ops.findIndex(someItem => this.rangeKeyFrom(someItem) === item.key)
        if (idx !== -1) ops.splice(idx, 1)
      }

      opKeys[item.key] = true

      if (item.type === 'del') {
        ops.push({ DeleteRequest: this.itemKey(item.key) })
      } else {
        ops.push({ PutRequest: this.dataItem(item.key, item.value) })
      }
    }

    let resp
    const reqs = []
    const params = { RequestItems: {} }

    while (ops.length > 0) {
      if (resp && resp.UnprocessedItems && resp.UnprocessedItems[this._tableName]) {
        reqs.push(...resp.UnprocessedItems[this._tableName])
      }
      reqs.push(...ops.splice(0, MAX_BATCH_SIZE - reqs.length))
      if (reqs.length === 0) return

      params.RequestItems[this._tableName] = reqs.splice(0)
      resp = await this._batchWriteItem(params)
    }
  }

  async query (params) {
    const tableParams = {
      TableName: this._tableName,
      ...params
    }
    return this._query(tableParams)
  }

  async delete (key) {
    const params = this.queryItem(key)
    await this._deleteItem(params)
  }

  async tableExists () {
    const params = { TableName: this._tableName }
    try {
      await this._describeTable(params)
    } catch (e) {
      return false
    }
    return true
  }

  async createTable (throughput = defaultProvisionedThroughput) {
    await this._createTable({
      TableName: this._tableName,
      AttributeDefinitions: [
        { AttributeName: HASH_KEY, AttributeType: 'S' },
        { AttributeName: RANGE_KEY, AttributeType: 'S' }
      ],
      KeySchema: [
        { AttributeName: HASH_KEY, KeyType: 'HASH' },
        { AttributeName: RANGE_KEY, KeyType: 'RANGE' }
      ],
      ProvisionedThroughput: throughput
    })
    await this._waitFor('tableExists', {
      TableName: this._tableName,
      $waiter: { delay: RESOURCE_WAITER_DELAY }
    })

    return true
  }

  async deleteTable () {
    await this._deleteTable({ TableName: this._tableName })
    await this._waitFor('tableNotExists', {
      TableName: this._tableName,
      $waiter: { delay: RESOURCE_WAITER_DELAY }
    })

    return true
  }
}

module.exports = { DynamoDbAsync }
