'use strict'

const { DynamoDbDown } = require('./lib/dynamoDbDown')

const globalStore = {}
module.exports = function (dependencies) {
  const func = function (location) {
    const instance = new DynamoDbDown(dependencies, location)
    globalStore[location] = instance
    return instance
  }

  func.destroy = async function (name, cb) {
    const store = globalStore[name]
    if (!store) return cb(new Error('NotFound'))

    try {
      store._deleteTable()
    } catch (e) {
      if (e && e.code !== 'ResourceNotFoundException') {
        return cb(e)
      }
    }

    delete globalStore[name]
    return cb()
  }

  return func
}
