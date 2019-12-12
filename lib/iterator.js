'use strict'

const through2 = require('through2')
const { AbstractIterator } = require('abstract-leveldown')
const { isPlainObject, castToBuffer, isBuffer } = require('./utils')

const EVENT_END = 'end'
const EVENT_ERROR = 'error'
const EVENT_PUSHED = 'pushed'
const EVENT_READABLE = 'readable'

class DynamoDbIterator extends AbstractIterator {
  constructor (db, options) {
    super(db)

    this.db = db
    this.options = options
    this._isOutOfRange = false
    this.dynamoDb = db.dynamoDb
    this._seekTarget = undefined
    this.dynamoDbAsync = db.dynamoDbAsync
    this.keyAsBuffer = options.keyAsBuffer !== false
    this.valueAsBuffer = options.valueAsBuffer !== false

    this._initResults()
  }

  _initResults () {
    if (this._results) this._results.destroy()
    this.options.inclusive = !this.options.gt && !this.options.lt
    this._results = this.createReadStream(this.options)
    this._results.once(EVENT_END, () => {
      this._endEmitted = true
    })
  }

  async _next (cb) {
    const onEnd = () => {
      this._results.removeListener(EVENT_READABLE, onReadable)
      cb()
    }

    const onReadable = () => {
      this._results.removeListener(EVENT_END, onEnd)
      this._next(cb)
    }

    await this._maybeSeek()
    if (this._isOutOfRange) return cb()

    const obj = this._results.read()

    if (!obj && this._endEmitted) return cb()

    if (!obj && !this._endEmitted) {
      this._results.once(EVENT_READABLE, onReadable)
      this._results.once(EVENT_END, onEnd)
      return
    }

    if (this.valueAsBuffer === false) {
      obj.value = isPlainObject(obj.value) ? JSON.stringify(obj.value) : obj.value.toString()
    }
    if (this.keyAsBuffer === false) {
      obj.key = obj.key.toString()
    }
    // FIXME: This could be better.
    const key = this.keyAsBuffer ? castToBuffer(obj.key) : obj.key
    const value = this.valueAsBuffer ? castToBuffer(obj.value) : obj.value

    cb(null, key, value)
  }

  async _peekNextKey () {
    const onPushNext = (next, resolve) => {
      this._results.removeListener(EVENT_END, onEnd)
      resolve(next)
    }
    const onEnd = resolve => {
      this._results.removeListener(EVENT_PUSHED, onPushNext)
      resolve(undefined)
    }
    const next = await new Promise(resolve => {
      if (this._endEmitted) return resolve()
      const next = this._results.read()
      if (next) {
        this._results.unshift(next)
        return resolve(next)
      } else {
        this._results.once(EVENT_PUSHED, next => onPushNext(next, resolve))
        this._results.once(EVENT_END, () => onEnd(resolve))
      }
    })
    return (next || {}).key
  }

  _getRange () {
    const reversed = this.options.reverse === true
    const start = reversed ? this.options.end : this.options.start
    const end = reversed ? this.options.start : this.options.end
    return {
      low: this.options.gt || this.options.gte || start,
      high: this.options.lt || this.options.lte || end,
      inclusiveLow: !this.options.gt,
      inclusiveHigh: !this.options.lt
    }
  }

  _isInRange (target) {
    const { high, low, inclusiveLow, inclusiveHigh } = this._getRange()
    const inRange =
      (!low || (inclusiveLow && target >= low) || target > low) &&
      (!high || (inclusiveHigh && target <= high) || target < high)
    return inRange
  }

  _outOfRange () {
    this._isOutOfRange = true
  }

  async _maybeSeek () {
    if (!this._seekTarget) return
    if (!this._isInRange(this._seekTarget)) return this._outOfRange()

    let nextKey, couldBeHere
    const seekKey = this._seekTarget
    const isReverse = this.options.reverse === true
    do {
      nextKey = await this._peekNextKey()
      if (!nextKey) return

      couldBeHere = isReverse ? nextKey <= seekKey || nextKey < seekKey : nextKey >= seekKey || nextKey > seekKey
      if (!couldBeHere) this._results.read()
    } while (!!nextKey && !couldBeHere)
    this._seekTarget = undefined
  }

  _seek (target) {
    this._isOutOfRange = false
    this._seekTarget = !!target && isBuffer(target) ? target.toString() : target
  }

  createReadStream (opts) {
    let returnCount = 0

    if (opts.limit < 0) {
      Reflect.deleteProperty(opts, 'limit')
    }

    const dynamoDbAsync = this.dynamoDbAsync
    const isFinished = () => {
      return opts.limit && returnCount > opts.limit
    }
    const pushNext = (transform, output) => {
      transform.emit(EVENT_PUSHED, output)
    }

    const stream = through2.obj(async function (data, enc, cb) {
      const key = dynamoDbAsync.rangeKeyFrom(data)
      const output = { key, value: data.value }

      if (isPlainObject(output.value)) {
        dynamoDbAsync.withoutKeys(output)
      }

      returnCount += 1
      this.push(output)
      pushNext(this, output)
      if (isFinished()) this.emit(EVENT_END)

      cb()
    })

    const onData = (err, data) => {
      this._waiting = false
      if (err) {
        err.code === 'ResourceNotFoundException' ? stream.end() : stream.emit(EVENT_ERROR, err)
        return stream
      }

      data.Items.forEach(item => {
        const rkey = this.dynamoDbAsync.rangeKeyFrom(item)
        const filtered = (opts.gt && !(rkey > opts.gt)) || (opts.lt && !(rkey < opts.lt))
        if (!filtered) {
          stream.write(item)
        }
      })

      opts.ExclusiveStartKey = data.LastEvaluatedKey
      if (opts.ExclusiveStartKey && !isFinished()) {
        this.getRange(opts, onData)
      } else {
        stream.end()
      }
    }

    if (opts.limit === 0) {
      stream.end()
    } else {
      this.getRange(opts, onData)
    }

    return stream
  }

  async getRange (options, cb) {
    const opts = { ...options }
    if (opts.gte) {
      if (opts.reverse) {
        opts.end = opts.gte
      } else {
        opts.start = opts.gte
      }
    }

    if (opts.lte) {
      if (opts.reverse) {
        opts.start = opts.lte
      } else {
        opts.end = opts.lte
      }
    }

    if (opts.gte > opts.lte && !opts.reverse) return cb(null, { Items: [] })

    const rkey = this.createRKey(opts)
    const params = {
      KeyConditions: this.dynamoDbAsync.keyConditionsFor(rkey),
      Limit: opts.limit,
      ScanIndexForward: !opts.reverse,
      ExclusiveStartKey: opts.ExclusiveStartKey
    }

    try {
      const records = await this.dynamoDbAsync.query(params)
      if (!records || !records.Items) throw new Error('Items not found')
      records.Items.forEach(item => (item.value = this.dynamoDbAsync.dataFromItem(item)))
      cb(null, records)
    } catch (err) {
      cb(err)
    }
  }

  createRKey (opts) {
    const defaultStart = '\u0000'
    const defaultEnd = '\xff\xff\xff\xff\xff\xff\xff\xff'

    if (opts.gt && opts.lt) {
      return {
        ComparisonOperator: 'BETWEEN',
        AttributeValueList: [{ S: opts.gt }, { S: opts.lt }]
      }
    }

    if (opts.lt) {
      return {
        ComparisonOperator: 'LT',
        AttributeValueList: [{ S: opts.lt }]
      }
    }

    if (opts.gt) {
      return {
        ComparisonOperator: 'GT',
        AttributeValueList: [{ S: opts.gt }]
      }
    }

    if (!opts.start && !opts.end) {
      return {
        ComparisonOperator: 'BETWEEN',
        AttributeValueList: [{ S: defaultStart }, { S: defaultEnd }]
      }
    }

    if (!opts.end) {
      const op = opts.reverse ? 'LE' : 'GE'
      return {
        ComparisonOperator: op,
        AttributeValueList: [{ S: opts.start }]
      }
    }

    if (!opts.start) {
      const op = opts.reverse ? 'GE' : 'LE'
      return {
        ComparisonOperator: op,
        AttributeValueList: [{ S: opts.end }]
      }
    }

    if (opts.reverse) {
      return {
        ComparisonOperator: 'BETWEEN',
        AttributeValueList: [{ S: opts.end }, { S: opts.start }]
      }
    }

    return {
      ComparisonOperator: 'BETWEEN',
      AttributeValueList: [{ S: opts.start }, { S: opts.end }]
    }
  }
}

module.exports = { DynamoDbIterator }
