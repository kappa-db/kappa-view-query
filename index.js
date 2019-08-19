const delay = require('delayed-stream')
const merge = require('merge-stream')
const through = require('through2')
const memdb = require('memdb')
const charwise = require('charwise')
const { EventEmitter } = require('events')
const debug = require('debug')('kappa-view-query')

const Explain = require('./explain')
const Filter = require('./filter')

const { isFunction } = require('./util')

module.exports = function KappaViewQuery (db, core, opts = {}) {
  var events = new EventEmitter()

  var {
    indexes = [],
    db = memdb(),
    validator = (msg) => msg
  } = opts

  indexes = indexes.map((idx) => ({
    key: idx.key,
    value: idx.value,
    exact: 'boolean' === typeof idx.exact ? idx.exact : false,
    createStream: (_opts) => {
      debug(`[INDEX] initialising ${idx.key}: indexing on ${idx.value}`)
      var stream = db.createReadStream(Object.assign(_opts, {
        lte: [idx.key, ..._opts.lte],
        gte: [idx.key, ..._opts.gte],
        keyEncoding: charwise,
        keys: true,
        values: true
      }))

      var thru = through.obj(function (msg, enc, next) {
        var msgId = msg.value
        var [ feedId, sequence ] = msgId.split('@')
        var feed = core._logs.feed(feedId)
        var seq = Number(sequence)

        feed.get(seq, (err, value) => {
          if (err) return next()
          debug(`[INDEX] indexing: feed ID ${feedId} sequence ${seq} message: ${JSON.stringify(value)}`)
          this.push({
            key: feed.key.toString('hex'),
            seq,
            value
          })
          next()
        })
      })

      stream.pipe(thru)

      return thru
    }
  }))

  var view = {
    maxBatch: opts.maxBatch || 100,

    map: (msgs, next) => {
      const ops = []

      msgs.forEach((msg) => {
        if (!validator(msg)) return
        var msgId = [msg.key, msg.seq].join('@')

        indexes.forEach((idx) => {
          var indexKeys = getIndexValues(msg, idx.value)

          if (indexKeys.length) {
            ops.push({
              type: 'put',
              key: [idx.key, ...indexKeys],
              value: msgId,
              keyEncoding: charwise,
            })
          }

          function getIndexValues (msg, value) {
            var child = value[0]
            if (Array.isArray(child)) {
              return value
                .map((val) => getIndexValues(msg, val))
                .reduce((acc, arr) => [...acc, ...arr], [])
                .filter(Boolean)
            } else if (typeof child === 'string') {
              return [value.reduce((obj, val) => obj[val], msg)]
                .filter(Boolean)
            } else return []
          }
        })
      })

      db.batch(ops, next)
    },
    indexed: (msgs) => {
      msgs.forEach((msg) => events.emit('insert', msg))
    },
    api: {
      read: (core, _opts) => {
        var __opts = view.api.explain(_opts)
        var source = __opts.createStream(__opts)
        debug(`[QUERY] ${JSON.stringify(_opts.query)}`)
        return Filter(source, _opts)
      },
      explain: Explain(indexes, (core, _opts) => {
        var delayed

        _opts.seqs = false
        _opts.values = true

        core.ready(() => {
          var feeds = core.feeds().reduce((acc, feed) => {
            var stream = feed.createReadStream(_opts)
            var thru = through.obj(function (value, enc, next) {
              this.push({ value })
              next()
            })
            stream.pipe(thru)
            acc.push(thru)
            return acc
          })

          delayed = delay.create(merge(feeds))
        })

        return delayed
      }),
      add: (core, _opts) => {
        var isValid = _opts && isFunction(_opts.createStream) && Array.isArray(_opts.index || _opts.value)
        if(!isValid) throw new Error('kappa-view-query.add: expected opts { index, createStream }')
        _opts.value = _opts.index || _opts.value
        indexes.push(_opts)
      },
      onUpdate: (core, cb) => {
        events.on('update', cb)
      },
      events
    }
  }

  return view
}

