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
    validator = (msg) => msg
  } = opts

  db = db ? db : memdb()

  indexes = indexes.map((idx) => ({
    key: idx.key,
    value: idx.value,
    exact: typeof idx.exact === 'boolean' ? idx.exact : false,
    createStream: (_opts) => {
      var thru = through.obj(function (msg, enc, next) {
        var msgId = msg.value
        var [ feedId, sequence ] = msgId.split('@')
        var feed = core._logs.feed(feedId)
        var seq = Number(sequence)

        debug(`[INDEX] indexing: feed ID ${feedId} sequence ${seq}`)

        feed.get(seq, (err, value) => {
          if (err) return next()
          debug(`[QUERY] got message ${JSON.stringify(value)}`)
          this.push({
            key: feed.key.toString('hex'),
            seq,
            value
          })
          next()
        })
      })

      db.createReadStream(Object.assign(_opts, {
        lte: [idx.key, ..._opts.lte],
        gte: [idx.key, ..._opts.gte],
        keyEncoding: charwise,
        keys: true,
        values: true
      })).pipe(thru)

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
        debug(`[QUERY] ${JSON.stringify(_opts.query)}`)
        var __opts = view.api.explain(core, _opts)
        var source = __opts.createStream(__opts)
        return Filter(source, _opts)
      },
      explain: (core, _opts) => Explain(indexes)(_opts),
      add: (core, _opts) => {
        var isValid = _opts && isFunction(_opts.createStream) && Array.isArray(_opts.index || _opts.value)
        if(!isValid) throw new Error('kappa-view-query.add: expected opts { index, createStream }')
        _opts.value = _opts.index || _opts.value
        indexes.push(_opts)
      },
      onUpdate: (core, cb) => {
        events.on('update', cb)
      },
      storeState: (state, cb) => {
        state = state.toString('base64')
        db.put('state', state, cb)
      },
      fetchState: (cb) => {
        db.get('state', function (err, state) {
          if (err && err.notFound) cb()
          else if (err) cb(err)
          else cb(null, Buffer.from(state, 'base64'))
        })
      },
      events
    }
  }

  return view
}
