const through = require('through2')
const memdb = require('memdb')
const charwise = require('charwise')
const { EventEmitter } = require('events')
const debug = require('debug')('kappa-view-query')

const Explain = require('./explain')
const Filter = require('./filter')

const { isFunction } = require('./util')

module.exports = function KappaViewQuery (db = memdb(), opts = {}) {
  const events = new EventEmitter()

  const {
    indexes = [],
    validator = (msg) => msg,
    keyEncoding = charwise
  } = opts

  const view = {
    maxBatch: opts.maxBatch || 100,

    map: (msgs, next) => {
      var ops = []

      msgs.forEach((msg) => {
        msg = validator(msg)
        if (!msg) return

        indexes.forEach((idx) => {
          var indexKeys = getIndexValues(msg, idx.value)

          if (indexKeys.length) {
            ops.push({
              type: 'put',
              key: [idx.key, ...indexKeys],
              value: [msg.key, msg.seq].join('@'),
              keyEncoding,
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

      debug(`[INDEXING] ${JSON.stringify(ops)}`)

      db.batch(ops, next)
    },
    indexed: (msgs) => {
      msgs.forEach((msg) => events.emit('update', msg))
    },
    api: {
      read: (core, _opts) => {
        var __opts = view.api.explain(core, _opts)
        var source = __opts.createStream(__opts)
        return Filter(source, _opts)
      },
      explain: (core, _opts) => {
        var explain = Explain(indexes.map((idx) => Object.assign(idx, {
          exact: typeof idx.exact === 'boolean' ? idx.exact : false,
          createStream: (__opts) => {
            var thru = through.obj(function (msg, enc, next) {
              var msgId = msg.value
              var [ feedId, sequence ] = msgId.split('@')
              var feed = core._logs.feed(feedId)
              var seq = Number(sequence)

              feed.get(seq, (err, value) => {
                if (err) return next()
                var msg = validator({ key: feed.key.toString('hex'), seq, value })
                if (!msg) return next()
                this.push(msg)
                next()
              })
            })

            var stream = db.createReadStream(Object.assign(__opts, {
              lte: [idx.key, ...__opts.lte],
              gte: [idx.key, ...__opts.gte],
              keyEncoding,
              keys: true,
              values: true
            }))

            stream.pipe(thru)

            return thru
          }
        })))

        return explain(_opts)
      },
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
