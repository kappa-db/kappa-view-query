const through = require('through2')
const memdb = require('memdb')
const charwise = require('charwise')
const { EventEmitter } = require('events')
const debug = require('debug')('kappa-view-query')
const liveStream = require('level-live-stream')
const { isFunction } = require('util')

const Explain = require('./explain')
const Filter = require('./filter')

module.exports = function KappaViewQuery (db = memdb(), opts = {}) {
  const events = new EventEmitter()

  const {
    indexes = [],
    validator = (msg) => msg,
    keyEncoding = charwise,
    getMessage,
  } = opts

  return {
    maxBatch: opts.maxBatch || 100,
    map,
    indexed,
    api: {
      read,
      explain,
      add,
      onUpdate: (core, cb) => events.on('update', cb),
      events
    }
  }

  function explain (core, _opts) {
    var expl = Explain(indexes.map((idx) => Object.assign(idx, {
      exact: typeof idx.exact === 'boolean' ? idx.exact : false,
      createStream: (__opts) => {
        var thru = through.obj(function (msg, enc, next) {
          if (msg.sync) {
            this.push(msg)
            return next()
          }

          getMessage(msg, (err, msg) => {
            if (err) return next()
            this.push(msg)
            next()
          })
        })

        var streamOpts = Object.assign(__opts, {
          lte: [idx.key, ...__opts.lte],
          gte: [idx.key, ...__opts.gte],
          keyEncoding,
          keys: true,
          values: true
        })

        var stream = __opts.live
          ? liveStream(db, streamOpts)
          : db.createReadStream(streamOpts)

        stream.pipe(thru)

        return thru
      }
    })))

    return expl(_opts)
  }

  function read (core, _opts) {
    var __opts = explain(core, _opts)
    var source = __opts.createStream(__opts)
    return Filter(source, _opts)
  }

  function add (core, _opts) {
    var isValid = _opts && isFunction(_opts.createStream) && Array.isArray(_opts.index || _opts.value)
    if(!isValid) throw new Error('kappa-view-query.add: expected opts { index, createStream }')
    _opts.value = _opts.index || _opts.value
    indexes.push(_opts)
  }

  function map (msgs, next) {
    var ops = []

    msgs.forEach((msg) => {
      msg = validator(msg)
      if (!msg) return

      indexes.forEach((idx) => {
        var indexKeys = getIndexValues(msg, idx.value)

        if (indexKeys.length) {
          ops.push({
            type: 'put',
            key: [idx.key, ...indexKeys, msg.seq, msg.key],
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

    db.batch(ops, next)
  }

  function indexed (msgs) {
    msgs.forEach((msg) => {
      events.emit('update', msg)
    })
  }
}
