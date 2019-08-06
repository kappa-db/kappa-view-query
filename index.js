const toPull = require('stream-to-pull-stream')
const through = require('through2')
const delay = require('delayed-stream')
const merge = require('merge-stream')
const memdb = require('memdb')
const charwise = require('charwise')
const FlumeViewQuery = require('flumeview-query/inject')
const { EventEmitter } = require('events')

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
          this.push({ key: feed.key.toString('hex'), seq, value })
          next()
        })
      })

      stream.pipe(thru)
    }
  }))

  var query = FlumeViewQuery({ stream: source }, indexes)

  var view = {
    maxBatch: opts.maxBatch || 100,

    map: (msgs, next) => {
      const ops = []

      msgs.forEach((msg) => {
        if (!validator(msg)) return
        var msgId = `${msg.key}@${msg.seq}`

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
      read: (core, _opts) => query.read(_opts),
      explain: (core, _opts) => query.explain(_opts),
      add: (core, _opts) => query.add(_opts),
      events
    }
  }

  function source (_opts) {
    // TODO: check if this is the correct way to use this?
    var delayed

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
  }

  return view
}

