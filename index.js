const toPull = require('stream-to-pull-stream')
const defer = require('pull-defer')
const pull = require('pull-stream')
const get = require('lodash.get')
const memdb = require('memdb')
const charwise = require('charwise')

const FlumeViewQuery = require('flumeview-query/inject')
const many = require('pull-many')

module.exports = function KappaViewQuery (db, core, opts = {}) {
  var {
    indexes = [],
    db = memdb(),
    validator = function (msg) {
      if (typeof msg !== 'object') return null
      if (typeof msg.value !== 'object') return null
      if (typeof msg.value.timestamp !== 'number') return null
      if (typeof msg.value.type !== 'string') return null
      return msg
    }
  } = opts


  indexes = indexes.map((idx) => {
    return {
      key: idx.key,
      value: idx.value,
      exact: 'boolean' === typeof idx.exact ? idx.exact : false,
      createStream: function (_opts) {
        _opts.keyEncoding = charwise
        _opts.lte.unshift(idx.key)
        _opts.gte.unshift(idx.key)
        _opts.keys = true
        _opts.values = true

        return pull(
          toPull(db.createReadStream(_opts)),
          pull.asyncMap((msg, next) => {
            var id = msg.value
            var feed = core._logs.feed(id.split('@')[0])
            var seq = Number(id.split('@')[1])
            feed.get(seq, function (err, value) {
              if (err) return next(err)

              next(null, {
                key: feed.key.toString('hex'),
                seq,
                value
              })
            })
          })
        )
      }
    }
  })

  function source (_opts) {
    var source = defer.source()

    core.ready(function () {
      source.resolve(
        pull(
          many(
            core.feeds().map((feed) => {
              return pull(
                toPull(feed.createReadStream(_opts)),
                pull.map((data) => ({ value: data }))
              )
            })
          )
        )
      )
    })

    return source
  }

  var query = FlumeViewQuery({ stream: source }, indexes)

  var view = {
    maxBatch: opts.maxBatch || 100,

    map: function (msgs, next) {
      const ops = []

      msgs.forEach((msg) => {
        if (!validator(msg)) return

        indexes.forEach((idx) => {
          var values = []
          var found = false

          if (Array.isArray(idx.value[0])) {
            var msgValues = idx.value
              .map(a => a.join('.'))
              .map(v => {
                var t = get(msg, v)
                if (typeof t === 'number') return t.toString()
                else return t
              })
              .filter(Boolean)

            if (msgValues.length === idx.value.length) {
              values = [idx.key, ...msgValues]
              found = true
            }
          } else {
            var value = get(msg, idx.value.join('.'))
            if (value) {
              values = [idx.key, value]
              found = true
            }
          }

          if (found && values.length > 1) {
            ops.push({
              type: 'put',
              key: values,
              value: `${msg.key}@${msg.seq}`,
              keyEncoding: charwise,
            })
          }
        })
      })

      db.batch(ops, next)
    },
    api: {
      read: (core, _opts) => query.read(_opts),
      explain: (core, _opts) => query.explain(_opts)
    }
  }
  return view
}

