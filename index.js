const MFR = require('map-filter-reduce')
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
    db = memdb()
  } = opts

  indexes = indexes.map((idx) => {
    return {
      key: idx.key,
      value: idx.value,
      exact: 'boolean' === typeof idx.exact ? idx.exact : false,
      createStream: function (_opts) {
        _opts.lte.unshift(idx.key)
        _opts.gte.unshift(idx.key)
        _opts.keys = true
        _opts.values = true

        console.log("CRRETE STREAM")
        return pull(
          toPull(db.createReadStream(_opts)),
          pull.asyncMap((msg, next) => {
            console.log("MESSAGE: ", msg)
            var id = msg.value
            var feed = core._logs.feed(id.split('@')[1])
            var seq = Number(id.split('@')[2])
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
          ),
          pull.through(console.log)
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
      var pending = msgs.length

      msgs.forEach((msg) => {
        --pending
        if (!sanitize(msg)) return

        indexes.forEach((idx) => {
          var values = []
          var found = false
          var accessor = idx.value

          if (Array.isArray(accessor[0])) {
            var msgValues = accessor
              .map(a => a.join('.'))
              .map(v => {
                var t = get(msg, v)
                if (typeof t === 'number') return t.toString()
                else return t
              })
              .filter(Boolean)

            if (msgValues.length === accessor.length) {
              values = msgValues
              found = true
            }
          } else {
            accessor = accessor.join('.')
            value = get(msg, accessor)

            if (value) {
              values = [value]
              found = true
            }
          }

          if (found && values.length > 0) {
            ++pending

            // use charwise for key.
            // set key encoding using charwise.

            db.get(idx.key, function (err) {
              if (err && err.notFound) {
                ops.push({
                  type: 'put',
                  key: values,
                  value: `${idx.key}@${msg.key}@${msg.seq}`,
                  keyEncoding: charwise,
                })
              }

              if (!--pending) done()
            })
          }
        })
      })

      if (!pending) done()

      function done () {
        db.batch(ops, next)
      }
    },
    api: {
      source: (core, _opts) => source(_opts),
      read: (core, _opts) => {
        return query.read(_opts)
      },
      explain: query.explain
    }
  }
  return view
}

function sanitize (msg) {
  if (typeof msg !== 'object') return null
  if (typeof msg.value !== 'object') return null
  if (typeof msg.value.timestamp !== 'number') return null
  if (typeof msg.value.type !== 'string') return null
  return msg
}
