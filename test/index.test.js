const test = require('tape')
const memdb = require('memdb')
const ram = require('random-access-memory')
const pull = require('pull-stream')
const kappa = require('kappa-core')
const View = require('../')

const db = memdb()

const core = kappa(ram, { valueEncoding: 'json' })

var indexes = [{ key: 'typ', value: [['value', 'type'], ['value', 'timestamp']]  }]
const view = View(db, core, indexes)
core.use('query', 1, view)

var seeds = [0, 1, 3, 5, 2, 4].map((i) => ({
  type: 'chat/message',
  timestamp: Date.now() + i,
  content: { body: `Hello World ${i}` }
}))

seedDB(() => {
  test('orders messages by type then timestamp', function (assert) {
    const opts = { query: [{ $filter: { value: { type: 'chat/message' }  } }] }

    pull(
      core.api.query.read(opts),
      pull.collect((err, msgs) => {
        if (err) return console.error(err)
        assert.deepEqual(
          msgs.map((msg) => msg.value),
          seeds.sort((a, b) => a.timestamp > b.timestamp ? +1 : -1),
          'Index orders messages by type and timestamp'
        )
        assert.end()
      })
    )
  })
})

function seedDB (cb) {
  core.ready(function () {
    core.writer('local', function (err, feed) {
      feed.append(seeds, cb)
    })
  })
}
