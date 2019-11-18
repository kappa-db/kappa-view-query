const { describe } = require('tape-plus')
const kappa = require('kappa-core')
const Query = require('../')
const ram = require('random-access-memory')
const memdb = require('memdb')
const level = require('level')
const collect = require('collect-stream')
const crypto = require('crypto')
const debug = require('debug')('kappa-view-query')

const seeds = require('./seeds.json')
  .sort((a, b) => a.timestamp < b.timestamp ? +1 : -1)

const drive = require('./drive.json')
  .sort((a, b) => a.timestamp < b.timestamp ? +1 : -1)

const { cleanup, tmp, replicate } = require('./util')

describe('basic', (context) => {
  let core, db, indexes

  context.beforeEach((c) => {
    core = kappa(ram, { valueEncoding: 'json'  })
    db = memdb()

    indexes = [
      { key: 'log', value: [['value', 'timestamp']] },
      { key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] },
      { key: 'fil', value: [['value', 'filename'], ['value', 'timestamp']] }
    ]

    core.use('query', Query(db, { indexes }))
  })

  context('perform a query', (assert, next) => {
    core.writer('local', (err, feed) => {
      feed.append(seeds, (err, _) => {
        assert.error(err)

        let query = [{ $filter: { value: { type: 'chat/message' } } }]

        core.ready('query', () => {
          collect(core.api.query.read({ reverse: true, query }), (err, msgs) => {
            var check = seeds.filter((msg) => msg.type === 'chat/message')

            assert.same(msgs.map((msg) => msg.value), check, 'querys messages using correct index')
            next()
          })
        })
      })
    })
  })

  context('get all messages', (assert, next) => {
    core.writer('local', (err, feed) => {
      feed.append(seeds, (err, _) => {
        assert.error(err)

        let query = [{ $filter: { value: { timestamp: { $gt: 0 } } } }]

        core.ready('query', () => {
          collect(core.api.query.read({ query }), (err, msgs) => {
            var check = seeds
            assert.equal(msgs.length, check.length, 'gets the same number of messages')
            assert.same(msgs.map((msg) => msg.value), check, 'querys messages using correct index')
            next()
          })
        })
      })
    })
  })

  context('fil index', (assert, next) => {
    core.writer('local', (err, feed) => {
      feed.append(seeds, (err, _) => {
        assert.error(err)

        let query = [{ $filter: { value: { filename: 'hello.txt', timestamp: { $gt: 0 } } } }]

        core.ready('query', () => {
          collect(core.api.query.read({ query }), (err, msgs) => {
            var check = drive
            assert.equal(msgs.length, check.length, 'gets the same number of messages')
            assert.same(msgs.map((msg) => msg.value), check, 'querys messages using correct index')
            next()
          })
        })
      })
    })
  })
})

describe('multiple feeds', (context) => {
  let core, db
  let name1, name2

  context.beforeEach((c) => {
    core = kappa(ram, { valueEncoding: 'json'  })
    db = memdb()

    indexes = [{ key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }]

    name1 = crypto.randomBytes(16).toString('hex')
    name2 = crypto.randomBytes(16).toString('hex')

    core.use('query', Query(db, { indexes }))
  })

  context('aggregates all valid messages from all feeds when querying', (assert, next) => {
    var query = [{ $filter: { value: { type: 'chat/message' } } }]
    var timestamp = Date.now()
    var count = 0

    setup(name1, (feed1) => {
      setup(name2, (feed2) => {
        debug(`initialised feed1: ${feed1.key.toString('hex')} feed2: ${feed2.key.toString('hex')}`)
        assert.same(2, core.feeds().length, 'two local feeds')

        core.ready('query', () => {
          collect(core.api.query.read({ query }), (err, msgs) => {
            assert.error(err, 'no error')
            assert.ok(msgs.length === 2, 'returns two messages')
            assert.same(msgs, [
              { key: feed1.key.toString('hex'), seq: 0, value: { type: 'chat/message', timestamp, content: { body: name1 } } },
              { key: feed2.key.toString('hex'), seq: 0, value: { type: 'chat/message', timestamp: timestamp + 1, content: { body: name2 } }}
            ], 'aggregates all feeds')
            next()
          })
        })
      })
    })

    function setup (name, cb) {
      core.writer(name, (err, feed) => {
        assert.error(err, 'no error')
        feed.append({
          type: 'chat/message',
          timestamp: timestamp + count,
          content: { body: name }
        }, (err, seq) => {
          assert.error(err, 'no error')
          count++
          cb(feed)
        })
      })
    }
  })
})

describe('multiple cores', (context) => {
  let core1, db1
  let core2, db2

  context.beforeEach((c) => {
    core1 = kappa(ram, { valueEncoding: 'json' })
    core2 = kappa(ram, { valueEncoding: 'json' })

    indexes = [{ key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }]

    core1.use('query', Query(memdb(), { indexes }))
    core2.use('query', Query(memdb(), { indexes }))
  })

  context('aggregates all valid messages from all feeds when querying', (assert, next) => {
    var query = [{ $filter: { value: { type: 'chat/message' } } }]
    var timestamp = Date.now()
    var count = 0

    setup(core1, (feed1) => {
      setup(core2, (feed2) => {
        debug(`initialised core1: ${feed1.key.toString('hex')} core2: ${feed2.key.toString('hex')}`)
        assert.same(1, core1.feeds().length, 'one feed')
        assert.same(1, core2.feeds().length, 'one feed')

        core1.ready('query', () => {
          collect(core1.api.query.read({ query }), (err, msgs) => {
            assert.error(err, 'no error')
            assert.ok(msgs.length === 1, 'returns a single message')

            replicate(core1, core2, (err) => {
              assert.error(err, 'no error')
              assert.same(2, core1.feeds().length, `first core has second core's feed`)
              assert.same(2, core2.feeds().length, `second core has first core's feed`)

              core2.ready('query', () => {
                collect(core2.api.query.read({ query }), (err, msgs) => {
                  assert.error(err, 'no error')
                  assert.ok(msgs.length === 2, 'returns two messages')
                  assert.same(msgs, [
                    { key: feed1.key.toString('hex'), seq: 0, value: { type: 'chat/message', timestamp } },
                    { key: feed2.key.toString('hex'), seq: 0, value: { type: 'chat/message', timestamp: timestamp + 1 } }
                  ], 'query aggregates messages from all feeds')
                  next()
                })
              })
            })
          })
        })
      })
    })

    function setup (kcore, cb) {
      kcore.writer('local', (err, feed) => {
        assert.error(err, 'no error')
        feed.append({
          type: 'chat/message',
          timestamp: timestamp + count
        }, (err, seq) => {
          count++
          assert.error(err, 'no error')
          cb(feed)
        })
      })
    }
  })
})
