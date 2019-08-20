const { describe } = require('tape-plus')
const kappa = require('kappa-core')
const Query = require('../')
const ram = require('random-access-memory')
const memdb = require('memdb')
const level = require('level')
const collect = require('collect-stream')
const crypto = require('crypto')

const { cleanup, tmp, replicate } = require('./util')

describe('basic', (context) => {
  let core, db, indexes

  context.beforeEach((c) => {
    core = kappa(ram, { valueEncoding: 'json'  })
    db = memdb()

    indexes = [
      { key: 'log', value: ['value', 'timestamp'] },
      { key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] },
    ]

    core.use('query', Query(db, core, { indexes }))
  })

  context('perform a query', (assert, next) => {
    let data = [{
      type: 'chat/message',
      timestamp: Date.now(),
      content: { body: 'First message' }
    }, {
      type: 'user/about',
      timestamp: Date.now(),
      content: { name: 'Grace' }
    }, {
    }, {
      type: 'chat/message',
      timestamp: Date.now() + 3,
      content: { body: 'Third message' }
    }, {
      type: 'chat/message',
      timestamp: Date.now() + 2,
      content: { body: 'Second message' }
    }, {
      type: 'user/about',
      timestamp: Date.now() + 1,
      content: { name: 'Poison Ivy' }
    }]

    core.ready(() => {
      core.writer('local', (err, feed) => {
        feed.append(data, (err, _) => {
          assert.error(err)

          let query = [{ $filter: { value: { type: 'chat/message' } } }]

          collect(core.api.query.read({ reverse: true, live: false, query }), (err, msgs) => {
            var check = data
              .filter((msg) => msg.type === 'chat/message')
              .sort((a, b) => a.timestamp < b.timestamp ? +1 : -1)

            assert.same(msgs.map((msg) => msg.value), check, 'querys messages using correct index')
            next()
          })
        })
      })
    })
  })
})

describe('multiple feeds', (context) => {
  let core, db, storage
  let name1, name2

  context.beforeEach((c) => {
    storage = tmp()
    core = kappa(storage, { valueEncoding: 'json'  })
    db = level(`${storage}/views`)  // memdb()

    indexes = [{ key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }]

    name1 = crypto.randomBytes(16).toString('hex')
    name2 = crypto.randomBytes(16).toString('hex')

    core.use('query', Query(db, core, { indexes }))
  })

  context.afterEach((c) => {
    // cleanup([storage])
  })

  context('aggregates all valid messages from all feeds when querying', (assert, next) => {
    let query = [{ $filter: { value: { type: 'chat/message' } } }]
    let timestamp = Date.now()

    core.ready(() => {
      setup(name1, (feed1) => {
        setup(name2, (feed2) => {
          assert.same(core.feeds().length, 2, 'two local feeds')

          collect(core.api.query.read({ live: false, query }), (err, msgs) => {
            assert.error(err, 'no error')
            assert.ok(msgs.length === 2, 'returns two messages')
            assert.same(msgs.map((msg) => msg.value), [
              { type: 'chat/message', timestamp, author: feed1.key.toString('base64') },
              { type: 'chat/message', timestamp, author: feed2.key.toString('base64') }
            ], 'replicated and aggregated')
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
          timestamp,
          author: feed.key.toString('base64')
        }, (err, seq) => {
          assert.error(err, 'no error')
          cb(feed)
        })
      })
    }
  })
})

describe('multiple cores', (context) => {
  let core1, db1, storage1
  let core2, db2, storage2

  context.beforeEach((c) => {
    storage1 = tmp()
    storage2 = tmp()
    core1 = kappa(storage1, { valueEncoding: 'json' })
    core2 = kappa(storage2, { valueEncoding: 'json' })
    db1 = level(`${storage1}/views`)  // memdb()
    db2 = level(`${storage2}/views`)  // memdb()
    // db1 = memdb()
    // db2 = memdb()

    indexes = [{ key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }]

    core1.use('query', Query(db1, core1, { indexes }))
    core2.use('query', Query(db2, core2, { indexes }))
  })

  context.afterEach((c) => {
    // cleanup([storage1, storage2])
  })

  context('aggregates all valid messages from all feeds when querying', (assert, next) => {
    let query = [{ $filter: { value: { type: 'chat/message' } } }]
    let timestamp = Date.now()

    setup(core1, (feed1) => {
      setup(core2, (feed2) => {
        collect(core1.api.query.read({ live: false, query }), (err, msgs) => {
          assert.error(err, 'no error')
          assert.ok(msgs.length === 1, 'returns a single message')

          replicate(core1, core2, (err) => {
            assert.error(err, 'no error')

            collect(core2.api.query.read({ live: false, query }), (err, msgs) => {
              assert.error(err, 'no error')
              assert.ok(msgs.length === 2, 'returns two messages')
              assert.same(msgs.map((msg) => msg.value), [
                { type: 'chat/message', timestamp, author: feed1.key.toString('base64') },
                { type: 'chat/message', timestamp, author: feed2.key.toString('base64') }
              ], 'replicated and aggregated')
              next()
            })
          })
        })
      })
    })

    function setup (kcore, cb) {
      kcore.ready(() => {
        kcore.writer('local', (err, feed) => {
          assert.error(err, 'no error')
          feed.append({
            type: 'chat/message',
            timestamp,
            author: feed.key.toString('base64')
          }, (err, seq) => {
            assert.error(err, 'no error')
            cb(feed)
          })
        })
      })
    }
  })
})
