const { describe } = require('tape-plus')
const kappa = require('kappa-core')
const Query = require('../')
const ram = require('random-access-memory')
const memdb = require('memdb')
const collect = require('collect-stream')

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
