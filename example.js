const kappa = require('kappa-core')
const ram = require('random-access-memory')
const collect = require('collect-stream')
const memdb = require('memdb')
const level = require('level')
const pull = require('pull-stream')

const Query = require('./')
const { validator } = require('./util')

const core = kappa(ram, { valueEncoding: 'json'  })

const db = memdb() || level('/tmp/db')

const indexes = [
  { key: 'log', value: ['value', 'timestamp'] },
  { key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] },
  { key: 'cha', value: [['value', 'type'], ['value', 'content', 'channel'], ['value', 'timestamp']] }
]

core.use('query', Query(db, core, { indexes, validator }))

core.ready(() => {
  core.writer('local', (err, feed) => {
    const data = [{
      type: 'chat/message',
      timestamp: 1561996331739,
      content: { body: 'First message' }
    }, {
      type: 'user/about',
      timestamp: 1561996331740,
      content: { name: 'Grace' }
    }, {
    }, {
      type: 'chat/message',
      timestamp: 1561996331742,
      content: { body: 'Third message' }
    }, {
      type: 'chat/message',
      timestamp: 1561996331743,
      content: { channel: 'dogs', body: 'Lurchers rule' }
    }, {
      type: 'chat/message',
      timestamp: 1561996331741,
      content: { body: 'Second message' }
    }, {
      type: 'user/about',
      timestamp: 1561996331754,
      content: { name: 'Poison Ivy' }
    }]

    feed.append(data)
  })

  const query = [{ $filter: { value: { type: 'chat/message', content: { channel: 'dogs' } } } }]

  // For live queries
  core.api.query.read({ live: true, query }).on('data', (msg) => {
    console.log(msg)
  })

  // For static queries
  collect(core.api.query.read({ query }), (err, msgs) => {
    if (err) return console.error(err)
    console.log(err, msgs)
  })
})
