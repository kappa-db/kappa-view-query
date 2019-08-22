const kappa = require('kappa-core')
const ram = require('random-access-memory')
const collect = require('collect-stream')
const memdb = require('memdb')

const Query = require('./')
const { validator } = require('./util')
const { cleaup, tmp } = require('./test/util')

const core = kappa(ram, { valueEncoding: 'json' })
const db = memdb()

const indexes = [
  { key: 'log', value: ['value', 'timestamp'] },
  { key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] },
  { key: 'cha', value: [['value', 'type'], ['value', 'content', 'channel'], ['value', 'timestamp']] }
]

core.use('query', Query(db, { indexes, validator }))

core.writer('local', (err, feed) => {
  const data = [{
    type: 'chat/message',
    timestamp: Date.now(),
    content: { body: 'Hi im new here...' }
  }, {
    type: 'user/about',
    timestamp: Date.now() + 1,
    content: { name: 'Grace' }
  }, {
    type: 'chat/message',
    timestamp: Date.now() + 2,
    content: { body: 'Second post' }
  }, {
    type: 'chat/message',
    timestamp: Date.now() + 3,
    content: { channel: 'dogs', body: 'Lurchers rule' }
  }, {
    type: 'chat/message',
    timestamp: Date.now() + 4,
    content: { channel: 'dogs', body: 'But sometimes I prefer labra-doodles' }
  }, {
    type: 'user/about',
    timestamp: Date.now() + 5,
    content: { name: 'Poison Ivy' }
  }]

  feed.append(data, (err, _) => {
    core.ready('query', () => {
      const query = [{ $filter: { value: { type: 'chat/message', content: { channel: 'dogs' } } } }]

      collect(core.api.query.read({ query }), (err, chats) => {
        if (err) return console.error(err)
        console.log(chats)

        collect(core.api.query.read({ query: [{ $filter: { value: { type: 'user/about' } } }] }), (err, users) => {
          if (err) return console.error(err)
          console.log(users)
        })
      })
    })
  })
})
