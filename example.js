const kappa = require('kappa-core')
const ram = require('random-access-memory')
const collect = require('collect-stream')
const memdb = require('memdb')

const Query = require('./')
const { validator } = require('./util')
const { cleaup, tmp } = require('./test/util')

const core = kappa(ram, { valueEncoding: 'json' })
const db = memdb()

function valueEncoding (msg) {
  if (typeof msg !== 'object') return null
  if (typeof msg.value !== 'object') return null
  if (typeof msg.value.timestamp !== 'number') return null
  if (typeof msg.value.type !== 'string') return null
  return msg
}

const indexes = [
  { key: 'log', value: [['value', 'timestamp']], valueEncoding },
  { key: 'typ', value: [['value', 'type'], ['value', 'timestamp']], valueEncoding },
  { key: 'cha', value: [['value', 'type'], ['value', 'content', 'channel'], ['value', 'timestamp']], valueEncoding }
]

core.use('query', Query(db, { indexes }))

core.writer('local', (err, feed) => {
  const data = [{
    type: 'chat/message',
    timestamp: Date.now(),
    content: { body: 'Hi im new here...' }
  }, {
    type: 'user/about',
    timestamp: Date.now(),
    content: { name: 'Grace' }
  }, {
    type: 'chat/message',
    timestamp: Date.now(),
    content: { body: 'Second post' }
  }, {
    type: 'chat/message',
    timestamp: Date.now(),
    content: { channel: 'dogs', body: 'Lurchers rule' }
  }, {
    type: 'chat/message',
    timestamp: Date.now(),
    content: { channel: 'dogs', body: 'But sometimes I prefer labra-doodles' }
  }, {
    type: 'user/about',
    timestamp: Date.now(),
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
