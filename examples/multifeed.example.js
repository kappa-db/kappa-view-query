const Kappa = require('kappa-core')
const createMultifeedSource = require('kappa-core/sources/multifeed')
const multifeed = require('multifeed')
const ram = require('random-access-memory')
const collect = require('collect-stream')
const memdb = require('memdb')
const sub = require('subleveldown')

const Query = require('../')
const { validator, fromMultifeed } = require('../util')

const seedData = [{
  type: "chat/message",
  timestamp: 1574069723314,
  content: {
    body: "First message"
  }
}, {
  type: "user/about",
  timestamp: 1574069723313,
  content: {
    name: "Grace"
  }
}, {
  type: "chat/message",
  timestamp: 1574069723317,
  content: {
    body: "Third message"
  }
}, {
  type: "chat/message",
  timestamp: 1574069723316,
  content: {
    body: "Second message"
  }
},{
  type: "user/about",
  timestamp: 1574069723315,
  content: {
    name: "Poison Ivy"
  }
}]

// an example using multifeed for aggregating and querying all feeds
function MultifeedExample () {
  const core = new Kappa()
  const feeds = multifeed(ram, { valueEncoding: 'json' })
  const db = memdb()

  core.use('query', createMultifeedSource({ feeds, db: sub(db, 'state') }), Query(sub(db, 'view'), {
    indexes: [
      { key: 'log', value: [['value', 'timestamp']] },
      { key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }
    ],
    validator,
    getMessage: fromMultifeed(feeds)
  }))

  core.ready('query', () => {
    // setup a live query to first log all chat/message
    core.view.query.ready({
      query: [{ $filter: { value: { type: 'chat/message' } } }],
      live: true,
      old: false
    }).on('data', (msg) => {
      if (msg.sync) return next()
      console.log(msg)
    })

    function next () {
      // then to first log all user/about
      core.view.query.read({
        query: [{ $filter: { value: { type: 'user/about' } } }],
        live: true,
        old: false
      }).on('data', (msg) => {
        console.log(msg)
      })
    }
  })

  // then append a bunch of data to two different feeds in a multifeed
  feeds.writer('one', (err, one) => {
    feeds.writer('two', (err, two) => {

      one.append(seedData.slice(0, 3))
      two.append(seedData.slice(3, 5))
    })
  })
}
