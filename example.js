const Kappa = require('kappa-core')
const multifeed = require('multifeed')
const hypercore = require('hypercore')
const ram = require('random-access-memory')
const collect = require('collect-stream')
const memdb = require('memdb')
const sub = require('subleveldown')

const Query = require('./')
const { validator, fromMultifeed, fromHypercore } = require('./util')
const { cleaup, tmp } = require('./test/util')

const seedData = require('./test/seeds.json')

// An example using a single hypercore
function HypercoreExample () {
  const core = new Kappa()
  const feed = hypercore(ram, { valueEncoding: 'json' })
  const db = memdb()

  core.use('query', createHypercoreSource({ feed, db: sub(db, 'state') }), Query(sub(db, 'view'), {
    // define a set of indexes
    indexes: [
      { key: 'log', value: [['value', 'timestamp']] },
      { key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }
    ],
    // you can pass a custom validator function to ensure all messages entering a feed match a specific format
    validator,
    // implement your own getMessage function, and perform any desired validation on each message returned by the query
    getMessage: fromHypercore(feed)
  }))

  feed.append(seedData, (err, _) => {
    core.ready('query', () => {
      const query = [{ $filter: { value: { type: 'chat/message' } } }]

      // grab then log all chat/message message types up until this point
      collect(core.view.query.read({ query }), (err, chats) => {
        if (err) return console.error(err)
        console.log(chats)

        // grab then log all user/about message types up until this point
        collect(core.view.query.read({ query: [{ $filter: { value: { type: 'user/about' } } }] }), (err, users) => {
          if (err) return console.error(err)
          console.log(users)
        })
      })
    })
  })
}

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
