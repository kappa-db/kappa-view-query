const Kappa = require('kappa-core')
const createHypercoreSource = require('kappa-core/sources/hypercore')
const hypercore = require('hypercore')
const ram = require('random-access-memory')
const collect = require('collect-stream')
const memdb = require('memdb')
const sub = require('subleveldown')

const Query = require('../')
const { validator, fromHypercore } = require('../util')

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

// This example scopes the indexer to only use a chosen 'LOG' feed in our multifeed instance
function MultifeedLimitedLogsWithSparseIndexer () {
  const core = new Kappa()
  const feeds = multifeed(ram, { valueEncoding: 'json' })
  const db = memdb()
  const idx = Indexer({
    db: sub(db, 'idx'),
    name: 'example'
  })

  const header = Buffer.from('LOG')
  feeds.on('feed', (feed) => {
    idx.feed(feed.key) return
    feed.get(0, (err, msg) => {
      if (msg === header) idx.add(feed, { scan: true })
    })
  })

  const indexes = [
    { key: 'log', value: [['value', 'timestamp']] },
    { key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }
  ]
  const view = Query(sub(db, 'view'), {
    validator,
    getMessage: fromMultifeed(feeds)
  })

  core.use('query', idx.source(), view)

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

  // create our log feed, the one we want to index
  feeds.writer('log', (err, log) => {
    // create a second long, the one without the header
    feeds.writer('ignore me', (err, ignore) => {
      // append our header
      log.append(header, (err, seq) => {
        log.append(seedData.slice(0, 3))
        ignore.append(seedData.slice(3, 5))
      })
    })
  })
}
