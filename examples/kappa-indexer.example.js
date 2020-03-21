const Kappa = require('kappa-core')
const Indexer = require('kappa-sparse-indexer')
const multifeed = require('multifeed')
const hypercore = require('hypercore')
const ram = require('random-access-memory')
const collect = require('collect-stream')
const memdb = require('memdb')
const sub = require('subleveldown')

const Query = require('../')
const { validator, fromMultifeed } = require('../util')
const { tmp } = require('../test/util')

// This example scopes the indexer to only use a chosen 'LOG' feed in our multifeed instance
// When multifeed learns about a new feed, either using feed.writer, or when emitted on replication
// our indexer will add the feed if it hasn't already, based on whether the first seq matches a specified header

// Some dummy data
const logMsgs = [{
  type: "user/about",
  timestamp: 1574069723313,
  content: {
    name: "Grace"
  }
}, {
  type: "chat/message",
  timestamp: 1574069723314,
  content: {
    body: "First message"
  }
}, {
  type: "chat/message",
  timestamp: 1574069723317,
  content: {
    body: "Third message"
  }
}]

const ignoredMsgs = [{
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

Example()

function Example () {
  const core = new Kappa()
  const feeds = multifeed(tmp(), { valueEncoding: 'json' })
  const db = memdb()
  const idx = new Indexer({
    db: sub(db, 'idx'),
    name: 'example'
  })

  const header = 'LOG'
  feeds.on('feed', (feed) => {
    if (idx.feed(feed.key)) return
    feed.get(0, (err, msg) => {
      if (msg === header) idx.add(feed, { scan: true })
    })
  })

  const indexes = [
    { key: 'log', value: [['value', 'timestamp']] },
    { key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }
  ]
  const view = Query(sub(db, 'view'), {
    indexes,
    validator,
    getMessage: fromMultifeed(feeds)
  })

  core.use('query', idx.source(), view)

  setupFirstFeed((err, log) => {
    setupQueries(() => {
      setupSecondFeed((err, ignoredFeed) => {
        // append the two remaining chat messages to the log
        log.append(logMsgs)
        // append some data to our ignored log, this won't be indexed
        ignoredFeed.append(ignoredMsgs)
      })
    })
  })

  function setupFirstFeed (callback) {
    // create a log feed, the one we want to index
    feeds.writer('log', (err, log) => {
      log.append(header, (err) => {
        // append our user/about message to the log
        log.append(logMsgs.shift(), (err, seq) => {
          callback(err, log)
        })
      })
    })
  }

  function setupSecondFeed (callback) {
    // create a second feed, the one we want to ignore
    feeds.writer('ignored', callback)
  }

  function setupQueries (callback) {
    // make sure our indexes are ready before executing any queries
    core.ready('query', () => {
      // setup a sync query to log all user/about
      collect(core.view.query.read({
        query: [{ $filter: { value: { type: 'user/about' } } }],
      }), (err, msgs) => {
        console.log(msgs)
        // [{
        //   type: 'user/about',
        //   timestamp: 1574069723313,
        //   content: { name: 'Grace' }
        // }]
        next()
      })

      function next () {
        // then listen live for all chat/message
        core.view.query.read({
          query: [{ $filter: { value: { type: 'chat/message', timestamp: { $gt: 0 } } } }],
          live: true
        }).on('data', (msg) => {
          console.log(msg)
        // {
        //   type: 'chat/message',
        //   timestamp: 1574069723314,
        //   content: { body: 'First message' }
        // }
        //
        // {
        //   type: 'chat/message',
        //   timestamp: 1574069723317,
        //   content: { body: 'Third message' } 
        // }
        })

        callback()
      }
    })
  }
}
