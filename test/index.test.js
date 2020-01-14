const { describe } = require('tape-plus')
const Kappa = require('kappa-core')
const createMultifeedSource = require('kappa-core/sources/multifeed')
const createHypercoreSource = require('kappa-core/sources/hypercore')
const multifeed = require('multifeed')
const hypercore = require('hypercore')
const ram = require('random-access-memory')
const memdb = require('memdb')
const sub = require('subleveldown')
const level = require('level')
const collect = require('collect-stream')
const crypto = require('crypto')
const debug = require('debug')('kappa-view-query')

const Query = require('../')

const { fromMultifeed, fromHypercore } = require('../util')
const { cleanup, tmp, replicate } = require('./util')

const seeds = require('./seeds.json').sort((a, b) => a.timestamp > b.timestamp ? +1 : -1)
const drive = require('./drive.json').sort((a, b) => a.timestamp > b.timestamp ? +1 : -1)

describe('hypercore', (context) => {
  context('perform a query', (assert, next) => {
    var core = new Kappa()
    var feed = hypercore(ram, { valueEncoding: 'json' })
    var db = memdb()

    var source = createHypercoreSource({ feed, db: sub(db, 'state') })
    core.use('query', source, Query(sub(db, 'view'), {
      indexes: [{ key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }],
      getMessage: fromHypercore(feed)
    }))

    core.ready('query', () => {
      feed.append(seeds, (err, _) => {
        assert.error(err, 'no error')

        let query = [{ $filter: { value: { type: 'chat/message' } } }]

        core.ready('query', () => {
          collect(core.view.query.read({ query }), (err, msgs) => {
            var check = seeds.filter((msg) => msg.type === 'chat/message')

            assert.same(msgs.map((msg) => msg.value), check, 'querys messages using correct index')
            next()
          })
        })
      })
    })
  })

  context('get all messages', (assert, next) => {
    var core = new Kappa()
    var feed = hypercore(ram, { valueEncoding: 'json' })
    var db = memdb()

    var source = createHypercoreSource({ feed, db: sub(db, 'state') })
    core.use('query', source, Query(sub(db, 'view'), {
      indexes: [{ key: 'log', value: [['value', 'timestamp']] }],
      getMessage: fromHypercore(feed)
    }))

    feed.append(seeds, (err, _) => {
      assert.error(err, 'no error')

      let query = [{ $filter: { value: { timestamp: { $gt: 0 } } } }]

      core.ready('query', () => {
        collect(core.view.query.read({ query }), (err, msgs) => {
          var check = seeds
          assert.equal(msgs.length, check.length, 'gets the same number of messages')
          assert.same(msgs.map((msg) => msg.value), check, 'querys messages using correct index')
          next()
        })
      })
    })
  })

  context('fil (used by cobox state feed)', (assert, next) => {
    var core = new Kappa()
    var feed = hypercore(ram, { valueEncoding: 'json' })
    var db = memdb()

    var source = createHypercoreSource({ feed, db: sub(db, 'state') })
    core.use('query', source, Query(sub(db, 'view'), {
      indexes: [
        { key: 'log', value: [['value', 'timestamp']] },
        { key: 'fil', value: [['value', 'filename']] },
      ],
      getMessage: fromHypercore(feed)
    }))

    feed.append(drive, (err, _) => {
      assert.error(err, 'no error')
      let filename = 'hello.txt'
      let helloQuery = [{ $filter: { value: { filename, timestamp: { $gt: 0 } } } }]

      core.ready('query', () => {
        collect(core.view.query.read({ query: helloQuery }), (err, msgs) => {
          var check = drive.filter((msg) => msg.filename === filename)
          assert.equal(msgs.length, check.length, 'gets the same number of messages')
          assert.same(msgs.map((msg) => msg.value), check, 'querys messages using correct index')

          let fileQuery = [{ $filter: { value: { timestamp: { $gt: 0 } } } }]

          collect(core.view.query.read({ query: fileQuery }), (err, msgs) => {
            var check = drive
            assert.equal(msgs.length, check.length, 'gets the same number of messages')
            assert.same(msgs.map((msg) => msg.value), check, 'querys messages using correct index')
            next()
          })
        })
      })
    })
  })

  context('live', (assert, next) => {
    var core = new Kappa()
    var feed = hypercore(ram, { valueEncoding: 'json' })
    var db = memdb()

    var source = createHypercoreSource({ feed, db: sub(db, 'state') })
    core.use('query', source, Query(sub(db, 'view'), {
      indexes: [{ key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }],
      getMessage: fromHypercore(feed)
    }))

    feed.append(seeds.slice(0, 2), (err, _) => {
      assert.error(err, 'no error')

      let count = 0
      let check = seeds.filter((msg) => msg.type === 'chat/message')

      let query = [{ $filter: { value: { type: 'chat/message' } } }]

      core.ready('query', () => {
        var stream = core.view.query.read({ live: true, query })

        stream.on('data', (msg) => {
          if (msg.sync) return done()
          assert.same(check[count], msg.value, 'streams each message live')
          ++count
          done()
        })

        feed.append(seeds.slice(3, 5), (err, _) => {
          assert.error(err, 'no error')
        })

        function done (err) {
          if (count === check.length) return next()
        }
      })
    })
  })
})

describe('multifeed', (context) => {
  context('aggregates all feeds', (assert, next) => {
    var core = new Kappa()
    var feeds = multifeed(ram, { valueEncoding: 'json' })
    var db = memdb()

    var source = createMultifeedSource({ feeds, db: sub(db, 'state') })
    core.use('query', source, Query(sub(db, 'view'), {
      indexes: [{ key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }],
      getMessage: fromMultifeed(feeds)
    }))

    var name1 = crypto.randomBytes(16).toString('hex')
    var name2 = crypto.randomBytes(16).toString('hex')
    var query = [{ $filter: { value: { type: 'chat/message' } } }]
    var timestamp = Date.now()
    var count = 0

    setup(name1, (err, feed1) => {
      assert.error(err, 'no error')

      setup(name2, (err, feed2) => {
        assert.error(err, 'no error')

        debug(`initialised feed1: ${feed1.key.toString('hex')} feed2: ${feed2.key.toString('hex')}`)
        assert.same(2, feeds.feeds().length, 'two local feeds')

        core.ready('query', () => {
          collect(core.view.query.read({ query }), (err, msgs) => {
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
      feeds.writer(name, (err, feed) => {
        feed.append({
          type: 'chat/message',
          timestamp: timestamp + count,
          content: { body: name }
        }, (err, seq) => {
          count++
          cb(null, feed)
        })
      })
    }
  })

  context('aggregates all feeds, colliding timestamps', (assert, next) => {
    var core = new Kappa()
    var feeds = multifeed(ram, { valueEncoding: 'json' })
    var db = memdb()

    var source = createMultifeedSource({ feeds, db: sub(db, 'state') })
    core.use('query', source, Query(sub(db, 'view'), {
      indexes: [{ key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }],
      getMessage: fromMultifeed(feeds)
    }))

    var name1 = crypto.randomBytes(16).toString('hex')
    var name2 = crypto.randomBytes(16).toString('hex')
    var query = [{ $filter: { value: { type: 'chat/message' } } }]
    var timestamp = Date.now()

    setup(name1, (err, feed1) => {
      assert.error(err, 'no error')
      setup(name2, (err, feed2) => {
        assert.error(err, 'no error')

        core.ready('query', () => {
          collect(core.view.query.read({ query }), (err, msgs) => {
            assert.error(err, 'no error')
            assert.ok(msgs.length === 2, 'returns two messages')
            assert.same(msgs, [
              { key: feed1.key.toString('hex'), seq: 0, value: { type: 'chat/message', timestamp, content: { body: name1 } } },
              { key: feed2.key.toString('hex'), seq: 1, value: { type: 'chat/message', timestamp, content: { body: name2 } }}
            ], 'aggregates all feeds')
            next()
          })
        })
      })
    })

    function setup (name, cb) {
      feeds.writer(name1, (err, feed) => {
        feed.append({
          type: 'chat/message',
          timestamp,
          content: { body: name }
        }, (err, seq) => {
          cb(null, feed)
        })
      })
    }
  })

  context('live', (assert, next) => {
    var core = new Kappa()
    var feeds = multifeed(ram, { valueEncoding: 'json' })
    var db = memdb()

    var source = createMultifeedSource({ feeds, db: sub(db, 'state') })
    core.use('query', source, Query(sub(db, 'view'), {
      indexes: [{ key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }],
      getMessage: fromMultifeed(feeds)
    }))

    var name1 = crypto.randomBytes(16).toString('hex')
    var name2 = crypto.randomBytes(16).toString('hex')
    var query = [{ $filter: { value: { type: 'chat/message' } } }]
    let timestamp = Date.now()

    feeds.writer(name1, (err, feed1) => {
      assert.error(err, 'no error')

      feeds.writer(name2, (err, feed2) => {
        assert.error(err, 'no error')

        let count = 0
        let check = seeds
          .map((msg) => Object.assign(msg, { timestamp }))
          .filter((msg) => msg.type === 'chat/message')

        core.ready('query', () => {
          var stream = core.view.query.read({ live: true, old: false, query })

          stream.on('data', (msg) => {
            assert.same(msg.value, check[count], 'streams each message live')
            ++count
            done()
          })
        })

        core.ready('query', () => {
          var batch2 = seeds.slice(3, 5)
          feed2.append(batch2, (err, _) => {
            assert.error(err, 'no error')
          })
        })

        core.ready('query', () => {
          var batch1 = seeds.slice(0, 3)
          feed1.append(batch1, (err, _) => {
            assert.error(err, 'no error')
          })
        })

        function done () {
          if (count === check.length) return next()
        }
      })
    })
  })
})

describe('multiple multifeeds', (context) => {
  context('aggregates all valid messages from all feeds when querying', (assert, next) => {
    var core1 = new Kappa()
    var core2 = new Kappa()
    var feeds1 = multifeed(ram, { valueEncoding: 'json' })
    var feeds2 = multifeed(ram, { valueEncoding: 'json' })
    var db1 = memdb()
    var db2 = memdb()

    var source1 = createMultifeedSource({ feeds: feeds1, db: sub(db1, 'state') })
    core1.use('query', source1, Query(sub(db1, 'view'), {
      indexes: [{ key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }],
      getMessage: fromMultifeed(feeds1)
    }))

    var source2 = createMultifeedSource({ feeds: feeds2, db: sub(db2, 'state') })
    core2.use('query', source2, Query(sub(db2, 'view'), {
      indexes: [{ key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }],
      getMessage: fromMultifeed(feeds2)
    }))

    var query = [{ $filter: { value: { type: 'chat/message' } } }]
    var timestamp = Date.now()
    var count = 0

    setup(feeds1, (err, feed1) => {
      assert.error(err, 'no error')
      setup(feeds2, (err, feed2) => {
        assert.error(err, 'no error')

        debug(`initialised core1: ${feed1.key.toString('hex')} core2: ${feed2.key.toString('hex')}`)
        assert.same(1, feeds1.feeds().length, 'one feed')
        assert.same(1, feeds2.feeds().length, 'one feed')

        core1.ready('query', () => {
          collect(core1.view.query.read({ query }), (err, msgs) => {
            assert.error(err, 'no error')
            assert.ok(msgs.length === 1, 'returns a single message')

            replicate(feeds1, feeds2, (err) => {
              assert.error(err, 'no error')
              assert.same(2, feeds1.feeds().length, `first core has second core's feed`)
              assert.same(2, feeds2.feeds().length, `second core has first core's feed`)

              core2.ready('query', () => {
                collect(core2.view.query.read({ query }), (err, msgs) => {
                  assert.error(err, 'no error')
                  assert.ok(msgs.length === 2, 'returns two messages')
                  assert.same(msgs, [
                    { key: feed1.key.toString('hex'), seq: 0, value: { type: 'chat/message', timestamp } },
                    { key: feed2.key.toString('hex'), seq: 0, value: { type: 'chat/message', timestamp: timestamp + 1} }
                  ], 'query aggregates messages from all feeds')
                  next()
                })
              })
            })
          })
        })
      })
    })

    function setup (multifeed, cb) {
      multifeed.writer('local', (err, feed) => {
        feed.append({
          type: 'chat/message',
          timestamp: timestamp + count,
        }, (err, seq) => {
          ++count
          cb(null, feed)
        })
      })
    }
  })

  context('live', (assert, next) => {
    var core1 = new Kappa()
    var core2 = new Kappa()
    var feeds1 = multifeed(ram, { valueEncoding: 'json' })
    var feeds2 = multifeed(ram, { valueEncoding: 'json' })
    var db1 = memdb()
    var db2 = memdb()

    var source1 = createMultifeedSource({ feeds: feeds1, db: sub(db1, 'state') })
    core1.use('query', source1, Query(sub(db1, 'view'), {
      indexes: [{ key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }],
      getMessage: fromMultifeed(feeds1)
    }))

    var source2 = createMultifeedSource({ feeds: feeds2, db: sub(db2, 'state') })
    core2.use('query', source2, Query(sub(db2, 'view'), {
      indexes: [{ key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }],
      getMessage: fromMultifeed(feeds2)
    }))

    let query = [{ $filter: { value: { type: 'user/about' } } }]
    let timestamp = Date.now()
    let feed1Name = { type: 'user/about', timestamp, content: { name: 'Magpie' } }
    let feed2Name = { type: 'user/about', timestamp, content: { name: 'Jackdaw' } }
    let count1 = 0
    let count2 = 0
    let check1 = [feed1Name, feed2Name]
    let check2 = [feed2Name, feed1Name]

    setup(feeds1, (err, feed1) => {
      assert.error(err, 'no error')

      setup(feeds2, (err, feed2) => {
        assert.error(err, 'no error')
        let core1ready, core2ready

        debug(`initialised core1: ${feed1.key.toString('hex')} core2: ${feed2.key.toString('hex')}`)

        core1.ready('query', () => {
          let stream1 = core1.view.query.read({ live: true, old: false, query })

          stream1.on('data', (msg) => {
            debug(`stream 1: ${JSON.stringify(msg, null, 2)}` )
            assert.same(msg.value, check1[count1], 'streams each message live')
            ++count1
            done()
          })

          core1ready = true
          doReplication()
        })

        core2.ready('query', () => {
          let stream2 = core2.view.query.read({ live: true, old: false, query })

          stream2.on('data', (msg) => {
            debug(`stream 2: ${JSON.stringify(msg, null, 2)}` )
            assert.same(msg.value, check2[count2], 'streams each message live')
            ++count2
            done()
          })

          core2ready = true
          doReplication()
        })

        function doReplication () {
          if (!(core1ready && core2ready)) return

          feed1.append(feed1Name, (err, seq) => {
            assert.error(err, 'no error')

            feed2.append(feed2Name, (err, seq) => {
              assert.error(err, 'no error')

              debug('replicating')
              replicate(feeds1, feeds2, (err) => {
                assert.error(err, 'no error')
                assert.same(2, feeds1.feeds().length, `first core has replicated second core's feed`)
                assert.same(2, feeds2.feeds().length, `second core has replicated first core's feed`)
              })
            })
          })
        }
      })
    })

    function done () {
      if (count1 === 2 && count2 === 2) return next()
    }

    function setup (multifeed, cb) {
      multifeed.writer('local', cb)
    }
  })
})
