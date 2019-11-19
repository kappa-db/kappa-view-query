const { describe } = require('tape-plus')
const kappa = require('kappa-core')
const Query = require('../')
const ram = require('random-access-memory')
const memdb = require('memdb')
const level = require('level')
const collect = require('collect-stream')
const crypto = require('crypto')
const debug = require('debug')('kappa-view-query')

const seeds = require('./seeds.json')
  .sort((a, b) => a.timestamp > b.timestamp ? +1 : -1)

const drive = require('./drive.json')
  .sort((a, b) => a.timestamp > b.timestamp ? +1 : -1)

const { cleanup, tmp, replicate } = require('./util')

describe('basic', (context) => {
  let core, db, indexes

  context.beforeEach((c) => {
    core = kappa(ram, { valueEncoding: 'json'  })
    db = memdb()

    indexes = [
      { key: 'log', value: [['value', 'timestamp']] },
      { key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] },
      { key: 'fil', value: [['value', 'filename'], ['value', 'timestamp']] }
    ]

    core.use('query', Query(db, { indexes }))
  })

  context('perform a query', (assert, next) => {
    core.writer('local', (err, feed) => {
      feed.append(seeds, (err, _) => {
        assert.error(err, 'no error')

        let query = [{ $filter: { value: { type: 'chat/message' } } }]

        core.ready('query', () => {
          collect(core.api.query.read({ query }), (err, msgs) => {
            var check = seeds.filter((msg) => msg.type === 'chat/message')

            assert.same(msgs.map((msg) => msg.value), check, 'querys messages using correct index')
            next()
          })
        })
      })
    })
  })

  context('get all messages', (assert, next) => {
    core.writer('local', (err, feed) => {
      feed.append(seeds, (err, _) => {
        assert.error(err, 'no error')

        let query = [{ $filter: { value: { timestamp: { $gt: 0 } } } }]

        core.ready('query', () => {
          collect(core.api.query.read({ query }), (err, msgs) => {
            var check = seeds
            assert.equal(msgs.length, check.length, 'gets the same number of messages')
            assert.same(msgs.map((msg) => msg.value), check, 'querys messages using correct index')
            next()
          })
        })
      })
    })
  })

  context('fil index - get all changes to a specific file, then get all changes to all files', (assert, next) => {
    core.writer('local', (err, feed) => {
      feed.append(drive, (err, _) => {
        assert.error(err, 'no error')
        let filename = 'hello.txt'
        let helloQuery = [{ $filter: { value: { filename, timestamp: { $gt: 0 } } } }]

        core.ready('query', () => {
          collect(core.api.query.read({ query: helloQuery }), (err, msgs) => {
            var check = drive.filter((msg) => msg.filename === filename)
            assert.equal(msgs.length, check.length, 'gets the same number of messages')
            assert.same(msgs.map((msg) => msg.value), check, 'querys messages using correct index')

            let fileQuery = [{ $filter: { value: { timestamp: { $gt: 0 } } } }]

            collect(core.api.query.read({ query: fileQuery }), (err, msgs) => {
              var check = drive
              assert.equal(msgs.length, check.length, 'gets the same number of messages')
              assert.same(msgs.map((msg) => msg.value), check, 'querys messages using correct index')
              next()
            })
          })
        })
      })
    })
  })

  context('live', (assert, next) => {
    core.writer('local', (err, feed) => {
      assert.error(err, 'no error')
      feed.append(seeds.slice(0, 2), (err, _) => {
        assert.error(err, 'no error')

        let count = 0
        let check = seeds.filter((msg) => msg.type === 'chat/message')

        let query = [{ $filter: { value: { type: 'chat/message' } } }]

        core.ready('query', () => {
          var stream = core.api.query.read({ live: true, query })

          stream.on('data', (msg) => {
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
})

describe('multiple feeds', (context) => {
  let core, db
  let name1, name2

  context.beforeEach((c) => {
    core = kappa(ram, { valueEncoding: 'json'  })
    db = memdb()

    indexes = [{ key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }]

    name1 = crypto.randomBytes(16).toString('hex')
    name2 = crypto.randomBytes(16).toString('hex')

    core.use('query', Query(db, { indexes }))
  })

  context('aggregates all feeds', (assert, next) => {
    var query = [{ $filter: { value: { type: 'chat/message' } } }]
    var timestamp = Date.now()
    var count = 0

    setup(name1, (err, feed1) => {
      assert.error(err, 'no error')

      setup(name2, (err, feed2) => {
        assert.error(err, 'no error')

        debug(`initialised feed1: ${feed1.key.toString('hex')} feed2: ${feed2.key.toString('hex')}`)
        assert.same(2, core.feeds().length, 'two local feeds')

        core.ready('query', () => {
          collect(core.api.query.read({ query }), (err, msgs) => {
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
      core.writer(name, (err, feed) => {
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
    var query = [{ $filter: { value: { type: 'chat/message' } } }]
    var timestamp = Date.now()

    setup(name1, (err, feed1) => {
      assert.error(err, 'no error')
      setup(name2, (err, feed2) => {
        assert.error(err, 'no error')

        core.ready('query', () => {
          collect(core.api.query.read({ query }), (err, msgs) => {
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
      core.writer(name1, (err, feed) => {
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
    var query = [{ $filter: { value: { type: 'chat/message' } } }]
    let timestamp = Date.now()

    core.writer(name1, (err, feed1) => {
      assert.error(err, 'no error')

      core.writer(name2, (err, feed2) => {
        assert.error(err, 'no error')

        let count = 0
        let check = seeds
          .map((msg) => Object.assign(msg, { timestamp }))
          .filter((msg) => msg.type === 'chat/message')

        // TODO: we have a race condition issue
        // if we wrap this batch in core.ready('query')
        // the first batch appears to come through
        // the query twice... once before sync: true,
        // once after sync: true, does this mean the query
        // is being re-run after sync is true?
        var batch1 = seeds.slice(0, 3)
        feed1.append(batch1, (err, _) => {
          assert.error(err, 'no error')
        })

        core.ready('query', () => {
          var stream = core.api.query.read({ live: true, query })

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

        function done () {
          if (count === check.length) return next()
        }
      })
    })
  })
})

describe('multiple cores', (context) => {
  let core1, db1
  let core2, db2

  context.beforeEach((c) => {
    core1 = kappa(ram, { valueEncoding: 'json' })
    core2 = kappa(ram, { valueEncoding: 'json' })

    indexes = [{ key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }]

    core1.use('query', Query(memdb(), { indexes }))
    core2.use('query', Query(memdb(), { indexes }))
  })

  context('aggregates all valid messages from all feeds when querying', (assert, next) => {
    var query = [{ $filter: { value: { type: 'chat/message' } } }]
    var timestamp = Date.now()
    var count = 0

    setup(core1, (err, feed1) => {
      assert.error(err, 'no error')
      setup(core2, (err, feed2) => {
        assert.error(err, 'no error')

        debug(`initialised core1: ${feed1.key.toString('hex')} core2: ${feed2.key.toString('hex')}`)
        assert.same(1, core1.feeds().length, 'one feed')
        assert.same(1, core2.feeds().length, 'one feed')

        core1.ready('query', () => {
          core2.ready('query', () => {
            collect(core1.api.query.read({ query }), (err, msgs) => {
              assert.error(err, 'no error')
              assert.ok(msgs.length === 1, 'returns a single message')

              replicate(core1, core2, (err) => {
                assert.error(err, 'no error')
                assert.same(2, core1.feeds().length, `first core has second core's feed`)
                assert.same(2, core2.feeds().length, `second core has first core's feed`)

                collect(core2.api.query.read({ query }), (err, msgs) => {
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

    function setup (kcore, cb) {
      kcore.writer('local', (err, feed) => {
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
    let query = [{ $filter: { value: { type: 'user/about' } } }]
    let timestamp = Date.now()
    let feed1Name = { type: 'user/about', timestamp, content: { name: 'Magpie' } }
    let feed2Name = { type: 'user/about', timestamp, content: { name: 'Jackdaw' } }
    let count1 = 0
    let count2 = 0
    let check1 = [feed1Name, feed2Name]
    let check2 = [feed2Name, feed1Name]

    setup(core1, (err, feed1) => {
      assert.error(err, 'no error')

      setup(core2, (err, feed2) => {
        assert.error(err, 'no error')

        core1.ready('query', () => {
          let stream1 = core1.api.query.read({ live: true, query })

          stream1.on('data', (msg) => {
            console.log("STREAM1", msg)
            assert.same(msg.value, check1[count1], 'streams each message live')
            ++count1
            done()
          })

          core2.ready('query', () => {
            let stream2 = core2.api.query.read({ live: true, query })

            stream2.on('data', (msg) => {
              console.log("STREAM2", msg)
              assert.same(msg.value, check2[count2], 'streams each message live')
              ++count2
              done()
            })
          })
        })

        feed1.append(feed1Name, (err, seq) => {
          assert.error(err, 'no error')

          feed2.append(feed2Name, (err, seq) => {
            assert.error(err, 'no error')

            console.log("REPLICATING")
            replicate(core1, core2, (err) => {
              assert.error(err, 'no error')
              assert.same(2, core1.feeds().length, `first core has replicated second core's feed`)
              assert.same(2, core2.feeds().length, `second core has replicated first core's feed`)
            })
          })
        })
      })
    })

    function done () {
      if (count1 === 2 && count2 === 2) return next()
    }

    function setup (kcore, cb) {
      kcore.writer('local', cb)
    }
  })
})
