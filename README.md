# Kappa View Query

`kappa-view-query` is a materialised view to be used with kappa-core. It provides an API that allows you to define your own indexes and execute custom [map-filter-reduce](https://github.com/dominictarr/map-filter-reduce) queries over your indexes.

`kappa-view-query` is inspired by [flumeview-query](https://github.com/flumedb/flumeview-query). It uses the same scoring system for determining the most efficient index relevant to the provided query.

## How it works

`kappa-view-query` uses a key / value store to compose a single index either in memory (using `memdb`) or stored as a file (using `level`). Each time a message is published to a feed, or is received via replication, `kappa-view-query` checks to see if any of the message's fields match any of the indexes.

We can define an index like this:

```js
{
  key: 'typ',
  value: [
    ['value', 'type'],
    ['value', 'timestamp']
  ]
}
```

This above index tells our view to store all messages that map to the data structure `value.type` and `value.timestamp`. If a message hitting the view does, it will save a reference to this message in our key / value store, where the matching field names along with the name of the index in question are compiled down into a single string. The value is a reference to the feed key and the sequence number, so we can retrieve that message from the correct hypercore later when we perform a query.

For example:

```js
{
  key: 'typ!chat/message!1566481592277',
  value: 'f38b5a5e9603ffc6c24f4431c271999f08f43fc67379faf13c9d75adda01e63c@3'
}
```

Lets write a query. For example, say we want all messages of type `chat/message` published between 13:00 and 15:00 on 22-08-2019, here's what our query would look like...

```js
var query = [{
  $filter: {
    value: {
      type: 'chat/message',
      timestamp: { $gte: 1566486000000, $lte: 1566478800000 }
    }
  }
}]
```

When we execute this query, our scoring system will first determine which index we previously provided gives us the best lens on the data. It does this by matching the requested fields, in this case, `value.type` and `value.timestamp`. The scoring system can be found at [query.js](./query.js).

In the case of the above dataset and query, the closest matching index is the one we provided above, named `typ`. At this point, `kappa-view-query` can then reduce the scope of our index file significantly, by filtering all references in our level or memdb, greater than or equal to `typ!chat/message!1566486000000`, but less than or equal to `type!chat/message!1566478800000`. This gives us a subset of references with which we can  fetch the actual messages from our hypercore feeds.

## Usage

### Hypercore

This example uses a single hypercore and collects all messages at a given point in time.

```js
const Kappa = require('kappa-core')
const hypercore = require('hypercore')
const ram = require('random-access-memory')
const collect = require('collect-stream')
const memdb = require('memdb')
const sub = require('subleveldown')

const Query = require('./')
const { validator, fromHypercore } = require('./util')
const { cleaup, tmp } = require('./test/util')

const seedData = require('./test/seeds.json')

const core = new Kappa()
const feed = hypercore(ram, { valueEncoding: 'json' })
const db = memdb()

core.use('query', createHypercoreSource({ feed, db: sub(db, 'state') }), Query(sub(db, 'view'), {
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
    const query = [{ $filter: { value: { type: 'chat/message' } }]

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
```

### Multifeed

This example uses a multifeed instance for managing hypercores and sets up two live streams to dump messages to the console as they arrive.

```js
const Kappa = require('kappa-core')
const multifeed = require('multifeed')
const ram = require('random-access-memory')
const collect = require('collect-stream')
const memdb = require('memdb')
const sub = require('subleveldown')

const Query = require('./')
const { validator, fromMultifeed } = require('./util')
const { cleaup, tmp } = require('./test/util')

const seedData = require('./test/seeds.json')

const core = new Kappa()
const feeds = multifeed(ram, { valueEncoding: 'json' })
const db = memdb()

core.use('query', createMultifeedSource({ feeds, db: sub(db, 'state') }), Query(sub(db, 'view'), {
  indexes: [
    { key: 'log', value: [['value', 'timestamp']] },
    { key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }
  ],
  validator,
  // make sure you define your own getMessage function, otherwise nothing will be returned by your queries
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
```

## API

```js
const View = require('kappa-view-query') 
```

Expects a LevelUP or LevelDOWN instance `leveldb`.
Expects a `getMessage` function to use your defined index to grab the message from the feed.

```js
// returns a readable stream
core.api.query.read(opts)

// returns information about index performance
core.api.query.explain(opts)

// append an index onto existing set
core.api.query.add(opts)
```

## Install

```bash
$ npm install kappa-view-query
```

## Acknowledgments
kappa-view-query was built by [@kyphae](https://github.com/kyphae/) and assisted by [@dominictarr](https://github.com/dominictarr). It uses [@dominictarr](https://github.com/dominictarr)'s scoring system and query interface from [flumeview-query](https://github.com/flumedb/flumeview-query).

## Releases
### 2.0.0
- Updated to remove the need for `pull-stream` and `flumeview-query` as an external dependency, providing better compatibility with the rest of the `kappa-db` ecosystem.
- `core.api.query.read` returns a regular readable node stream.
- In order to continue to use pull-streams:
  - Use the updated fork of v1 at [kappa-view-pull-query](https://github.com/kappa-db/kappa-view-pull-query),
  - Or make use of [stream-to-pull-stream](https://github.com/pull-stream/pull-stream-to-stream/).

### 2.0.7
- Fixed an outstanding issue where live streams were not working. Queries with `{ live: true }` setup will now properly pipe messages through as they are indexed.
- Fixed an outstanding issue where messages with a matching timestamp were colliding, where the last indexed would over-writing the previous. Messages are now indexed, on top of provided values, on the sequence number and the feed id, for guaranteed uniqueness.

### 3.0.0 (not yet released)
- Updated to use the experimental version of kappa-core which includes breaking API changes. See Frando's [kappa-core fork](https://github.com/Frando/kappa-core/tree/kappa5-new).
