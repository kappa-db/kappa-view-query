# Kappa View Query

`kappa-view-query` is a materialised view to be used with kappa-core. It provides an API that allows you to define your own indexes and execute custom [map-filter-reduce](https://github.com/dominictarr/map-filter-reduce) queries over a collection of hypercores.

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

```js
const kappa = require('kappa-core')
const Query = require('./')
const ram = require('random-access-memory')
const memdb = require('memdb')

// Initialised your kappa-core back-end
const core = kappa(ram, { valueEncoding: 'json'  })
const db = memdb()

// Define a validator or a message decoder to determine if a message should be indexed or not
function validator (msg) {
  if (typeof msg !== 'object') return null
  if (typeof msg.value !== 'object') return null
  if (typeof msg.value.timestamp !== 'number') return null
  if (typeof msg.value.type !== 'string') return null
  return msg
}

// here's an alternative using protocol buffers, assuming a message schema exists
const { Message } = protobuf(fs.readFileSync(path.join(path.resolve(__dirname), 'message.proto')))

function validator (msg) {
  try { msg.value = Message.decode(msg.value) }
  catch (err) { return console.error(err) && false }
  return msg
}

// Define a set of indexes under a namespace
const indexes = [
  { key: 'log', value: ['value', 'timestamp'] },
  { key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] },
  { key: 'cha', value: [['value', 'type'], ['value', 'content', 'channel']] }
]

core.use('query', Query(db, { indexes, validator }))

core.writer('local', (err, feed) => {
  // Populate our feed with some messages
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

  feed.append(data, (err, seq) => {
    // Define a query: filter where the message value contains type 'chat/message', and the content references the channel 'dogs'
    const query = [{ $filter: { value: { type: 'chat/message', content: { channel: 'dogs' } } } }]

    core.ready('query', () => {
      // For static queries
      collect(core.api.query.read({ query }), (err, msgs) => {
        console.log(msgs)

        // Logs all messages of type chat/message that reference the dogs channel, and order by timestamp...
        // {
        //   type: 'chat/message',
        //   timestamp: 1561996331743,
        //   content: { channel: 'dogs', body: 'Lurchers rule' }
        // }
        // {
        //   type: 'chat/message',
        //   timestamp: Date.now(),
        //   content: { channel: 'dogs', body: 'But sometimes I prefer labra-doodles' }
        // }
      })
    })
  })
})
```

## API

```js
const View = require('kappa-view-query') 
```

Expects a LevelUP or LevelDOWN instance `leveldb`.

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
kappa-view-query was built by [@kyphae](https://github.com/kyphae/) and assisted by [@dominictarr](https://github.com/dominictarr). It uses [@dominictarr](https://github.com/dominictarr)'s scoring system and query interface.

## Version Changes
Version 2 - Updated to remove `pull-stream` and `flumeview-query` as a core dependency for better compatibility with the rest of the `kappa-db` ecosystem. `api.query.read` now returns a regular readable node stream. In order to continue to use pull-streams, check out an updated fork of V1: [kappa-view-pull-query](https://github.com/kappa-db/kappa-view-pull-query)

## Todos
* [ ] write more comprehensive tests to ensure we're properly using map-filter-reduce
* [ ] write tests for allowing different kinds of validators
* [ ] make it so we can use multiple message schemas, and it selects the most appropriate schema based on the query
