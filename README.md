# Kappa View Query

> provides a querying interface to act on hypercores – inspired by [ssb-query](https://github.com/ssbc/ssb-query) which uses [map-filter-reduce](https://github.com/dominictarr/map-filter-reduce) and [flumeview-query](https://github.com/flumedb/flumeview-query) – as a kappa-core materialised view.

## Usage

```js
const kappa = require('kappa-core')
const Query = require('./')
const ram = require('random-access-memory')

const memdb = require('memdb')
const level = require('level')

const pull = require('pull-stream')

const core = kappa(ram, { valueEncoding: 'json'  })
const db = memdb() || level('/tmp/db')

function validator (msg) {
  if (typeof msg !== 'object') return null
  if (typeof msg.value !== 'object') return null
  if (typeof msg.value.timestamp !== 'number') return null
  if (typeof msg.value.type !== 'string') return null
  return msg
}

const indexes = [
  { key: 'log', value: ['value', 'timestamp'] },
  { key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] },
  { key: 'cha', value: [['value', 'type'], ['value', 'content', 'channel'], ['value', 'timestamp']] }
]

core.use('query', Query(db, { indexes, validator }))

core.ready(() => {
  core.writer('local', (err, feed) => {
    const data = [{
      type: 'chat/message',
      timestamp: 1561996331739,
      content: { body: 'First message' }
    }, {
      type: 'user/about',
      timestamp: 1561996331740,
      content: { name: 'Grace' }
    }, {
    }, {
      type: 'chat/message',
      timestamp: 1561996331742,
      content: { body: 'Third message' }
    }, {
      type: 'chat/message',
      timestamp: 1561996331743,
      content: { channel: 'dogs', body: 'Lurchers rule' }
    }, {
      type: 'chat/message',
      timestamp: 1561996331741,
      content: { body: 'Second message' }
    }, {
      type: 'user/about',
      timestamp: 1561996331754,
      content: { name: 'Poison Ivy' }
    }]

    feed.append(data)
  })

  const query = [{ $filter: { value: { type: 'chat/message', content: { channel: 'dogs' } } } }]

  // For live queries
  core.api.query.read({ live: true, query }).on('data', (err, msg) => {
    // Do stuff with each message
  })

  // For static queries
  collect(core.api.query.read({ query }), (err, msgs) => {
    // Do stuff with the collection
  })

  // logs each message filtered by type then ordered by timestamp 
  // {
  //   key: 'd20dff5a33bbd35596bf355ece0142af2e81aebf192dcbccbc672b964fb374d7',
  //   seq: 3,
  //   value: {
  //     type: 'chat/message',
  //       timestamp: 1561996331741,
  //       content: { body: 'Third message'  }
  //   }
  // }
  // {
  //   key: 'd20dff5a33bbd35596bf355ece0142af2e81aebf192dcbccbc672b964fb374d7',
  //   seq: 4,
  //   value: {
  //     type: 'chat/message',
  //       timestamp: 1561996331740,
  //       content: { body: 'Second message'  }
  //   }
  // }
  // {
  //   key: 'd20dff5a33bbd35596bf355ece0142af2e81aebf192dcbccbc672b964fb374d7',
  //   seq: 0,
  //   value: { type: 'chat/message',
  //       timestamp: 1561996331739,
  //       content: { body: 'First message'  }
  //   }
  // }
})
```

## API

```js
const View = require('kappa-view-query') 
```

Expects a LevelUP or LevelDOWN instance `leveldb`.

```js
// returns a source to be used by pull-stream
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
kappa-view-query was inspired by flumeview-query and programmed by [@kyphae](https://github.com/kyphae/) and [@dominictarr](https://github.com/dominictarr).
