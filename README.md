# Kappa View Query

> provides a querying interface for custom indexes – inspired by [ssb-query](https://github.com/ssbc/ssb-query) which uses [map-filter-reduce](https://github.com/dominictarr/map-filter-reduce) and [flumeview-query](https://github.com/flumedb/flumeview-query) – as a kappa-core materialised view.

## Usage

```js
const kappa = require('kappa-core')
const Query = require('kappa-view-query')
const ram = require('random-access-memory')

const memdb = require('memdb')
const level = require('level') 

const pull = require('pull-stream')

const core = kappa(ram, { valueEncoding: 'json'  })

// either memdb for a memoryDown level instance, or
// write indexes to a leveldb instance stored as files 
const db = memdb() || level('/tmp/db')

// custom validator enabling you to write your own message schemas
const validator = function (msg) {
  if (typeof msg !== 'object') return null
  if (typeof msg.value !== 'object') return null
  if (typeof msg.value.timestamp !== 'number') return null
  if (typeof msg.value.type !== 'string') return null
  return msg
}

// some example indexes, ported over from ssb-query
const indexes = [
  // indexes all messages from all feeds by timestamp 
  { key: 'log', value: ['value', 'timestamp'] },
  // indexes all messages from all feeds by message type, then by timestamp 
  { key: 'typ', value: [['value', 'type'], ['value', 'timestamp']] }
] 

core.use('query', Query(db, core, { indexes, validator })) 

core.ready(function () {
  core.writer(function (err, feed) => {
    // append messages to feed in the 'wrong' order
    const data = [{
      type: 'chat/message',
      timestamp: 1561996331739,
      content: { body: 'First message' } 
    }, {
      type: 'user/about',
      timestamp: 1561996331739,
      content: { name: 'Grace' }
    }, {
    }, {
      type: 'chat/message',
      timestamp: 1561996331741,
      content: { body: 'Third message' } 
    }, {
      type: 'chat/message',
      timestamp: 1561996331740,
      content: { body: 'Second message' } 
    }, {
      type: 'user/about',
      timestamp: 1561996331754,
      content: { name: 'Poison Ivy' }
    }]

    feed.append(data)
  })

  // get all messages of type 'chat/message', and order by timestamp
  const query = [{ $filter: { value: { type: 'chat/message' } } }]

  pull(
    // the view will use flumeview-query's scoring system to choose 
    // the most relevant index, in this case, the second index – 'typ'
    core.api.query.read({ live: true, reverse: true, query }),
    pull.collect((err, msgs) => {
      console.log(msgs)
      // returns a list of messages filtered by type then ordered by timestamp 
      // [{
      //   type: 'chat/message',
      //   timestamp: 1561996331739,
      //   content: { body: 'First message' } 
      // }, {
      //   type: 'chat/message',
      //   timestamp: 1561996331740,
      //   content: { body: 'Second message' } 
      // }, {
      //   type: 'chat/message',
      //   timestamp: 1561996331741,
      //   content: { body: 'Third message' } 
      // }]
    })
  )
})
```

## API

```
const View = require('kappa-view-query') 
```

Expects a LevelUP or LevelDOWN instance `leveldb`.

## Install

```bash
$ npm install kappa-view-query 
```

## Acknowledgments 
kappa-view-query was inspired by flumeview-query and programmed by [@kieran](https://github.com/KGibb8/) and [@dominictarr](https://github.com/dominictarr).

## TODO

* Remove need for `core` to be passed as an argument, instead access from within the view itself, which should be available from kappa-core after being plugged in
* Inherit test suite from flumeview-query
