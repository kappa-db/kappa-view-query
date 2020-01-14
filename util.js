var deepEqual = require('deep-equal')

function has (key, obj) {
  if('string' === typeof key)
    return Object.hasOwnProperty.call(obj, key)
  for(var i in key) {
    if(Object.hasOwnProperty.call(obj, key[i]))
      obj = obj[key[i]]
    else
      return false
  }
  return true
}

function get (key, obj) {
  if ('string' === typeof key) return obj[key]
  for (var i in key) {
    obj = obj[key[i]]
    if (!obj) return obj
  }
  return obj
}

function set(key, value, obj) {
  if('string' === typeof key)
    obj[key] = value
  else {
    for(var i = 0 ; i < key.length - 1; i++) {
      obj = (obj[key[i]] = obj[key[i]] || {})
    }
    obj[key[key.length -1]] = value
  }
}

function findByPath (indexes, path) {
  return indexes.find((index) => {
    return deepEqual(index.value, path)
  })
}

function validator (msg) {
  if (typeof msg !== 'object') return null
  if (typeof msg.value !== 'object') return null
  if (typeof msg.value.timestamp !== 'number') return null
  return msg
}

function fromMultifeed (feeds, opts = {}) {
  var validate = opts.validator || function (msg) { return msg }

  return function getMessage (msg, cb) {
    var msgId = msg.value
    var [ feedId, sequence ] = msgId.split('@')
    var feed = feeds.feed(feedId)
    var seq = Number(sequence)

    feed.get(seq, (err, value) => {
      var msg = validate({
        key: feed.key.toString('hex'),
        seq,
        value
      })
      if (!msg) return cb(new Error('message failed to validate'))
      return cb(null, msg)
    })
  }
}

function fromHypercore (feed, opts = {}) {
  var validate = opts.validator || function (msg) { return msg }

  return function getMessage (msg, cb) {
    var msgId = msg.value
    var sequence = msgId.split('@')[1]
    var seq = Number(sequence)

    feed.get(seq, (err, value) => {
      var msg = validate({
        key: feed.key.toString('hex'),
        seq,
        value
      })
      if (!msg) return cb(new Error('message failed to validate'))
      return cb(null, msg)
    })
  }
}

module.exports = {
  has,
  get,
  set,
  findByPath,
  validator,
  fromMultifeed,
  fromHypercore
}
