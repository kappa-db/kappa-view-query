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
  if('string' === typeof key) return obj[key]
  for(var i in key) {
    obj = obj[key[i]]
    if(!obj) return obj
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

function isFunction (variable) {
  return typeof variable === 'function'
}

module.exports = {
  has,
  get,
  set,
  findByPath,
  isFunction
}
