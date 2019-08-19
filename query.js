var Q = require('map-filter-reduce/util')
var { get } = require('./util')

module.exports = function Query (index, query, exact) {
  function bound (value, range, sentinel) {
    return value == null
      ? sentinel
      : Q.isRange(value)
        ? range(value)
        : Q.isExact(value)
          ? value
          : sentinel
  }

  function build (index, map, b) {
    var a = []
    for(var i = 0; i < index.value.length; i++)
      a.push(map(get(index.value[i], query)))
    if(!exact) a.push(b)
    return a
  }

  return {
    gte: build(index, function (value) {
      return bound(value, Q.lower, Q.LO)
    }, Q.LO),
    lte: build(index, function (value) {
      return bound(value, Q.upper, Q.HI)
    }, Q.HI)
  }
}
