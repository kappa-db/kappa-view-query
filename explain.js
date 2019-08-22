const Query = require('./query')
const Select = require('./select')
const { findByPath } = require('./util')
const debug = require('debug')('kappa-view-query')

module.exports = function Explain (indexes) {
  return function explain (opts = {}) {
    var query, sort

    if (Array.isArray(opts.query)) {
      query = opts.query[0].$filter || {}
      sort = opts.query[opts.query.length-1].$sort
    } else if (opts.query) {
      query = opts.query
    } else {
      query = {}
    }

    var r = sort && { index: findByPath(indexes, sort), scores: {} } || Select(indexes, query, true)

    var index = r.index

    var _opts = Query(index, query, index.exact)
    _opts.reverse = !!opts.reverse
    _opts.old = (opts.old !== false)
    _opts.live = (opts.live === true || opts.old === false)
    _opts.createStream = index.createStream
    _opts.index = index.key,
    _opts.scores = r.scores

    return _opts
  }
}
