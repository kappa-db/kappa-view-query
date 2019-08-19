const Query = require('./query')
const Select = require('./select')
const { findByPath } = require('./util')

module.exports = function Explain (indexes, scan) {
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

    if(!index) return {
      scan: true,
      createStream: scan,
      reverse: !!opts.reverse,
      live: !!(opts.live === true || opts.old === false),
      old: opts.old !== false,
      sync: !!opts.sync,
    }

    var _opts = Query(index, query, index.exact)
    _opts.reverse = !!opts.reverse
    _opts.old = (opts.old !== false)
    _opts.live = (opts.live === true || opts.old === false)
    _opts.createStream = index.createStream
    _opts.sync = opts.sync
    _opts.index = index.key,
    _opts.scores = r.scores

    return _opts
  }
}

