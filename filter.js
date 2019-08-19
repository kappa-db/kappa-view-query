const toStream = require('pull-stream-to-stream')
const toPull = require('stream-to-pull-stream')
const MFR = require('map-filter-reduce')
const pull = require('pull-stream')

module.exports = function Filter(source, opts = {}) {
  return toStream(
    pull(
      toPull(source),
      Array.isArray(opts.query) ? MFR(opts.query) : pull.through(),
      opts.limit && pull.take(opts.limit)
    )
  )
}
