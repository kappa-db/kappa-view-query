const memdb = require('memdb')
const Query = require('../')
const ram = require('random-access-memory')
const kappa = require('kappa-core')

module.exports = function (name, tests, memory) {
  const core = kappa(ram, { valueEncoding: 'json' })
  core.use('query', Query(memdb, core, { indexes: tests.indexes }))

  core.ready(function () {
    core.writer('local', function (err, feed) {
      tests(feed.append, core.api.query.read, (cb) => cb())
    })
  })
}
