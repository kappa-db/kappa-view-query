const tmpdir = require('tmp').dirSync
const mkdirp = require('mkdirp')
const rimraf = require('rimraf')
const debug = require('debug')('kappa-view-query')

function cleanup (dir) {
  rimraf(dir, (err) => {
    debug(`[CLEANUP] ${ err ? 'failed' : 'success'}`)
    if (err) throw (err)
  })
}

function tmp () {
  var root = `./tmp/`
  var path = `.${tmpdir().name}`
  mkdirp.sync(path)
  debug(`[TEMP] ${path}`)
  return { path, root }
}

module.exports = { cleanup, tmp }
