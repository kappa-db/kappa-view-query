const rimraf = require('rimraf')
const debug = require('debug')('kappa-view-query')
const tmpdir = require('tmp').dirSync
const mkdirp = require('mkdirp')

function cleanup (dirs, cb) {
  if (!cb) cb = noop
  if (!Array.isArray(dirs)) dirs = [dirs]

  var pending = dirs.length

  function next (n) {
    var dir = dirs[n]
    if (!dir) return

    rimraf(dir, (err) => {
      debug(`[CLEANUP] ${dir} : ${ err ? 'failed' : 'success'}`)
      if (err) return done(err)
      process.nextTick(next, n + 1)
      done()
    })
  }

  function done (err) {
    if (err) {
      pending = Infinity
      return cb(err)
    }
    if (!--pending) return cb()
  }

  next(0)
}

function tmp () {
  var path = tmpdir().name
  mkdirp.sync(path)
  debug(`[TEMP] creating temp directory ${path}`)
  return path
}

function uniq (array) {
  if (!Array.isArray(array)) array = [array]
  return Array.from(new Set(array))
}

function noop () {}

module.exports = { cleanup, tmp, uniq }
