const rimraf = require('rimraf')
const debug = require('debug')('kappa-view-query')
const tmpdir = require('tmp').dirSync
const mkdirp = require('mkdirp')
const path = require('path')

function cleanup (dirs, cb) {
  if (!cb) cb = noop
  if (!Array.isArray(dirs)) dirs = [dirs]
  var pending = 1

  function next (n) {
    var dir = dirs[n]
    if (!dir) return done()
    ++pending
    process.nextTick(next, n + 1)

    rimraf(dir, (err) => {
      if (err) return done(err)
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
  var tmpDir = tmpdir().name
  mkdirp.sync(tmpDir)
  return tmpDir
}

function uniq (array) {
  if (!Array.isArray(array)) array = [array]
  return Array.from(new Set(array))
}

function replicate (core1, core2, cb) {
  var stream = core1.replicate()
  stream.pipe(core2.replicate()).pipe(stream)
  stream.on('end', cb)
}

function noop () {}

module.exports = { cleanup, tmp, uniq, replicate }
