const _ = require('lodash')
const chalk = require('chalk')
const Promise = require('bluebird')
const { FlattenedTransformStream } = require('@dataplug/dataplug')
const { SqlSerializerStream, SqlCommandsWriter } = require('@dataplug/dataplug-sql')
const { JsonStreamReader } = require('@dataplug/dataplug-json')
const { Progress } = require('@dataplug/dataplug-cli')

let declaration = {
  command: 'serialize',
  description: 'Transforms input stream to SQL-insert commands'
}
declaration.builder = (yargs) => yargs
  .option('progress', {
    alias: 'p',
    describe: 'Show progress in console'
  })
  .option('dialect', {
    alias: 'l',
    describe: 'SQL dialect to use',
    enum: [
      'postgres', 'postgresql', 'pg'
    ],
    required: true
  })
  .option('truncate', {
    describe: 'Truncate entity table prior to first insert',
    type: 'boolean',
    default: false
  })
  .option('missing', {
    describe: 'Column name to use as "missing" marker',
    type: 'string'
  })
  .option('delele', {
    alias: 'd',
    describe: 'Delete entries by identity prior to inserting',
    type: 'boolean',
    default: false
  })
  .option('on-conflict', {
    describe: 'Behavior on conflict',
    enum: [
      'update',
      'skip',
      'fail'
    ],
    default: 'fail'
  })
  .option('chunks-delimiter', {
    description: 'String used as a chunks delimiter',
    type: 'string',
    default: '\\n'
  })
  .option('queries-delimiter', {
    description: 'String used as a queries delimiter',
    type: 'string',
    default: '\\n'
  })
  .coerce(['chunks-delimiter', 'queries-delimiter'], value => JSON.parse(JSON.stringify(value).replace(/\\\\/g, '\\')))
declaration.handler = (argv, collection) => {
  const progress = !argv.progress ? null : new Progress({
    sqlized: (value) => chalk.yellow('?') + ` sqlized: ${value}`
  })
  if (progress) {
    progress.start()
  }

  process.on('SIGINT', function () {
    process.stdin.unpipe()
  })

  const nonMissingMixin = !argv.missing ? undefined : _.set({}, argv.missing, false)
  const missingMixin = !argv.missing ? undefined : _.set({}, argv.missing, true)

  const reader = new JsonStreamReader()
  const transform = new FlattenedTransformStream((entry) => {
    if (!argv.missing) {
      return entry
    }

    return _.assign({}, entry, nonMissingMixin)
  })
  const writer = new SqlSerializerStream(argv.dialect, collection.name, undefined, {
    prologue: argv.truncate
      ? 'truncate'
      : (argv.missing ? missingMixin : undefined),
    preprocessor: argv.delete ? 'delete-by-identity' : undefined,
    postprocessor: (argv.onConflict !== 'fail')
      ? `${argv.onConflict}-on-conflict`
      : undefined
  })
  const emitter = new SqlCommandsWriter(argv.dialect, {
    queriesDelimiter: argv.queriesDelimiter,
    chunksDelimiter: argv.chunksDelimiter
  })

  if (progress) {
    progress.sqlized = 0
  }

  new Promise((resolve, reject) => {
    process.stdin
      .pipe(reader)
      .on('error', (error) => {
        reject('Failed to deserialize data as JSON: ' + error ? error : 'no specific information')
      })
      .pipe(transform)
      .on('error', (error) => {
        reject('Failed to transfrom data: ' + error ? error : 'no specific information')
      })
      .pipe(writer)
      .on('error', (error) => {
        reject('Failed to serialize data as SQL: ' + error ? error : 'no specific information')
      })
      .pipe(emitter)
      .on('error', (error) => {
        reject('Failed to emit SQL commands: ' + error ? error : 'no specific information')
      })
      .pipe(process.stdout)
      .on('finish', () => {
        resolve()
      })
  })
  .then(() => {
    if (progress) {
      progress.cancel()
    }
  })
  .catch((error) => {
    if (progress) {
      progress.cancel()
    }
    console.error(chalk.red('!'), error || 'Unknown error')
    process.exit(70)
  })
}

module.exports = declaration
