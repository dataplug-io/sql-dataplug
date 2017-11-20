const _ = require('lodash')
const chalk = require('chalk')
const Promise = require('bluebird')
const moment = require('moment')
const logger = require('winston')
const { FlattenedTransformStream } = require('@dataplug/dataplug-flatters')
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
    alias: 'm',
    describe: 'Column name to use as "missing" marker',
    type: 'string'
  })
  .option('timestamp', {
    alias: 't',
    describe: 'Column name to use as "timestamp" marker',
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
  .option('abort', {
    alias: 'a',
    describe: 'Abort on any error',
    type: 'boolean',
    default: false
  })
declaration.handler = (argv, collection) => {
  const progress = !argv.progress ? null : new Progress({
    sqlized: (value) => chalk.yellow('?') + ` sqlized: ${value}`
  })
  if (progress) {
    progress.start()
  }

  let prologueModifier = null
  if (argv.missing) {
    prologueModifier = _.assign({}, prologueModifier, _.set({}, argv.missing, true))
  }

  let dataModifier = null
  let metadataModifier = null
  if (argv.missing) {
    dataModifier = _.assign({}, dataModifier, _.set({}, argv.missing, false))
  }
  if (argv.timestamp) {
    dataModifier = _.assign({}, dataModifier, _.set({}, argv.timestamp, moment.utc().format()))
  }
  if (dataModifier) {
    dataModifier = (data) => {
      return dataModifier
        ? _.merge({}, data, dataModifier)
        : data
    }

    metadataModifier = (entity) => {
      const metadata = {
        fields: {}
      }
      if (argv.missing) {
        metadata.fields[argv.missing] = {
          type: 'boolean'
        }
      }
      if (argv.timestamp) {
        metadata.fields[argv.timestamp] = {
          type: 'timestamp'
        }
      }
      return metadata
    }
  }

  const reader = new JsonStreamReader()
  const transform = new FlattenedTransformStream(dataModifier, metadataModifier, argv.abort)
  const writer = new SqlSerializerStream(argv.dialect, collection.name, undefined, {
    prologue: argv.truncate
      ? 'truncate'
      : (prologueModifier !== null ? prologueModifier : undefined),
    preprocessor: argv.delete ? 'delete-by-identity' : undefined,
    postprocessor: (argv.onConflict !== 'fail')
      ? `${argv.onConflict}-on-conflict`
      : undefined,
    abortOnError: argv.abort
  })
  const emitter = new SqlCommandsWriter({
    queriesDelimiter: argv.queriesDelimiter,
    chunksDelimiter: argv.chunksDelimiter,
    abortOnError: argv.abort
  })

  if (progress) {
    progress.sqlized = 0
  }

  new Promise((resolve, reject) => {
    process.stdin
      .on('error', (error) => {
        logger.log('error', 'Error while reading data from stdin:', error)
        reject(error)
      })
      .pipe(reader)
      .on('error', (error) => {
        logger.log('error', 'Error while deserializing data as JSON:', error)
        reject(error)
      })
      .pipe(transform)
      .on('error', (error) => {
        logger.log('error', 'Error while transform data:', error)
        reject(error)
      })
      .on('data', () => {
        if (progress) {
          progress.sqlized += 1
        }
      })
      .pipe(writer)
      .on('error', (error) => {
        logger.log('error', 'Error while serialize data as SQL:', error)
        reject(error)
      })
      .pipe(emitter)
      .on('error', (error) => {
        logger.log('error', 'Error while emitting SQL statements:', error)
        reject(error)
      })
      .pipe(process.stdout)
      .on('error', (error) => {
        logger.log('error', 'Error while writing data to stdout:', error)
        reject(error)
      })
      .on('unpipe', () => {
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
    logger.log('error', chalk.red('!'), 'Aborted due to:', error)
    process.exit(70)
  })
}

module.exports = declaration
