const chalk = require('chalk')
const Promise = require('bluebird')
const { SqlStreamWriter } = require('@dataplug/dataplug-sql')
const { JsonStreamReader } = require('@dataplug/dataplug-json')
const { Progress } = require('@dataplug/dataplug-cli')

let declaration = {
  command: 'sql',
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
  .option('flat', {
    describe: 'Expect containered data on input',
    type: 'boolean',
    default: false
  })
declaration.handler = (argv, collection) => {
  const progress = !argv.progress ? null : new Progress({
    sqlized: (value) => chalk.yellow('?') + ` sqlized: ${value}`
  })
  if (progress) {
    progress.sqlized = 0
    progress.start()
  }

  const reader = new JsonStreamReader()
  const writer = new SqlStreamWriter(argv.dialect, collection.name, argv.flat)
  writer.on('pipe', (source) => {
    process.on('SIGINT', function () {
      source.unpipe(writer)
      writer.end()
    })
  })

  new Promise((resolve, reject) => {
    process.stdin
      .pipe(reader)
      .on('error', (error) => {
        reject('Failed to deserialize data as JSON: ' + error ? error : 'no specific information')
      })
      .pipe(writer)
      .on('error', (error) => {
        reject('Failed to serialize data as SQL: ' + error ? error : 'no specific information')
      })
      .on('end', () => {
        resolve()
      })
      .pipe(process.stdout)
  })
  .then(() => {
    if (progress) {
      progress.cancel()
    }
    process.exit()
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
