const chalk = require('chalk')
const Promise = require('bluebird')
const { SchemaFlatter } = require('@dataplug/dataplug')
const { JsonStreamReader } = require('@dataplug/dataplug-json')
const { generateSqlSchema } = require('@dataplug/dataplug-sql')

let declaration = {
  command: 'sql-schema',
  description: 'Transforms JSON schema to SQL queries to create corresponding SQL schema'
}
declaration.builder = (yargs) => yargs
  .option('dialect', {
    alias: 'l',
    describe: 'SQL dialect to use',
    enum: [
      'postgres', 'postgresql', 'pg'
    ],
    required: true
  })
  .option('eof', {
    describe: 'Adds extra query delimiter after last query',
    type: 'boolean',
    default: true
  })
  .option('delimiter', {
    description: 'String used as a query delimiter',
    type: 'string',
    default: '\\n'
  })
  .coerce('delimiter', value => JSON.parse(JSON.stringify(value).replace(/\\\\/g, '\\')))
declaration.handler = (argv, collection) => {
  const reader = new JsonStreamReader('!')
  new Promise((resolve, reject) => {
    process.stdin
      .pipe(reader)
      .on('error', (error) => {
        reject('Failed to deserialize data as JSON schema: ' + error ? error : 'no specific information')
      })
      .on('data', (data) => {
        resolve(data)
      })
  })
  .then((jsonSchema) => {
    const entities = new SchemaFlatter().flatten(jsonSchema, collection.name)
    const queries = generateSqlSchema(argv.dialect, entities)
    const delimiter = argv.delimiter
      ? `;${argv.delimiter}`
      : ';'
    let sql = queries.join(delimiter)
    if (argv.eof) {
      sql += delimiter
    }
    process.stdout.write(sql)
  })
  .catch((error) => {
    console.error(chalk.red('!'), error || 'Unknown error')
    process.exit(70)
  })
}

module.exports = declaration
