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
      'maria',
      'mssql',
      'mysql',
      'mysql2',
      'oracle',
      'oracledb',
      'postgres',
      'sqlite3',
      'strong-oracle',
      'websql'
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
    default: ';\\n'
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
    let sql = queries.join(argv.delimiter)
    if (argv.eof) {
      sql += argv.delimiter
    }
    process.stdout.write(sql)
    process.exit()
  })
  .catch((error) => {
    console.error(chalk.red('!'), error || 'Unknown error')
    process.exit(70)
  })
}

module.exports = declaration