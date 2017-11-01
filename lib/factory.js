const dataplug = require('@dataplug/dataplug')
const { SqlDataWriter } = require('@dataplug/dataplug-sql')

const targetConfigDeclaration = dataplug.config.declare()
  .parameters({
    connectionString: {
      description: 'An SQL database connection string',
      type: 'string',
      required: true
    },
    flat: {
      description: 'Expect containered data on input',
      type: 'boolean',
      default: false
    },
    duplicate: {
      description: 'Action to perform when duplicate entry found',
      enum: ['update', 'ignore', 'fail'],
      default: 'fail'
    }
  })
const targetConfigToOptionsMapping = dataplug.config.map()

const genericCollection = {
  origin: 'sql',
  name: null,
  target: dataplug.target(targetConfigDeclaration, () => {
    throw new Error('Generic collection does not provide a target implementation.')
  })
}

/**
 * Creates collection with specified name
 */
function createCollection (name) {
  let collection = Object.assign({}, genericCollection)

  collection.name = name
  collection.target = dataplug.target(targetConfigDeclaration, (params) => {
    const options = targetConfigToOptionsMapping.apply(params)
    let updateExisting
    if (options.duplicate === 'update') {
      updateExisting = true
    } else if (options.duplicate === 'ignore') {
      updateExisting = false
    }
    return new SqlDataWriter(options.connectionString, name, options.flat, updateExisting)
  })

  return collection
}

module.exports = {
  createCollection,
  genericCollection
}
