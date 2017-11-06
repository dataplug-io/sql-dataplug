const dataplug = require('@dataplug/dataplug')
const { SqlDataWriter } = require('@dataplug/dataplug-sql')

const targetConfigDeclaration = dataplug.config.declare()
  .parameters({
    connectionString: {
      description: 'An SQL database connection string',
      type: 'string',
      required: true
    },
    reinsert: {
      description: 'Delete entry by identity before inserting',
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
  .asIs('connectionString')
  .asIs('reinsert')
  .asIs('duplicate')

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
    let updateBehavior
    if (options.duplicate === 'update') {
      updateBehavior = true
    } else if (options.duplicate === 'ignore') {
      updateBehavior = false
    }
    return new SqlDataWriter(options.connectionString, name, options.reinsert, updateBehavior) // TODO: support metadata
  })

  return collection
}

module.exports = {
  createCollection,
  genericCollection
}
