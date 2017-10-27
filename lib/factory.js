const dataplug = require('@dataplug/dataplug')

const targetConfigDeclaration = dataplug.config.declare()
  .parameters({
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
    // return new CsvStreamWriter(name, params.targetDir, options)
  })

  return collection
}

module.exports = {
  createCollection,
  genericCollection
}
