const _ = require('lodash')
const { URL } = require('url')
const dataplug = require('@dataplug/dataplug')
const { SqlSerializerStream, SqlStreamWriter } = require('@dataplug/dataplug-sql')

const targetConfigDeclaration = dataplug.config.declare()
  .parameters({
    connectionString: {
      description: 'An SQL database connection string',
      type: 'string',
      required: true
    },
    truncate: {
      description: 'Truncate entity table prior to first insert',
      type: 'boolean',
      default: false
    },
    missing: {
      description: 'Column name to use as "missing" marker',
      type: 'string'
    },
    delete: {
      description: 'Delete entries by identity prior to inserting',
      type: 'boolean',
      default: false
    },
    onConflict: {
      description: 'Behavior on conflict',
      enum: [
        'update',
        'skip',
        'fail'
      ],
      default: 'fail'
    },
    singleTransaction: {
      description: 'If true, use single transaction for entire batch',
      type: 'boolean',
      default: false
    }
  })
const targetConfigToOptionsMapping = dataplug.config.map()
  .asIs('connectionString')
  .asIs('truncate')
  .asIs('missing')
  .asIs('delete')
  .asIs('onConflict')
  .asIs('singleTransaction')

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

    const nonMissingMixin = !options.missing ? undefined : _.set({}, options.missing, false)
    const missingMixin = !options.missing ? undefined : _.set({}, options.missing, true)
    const transform = !options.missing ? undefined : new dataplug.FlattenedTransformStream((entry) => {
      return _.assign({}, entry, nonMissingMixin)
    })

    const dbDialect = new URL(options.connectionString).protocol.replace(/:$/, '')
    const serializer = new SqlSerializerStream(dbDialect, name, undefined, {
      prologue: options.truncate
        ? 'truncate'
        : (options.missing ? missingMixin : undefined),
      preprocessor: options.delete ? 'delete-by-identity' : undefined,
      postprocessor: (options.onConflict !== 'fail')
        ? `${options.onConflict}-on-conflict`
        : undefined
    })

    const writer = new SqlStreamWriter(options.connectionString, options.singleTransaction)

    if (transform) {
      transform
        .pipe(serializer)
    }
    serializer
      .pipe(writer)

    return transform
      ? [transform, serializer, writer]
      : [serializer, writer]
  })

  return collection
}

module.exports = {
  createCollection,
  genericCollection
}
