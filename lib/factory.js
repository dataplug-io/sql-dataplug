const _ = require('lodash')
const moment = require('moment')
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
    timestamp: {
      description: 'Column name to use as "timestamp" marker',
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
  .asIs('timestamp')
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

    let prologueModifier = null
    if (options.missing) {
      prologueModifier = _.assign({}, prologueModifier, _.set({}, options.missing, true))
    }

    let entryModifier = null
    if (options.missing) {
      entryModifier = _.assign({}, entryModifier, _.set({}, options.missing, false))
    }
    if (options.timestamp) {
      entryModifier = _.assign({}, entryModifier, _.set({}, options.timestamp, moment.utc().format()))
    }
    const transform = !entryModifier ? undefined : new dataplug.FlattenedTransformStream((entry) => {
      return _.assign({}, entry, entryModifier)
    })

    const dbDialect = new URL(options.connectionString).protocol.replace(/:$/, '')
    const serializer = new SqlSerializerStream(dbDialect, name, undefined, {
      prologue: options.truncate
        ? 'truncate'
        : (prologueModifier !== null ? prologueModifier : undefined),
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
