/* eslint-env node, mocha */
require('chai')
  .should()
const dataplugTestsuite = require('@dataplug/dataplug-testsuite')
const sqlDataplug = require('../lib')

describe('sql-dataplug', () => {
  dataplugTestsuite
    .forCollectionFactory('sql', sqlDataplug.factory)
    .use()
})
