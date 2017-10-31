#!/usr/bin/env node

const path = require('path')

require('@dataplug/dataplug-cli').build()
  .usingCollectionFactory(require('../lib/factory'))
  .usingCommandsFromDir(path.join(__dirname, '../lib/commands'))
  .process()
