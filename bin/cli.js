#!/usr/bin/env node

require('@dataplug/dataplug-cli').build()
  .usingCollectionFactory(require('../lib/factory'))
  .process()
