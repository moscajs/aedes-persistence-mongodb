'use strict'

const test = require('node:test')
const { MongoClient } = require('mongodb')
const dbname = 'aedes-test'
const mongourl = `mongodb://127.0.0.1/${dbname}`

// Testing starts here.
// this test is only there to ensure that mongdb has been started correctly
// This test needs to run first to avoid the rest of the tests hanging if MongDB did not start correctly
async function doTest () {
  const mongoClient = new MongoClient(mongourl, { w: 1 })
  const db = mongoClient.db(dbname)
  test('MongoDB connection', async (t) => {
    await db.admin().ping()
  })

  test.after(() => {
    mongoClient.close()
  })
}
doTest()
