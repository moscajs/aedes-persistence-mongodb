'use strict'

const { MongoClient } = require('mongodb')
const regEscape = require('escape-string-regexp')

const dbname = 'aedes-test-regex-limit'
const mongourl = `mongodb://127.0.0.1/${dbname}`

async function setup () {
  const client = new MongoClient(mongourl)
  await client.connect()
  const db = client.db(dbname)
  const retained = db.collection('retained')

  // Clean and setup
  await retained.deleteMany({})

  // Insert some test messages
  const messages = []
  for (let i = 0; i < 100; i++) {
    messages.push({
      topic: `sensor/device${i}/temperature`,
      payload: Buffer.from(`temp-${i}`),
      qos: 0,
      retain: true
    })
  }
  await retained.insertMany(messages)

  return { client, db, retained }
}

async function testWithoutBatching (retained, patterns) {
  console.log('\n🔴 Testing WITHOUT batching (single large regex)...')
  console.log(`   Patterns: ${patterns.length}`)
  console.log(`   Total length: ${patterns.reduce((sum, p) => sum + p.length, 0)} chars`)

  try {
    // Create one massive regex like the old code would
    const regexes = patterns.map(pattern =>
      regEscape(pattern).replace(/(\/*#|\\\+).*$/, '')
    )
    const combinedRegex = regexes.join('|')
    console.log(`   Regex length: ${combinedRegex.length} chars`)

    const topic = new RegExp(combinedRegex)
    const cursor = retained.find({ topic }).project({ _id: 0 })

    const results = []
    for await (const doc of cursor) {
      results.push(doc.topic)
    }

    console.log(`   ✅ SUCCESS: Retrieved ${results.length} results`)
    return { success: true, results: results.length, error: null }
  } catch (error) {
    console.log(`   ❌ ERROR: ${error.message}`)
    return { success: false, results: 0, error: error.message }
  }
}

async function testWithBatching (retained, patterns, maxPatterns, maxLength) {
  console.log('\n🟢 Testing WITH batching...')
  console.log(`   Patterns: ${patterns.length}`)
  console.log(`   Batch limits: ${maxPatterns} patterns / ${maxLength} chars`)

  try {
    // Batching logic
    const batches = []
    let currentBatch = []
    let currentLength = 0

    for (const pattern of patterns) {
      const patternLength = pattern.length
      if (currentBatch.length >= maxPatterns ||
          (currentLength + patternLength > maxLength && currentBatch.length > 0)) {
        batches.push(currentBatch)
        currentBatch = []
        currentLength = 0
      }
      currentBatch.push(pattern)
      currentLength += patternLength
    }
    if (currentBatch.length > 0) {
      batches.push(currentBatch)
    }

    console.log(`   Created ${batches.length} batches`)

    const seenTopics = new Set()
    let maxBatchRegexSize = 0

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i]
      const regexes = batch.map(pattern =>
        regEscape(pattern).replace(/(\/*#|\\\+).*$/, '')
      )
      const batchRegex = regexes.join('|')
      maxBatchRegexSize = Math.max(maxBatchRegexSize, batchRegex.length)

      const topic = new RegExp(batchRegex)
      const cursor = retained.find({ topic }).project({ _id: 0 })

      for await (const doc of cursor) {
        seenTopics.add(doc.topic)
      }
    }

    console.log(`   Max batch regex size: ${maxBatchRegexSize} chars`)
    console.log(`   ✅ SUCCESS: Retrieved ${seenTopics.size} results`)
    return { success: true, results: seenTopics.size, error: null }
  } catch (error) {
    console.log(`   ❌ ERROR: ${error.message}`)
    return { success: false, results: 0, error: error.message }
  }
}

function generateLargePatternSet (count, avgLength) {
  const patterns = []
  for (let i = 0; i < count; i++) {
    const padding = 'x'.repeat(Math.max(0, avgLength - 30))
    patterns.push(`sensor/${padding}device${i}/#`)
  }
  return patterns
}

async function runTests () {
  console.log('================================================================================')
  console.log('REGEX SIZE LIMIT TEST - Reproducing "regular expression is too large" error')
  console.log('================================================================================')

  const { client, retained } = await setup()

  // Test scenarios designed to trigger the error
  const scenarios = [
    { count: 100, avgLength: 400, name: 'Trigger error: 100 × 400 chars = 40KB' },
    { count: 200, avgLength: 200, name: 'Trigger error: 200 × 200 chars = 40KB' },
    { count: 500, avgLength: 100, name: 'Many patterns: 500 × 100 chars = 50KB' }
  ]

  for (const scenario of scenarios) {
    console.log('\n' + '='.repeat(80))
    console.log(`\n📋 Scenario: ${scenario.name}`)
    console.log('─'.repeat(80))

    const patterns = generateLargePatternSet(scenario.count, scenario.avgLength)

    // Test without batching
    const withoutResult = await testWithoutBatching(retained, patterns)

    // Test with conservative batching (to ensure it works)
    const withResult1 = await testWithBatching(retained, patterns, 50, 5000)

    // Test with optimized batching
    const withResult2 = await testWithBatching(retained, patterns, 200, 15000)

    // Summary
    console.log('\n   📊 Summary:')
    if (!withoutResult.success) {
      console.log('   ❌ Without batching: ERROR (regex too large)')
      if (withResult1.success) {
        console.log('   ✅ With batching (50/5000): SUCCESS')
      }
      if (withResult2.success) {
        console.log('   ✅ With batching (200/15000): SUCCESS')
      }
      if (withResult1.success || withResult2.success) {
        console.log('\n   ✓ Batching SOLVES the problem!')
      }
    } else if (withoutResult.success && withResult1.success) {
      console.log('   ℹ️  All approaches work for this size')
      console.log('   ✓ Without batching: OK (but risky)')
      console.log('   ✓ With batching: OK (safer)')
    }
  }

  console.log('\n' + '='.repeat(80))
  console.log('CONCLUSION')
  console.log('='.repeat(80))
  console.log('\nThe batching approach ensures patterns are split into manageable chunks,')
  console.log('preventing "regular expression is too large" errors while maintaining')
  console.log('correct query results through deduplication.')
  console.log('\n✅ Test complete!\n')

  await client.close()
}

runTests().catch(console.error)
