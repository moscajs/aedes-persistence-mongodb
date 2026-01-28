'use strict'

const { MongoClient } = require('mongodb')
const { performance } = require('perf_hooks')

const dbname = 'aedes-benchmark'
const mongourl = `mongodb://127.0.0.1/${dbname}`

// Test configurations - testing progressively higher limits
const BATCH_CONFIGS = [
  { maxPatterns: 50, maxLength: 5000, name: 'Current (50/5000)' },
  { maxPatterns: 100, maxLength: 10000, name: 'Medium (100/10000)' },
  { maxPatterns: 200, maxLength: 15000, name: 'Large (200/15000)' },
  { maxPatterns: 300, maxLength: 20000, name: 'XLarge (300/20000)' },
  { maxPatterns: 500, maxLength: 25000, name: 'XXLarge (500/25000)' },
  { maxPatterns: 1000, maxLength: 30000, name: 'Extreme (1000/30000)' },
  { maxPatterns: 10000, maxLength: 100000, name: 'No limit (10000/100000)' }
]

const PATTERN_SCENARIOS = [
  { count: 60, avgLength: 20, name: '60 short patterns' },
  { count: 100, avgLength: 50, name: '100 medium patterns' },
  { count: 200, avgLength: 30, name: '200 short-medium patterns' },
  { count: 300, avgLength: 50, name: '300 medium patterns' },
  { count: 500, avgLength: 40, name: '500 medium patterns' },
  { count: 100, avgLength: 150, name: '100 long patterns' }
]

async function setup () {
  const client = new MongoClient(mongourl)
  await client.connect()
  const db = client.db(dbname)
  const retained = db.collection('retained')

  // Clean and setup
  await retained.deleteMany({})

  // Insert 1000 retained messages with various topics
  const messages = []
  for (let i = 0; i < 1000; i++) {
    messages.push({
      topic: `sensor/device${i % 100}/metric${i % 10}/value`,
      payload: Buffer.from(`value-${i}`),
      qos: 0,
      retain: true
    })
  }
  await retained.insertMany(messages)

  return { client, db, retained }
}

function generatePatterns (count, avgLength) {
  const patterns = []
  for (let i = 0; i < count; i++) {
    const padding = 'x'.repeat(Math.max(0, avgLength - 30))
    patterns.push(`sensor/device${i % 100}/${padding}#`)
  }
  return patterns
}

function createBatches (patterns, maxPatterns, maxLength) {
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
  return batches
}

async function benchmarkQuery (retained, patterns, config) {
  const batches = createBatches(patterns, config.maxPatterns, config.maxLength)
  const seenTopics = new Set()

  const start = performance.now()

  try {
    for (const batch of batches) {
      const regexes = batch.map(p => p.replace(/(\/*#|\\\+).*$/, ''))
      const topic = new RegExp(regexes.join('|'))

      const cursor = retained.find({ topic }).project({ _id: 0 })
      for await (const doc of cursor) {
        seenTopics.add(doc.topic)
      }
    }

    const duration = performance.now() - start

    return {
      duration,
      batchCount: batches.length,
      resultsCount: seenTopics.size,
      avgBatchSize: patterns.length / batches.length,
      error: null
    }
  } catch (error) {
    const duration = performance.now() - start
    return {
      duration,
      batchCount: batches.length,
      resultsCount: 0,
      avgBatchSize: patterns.length / batches.length,
      error: error.message
    }
  }
}

async function runBenchmark () {
  console.log('Setting up MongoDB...')
  const { client, retained } = await setup()

  console.log('\n' + '='.repeat(80))
  console.log('BATCHING PERFORMANCE BENCHMARK')
  console.log('='.repeat(80) + '\n')

  const results = []

  for (const scenario of PATTERN_SCENARIOS) {
    console.log(`\n📊 Scenario: ${scenario.name}`)
    console.log(`   Patterns: ${scenario.count}, Avg length: ${scenario.avgLength} chars`)
    console.log('-'.repeat(80))

    const patterns = generatePatterns(scenario.count, scenario.avgLength)
    const totalLength = patterns.reduce((sum, p) => sum + p.length, 0)
    console.log(`   Total pattern length: ${totalLength} chars\n`)

    for (const config of BATCH_CONFIGS) {
      // Run 3 times and take average
      const runs = []
      let hasError = false
      for (let i = 0; i < 3; i++) {
        const result = await benchmarkQuery(retained, patterns, config)
        runs.push(result)
        if (result.error) {
          hasError = true
          break
        }
      }

      if (hasError) {
        console.log(`   ${config.name.padEnd(30)} ❌ ERROR: ${runs[0].error.substring(0, 50)}...`)
        continue
      }

      const avgDuration = runs.reduce((sum, r) => sum + r.duration, 0) / runs.length
      const batchCount = runs[0].batchCount

      console.log(`   ${config.name.padEnd(30)} ${avgDuration.toFixed(2).padStart(8)}ms  (${batchCount} batches, ${runs[0].resultsCount} results)`)

      results.push({
        scenario: scenario.name,
        config: config.name,
        duration: avgDuration,
        batches: batchCount
      })
    }
  }

  // Summary
  console.log('\n' + '='.repeat(80))
  console.log('SUMMARY')
  console.log('='.repeat(80))

  const grouped = {}
  for (const result of results) {
    if (!grouped[result.config]) grouped[result.config] = []
    grouped[result.config].push(result.duration)
  }

  console.log('\nAverage duration across all scenarios:')
  for (const [config, durations] of Object.entries(grouped)) {
    const avg = durations.reduce((a, b) => a + b, 0) / durations.length
    const improvement = grouped['Current (50/5000)']
      ? ((grouped['Current (50/5000)'][0] - avg) / grouped['Current (50/5000)'][0] * 100).toFixed(1)
      : 0
    console.log(`  ${config.padEnd(30)} ${avg.toFixed(2).padStart(8)}ms  (${improvement > 0 ? '+' : ''}${improvement}% vs current)`)
  }

  // Find optimal
  let bestConfig = null
  let bestAvg = Infinity
  for (const [config, durations] of Object.entries(grouped)) {
    const avg = durations.reduce((a, b) => a + b, 0) / durations.length
    if (avg < bestAvg) {
      bestAvg = avg
      bestConfig = config
    }
  }

  console.log(`\n🏆 Best performing configuration: ${bestConfig} (${bestAvg.toFixed(2)}ms avg)`)

  console.log('\n✅ Benchmark complete!')

  await client.close()
}

runBenchmark().catch(console.error)
