import WebSocket from 'ws'

const url = process.env.DASHBOARD_WS_URL ?? 'ws://127.0.0.1:5173/ws'
const ingestUrl = process.env.DASHBOARD_INGEST_URL ?? 'http://127.0.0.1:8000/ingest'
const timeoutMs = Number(process.env.WS_TEST_TIMEOUT_MS ?? 15000)

const requiredNumberFields = ['latest_price', 'confidence', 'pipeline_latency_ms']
const testVenue = `smoke_${Date.now()}`
let prediction = null
let venueReport = null
let finished = false

const socket = new WebSocket(url)
const timeout = setTimeout(() => {
  console.error(`Timed out waiting for prediction and venue report from ${url}`)
  socket.terminate()
  process.exitCode = 1
}, timeoutMs)

function finishIfComplete() {
  if (finished || !prediction || !venueReport) return
  finished = true
  clearTimeout(timeout)
  console.log(JSON.stringify({
    proxied_websocket: url,
    latest_price: prediction.latest_price,
    prediction: prediction.prediction,
    confidence: prediction.confidence,
    pipeline_latency_ms: prediction.pipeline_latency_ms,
    execution_venue: venueReport.data.venue,
    fill_rate: venueReport.data.fill_rate,
    window_orders: venueReport.data.window_orders,
  }, null, 2))
  socket.close()
}

socket.on('open', async () => {
  try {
    const response = await fetch(ingestUrl, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        type: 'venue_report',
        data: {
          venue: testVenue,
          window_orders: 25,
          fill_rate: 0.96,
          slippage_bps_p50: 0.8,
          slippage_bps_p95: 2.7,
          latency_ms_p50: 18,
          latency_ms_p95: 46,
          latency_ms_p99: 74,
          comparable: true,
          timestamp: Date.now() / 1000,
        },
      }),
    })
    if (!response.ok) throw new Error(`Ingest returned HTTP ${response.status}`)
  } catch (error) {
    clearTimeout(timeout)
    console.error(error)
    socket.terminate()
    process.exitCode = 1
  }
})

socket.on('message', (raw) => {
  try {
    const payload = JSON.parse(raw.toString())
    if (payload.type === 'venue_report') {
      if (payload.data?.venue === testVenue) venueReport = payload
      finishIfComplete()
      return
    }

    for (const field of requiredNumberFields) {
      if (typeof payload[field] !== 'number' || !Number.isFinite(payload[field])) {
        return // Shared socket can carry future message types too.
      }
    }
    if (!['up', 'down'].includes(payload.prediction)) {
      throw new Error('Invalid prediction')
    }
    prediction = payload
    finishIfComplete()
  } catch (error) {
    clearTimeout(timeout)
    console.error(error)
    socket.terminate()
    process.exitCode = 1
  }
})

socket.on('error', (error) => {
  clearTimeout(timeout)
  console.error(error)
  process.exitCode = 1
})
