import { describe, expect, it } from 'vitest'
import {
  mergeExecutionMetric,
  parseExecutionFrame,
  type ExecutionFeed,
  type ExecutionMetric,
} from './executionDataSource'
import { deriveFeedState, STALE_AFTER_MS } from './executionFeedState'

const venueReport: ExecutionMetric = {
  venue: 'binance_testnet',
  fill_rate: 0.97,
  slippage_bps_p50: 0.8,
  slippage_bps_p95: 2.6,
  latency_ms_p50: 18,
  latency_ms_p95: 45,
  latency_ms_p99: 71,
  window_orders: 84,
  comparable: true,
  timestamp: 1234.5,
}

describe('executionDataSource', () => {
  it('parses venue_report envelopes from the shared websocket', () => {
    expect(parseExecutionFrame({ type: 'venue_report', data: venueReport })).toEqual(venueReport)
  })

  it('ignores prediction, metric, and malformed frames', () => {
    expect(parseExecutionFrame({ latest_price: 65_000, prediction: 'up' })).toBeNull()
    expect(parseExecutionFrame({ type: 'metric', data: { order_id: 'x' } })).toBeNull()
    expect(parseExecutionFrame({
      type: 'venue_report',
      data: { ...venueReport, fill_rate: 1.5 },
    })).toBeNull()
    expect(parseExecutionFrame('venue_report')).toBeNull()
  })

  it('upserts updates by venue instead of duplicating ranking rows', () => {
    const updated = { ...venueReport, fill_rate: 0.99, timestamp: 1235 }
    expect(mergeExecutionMetric([venueReport], updated)).toEqual([updated])
  })

  it('keeps independent venues side by side', () => {
    const other = { ...venueReport, venue: 'alpaca' }
    expect(mergeExecutionMetric([venueReport], other)).toEqual([other, venueReport])
  })
})

describe('deriveFeedState', () => {
  const now = 1_000_000
  const feed = (overrides: Partial<ExecutionFeed>): ExecutionFeed => ({
    metrics: [venueReport],
    connection: 'connected',
    lastUpdateMs: now,
    ...overrides,
  })

  it('reports socket connectivity before data freshness', () => {
    expect(deriveFeedState(feed({ connection: 'connecting' }), now)).toBe('connecting')
    expect(deriveFeedState(feed({ connection: 'reconnecting' }), now)).toBe('reconnecting')
    expect(deriveFeedState(feed({ connection: 'disconnected' }), now)).toBe('disconnected')
  })

  it('awaits ROS 2 while connected but empty', () => {
    expect(deriveFeedState(feed({ metrics: [], lastUpdateMs: null }), now)).toBe('awaiting')
  })

  it('is live within the staleness window and stale after it', () => {
    expect(deriveFeedState(feed({}), now + STALE_AFTER_MS)).toBe('live')
    expect(deriveFeedState(feed({}), now + STALE_AFTER_MS + 1)).toBe('stale')
  })
})
