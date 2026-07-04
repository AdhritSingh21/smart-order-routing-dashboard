import { describe, expect, it } from 'vitest'
import { parseRoutingFrame, type RoutingFrame } from './routingDataSource'

const okVenue = {
  venue: 'alpaca',
  status: 'ok' as const,
  recommended: true,
  orders_observed: 42,
  predicted_slippage_bps: 2.31,
  predicted_fill_probability: 0.94,
  predicted_latency_ms_p95: 210.5,
  routing_score: 89.4,
}

const warmingVenue = {
  venue: 'binance_testnet',
  status: 'warming_up' as const,
  recommended: false,
  orders_observed: 4,
}

const validation = {
  window: 100,
  orders_scored: 37,
  slippage_mae: 2.4,
  slippage_baseline_mae: 2.9,
  fill_accuracy: 0.86,
  fill_brier: 0.12,
  latency_p95_coverage: 0.91,
  recommendation_hit_rate: null,
  recommendation_windows: 0,
}

function frame(overrides: Record<string, unknown> = {}): unknown {
  return {
    type: 'execution_prediction',
    data: {
      generated_at: 1_700_000_123.4,
      model_status: 'demo',
      recommended_venue: 'alpaca',
      venues: [okVenue, warmingVenue],
      validation,
      ...overrides,
    },
  }
}

describe('parseRoutingFrame', () => {
  it('parses execution_prediction envelopes from the shared websocket', () => {
    const parsed = parseRoutingFrame(frame()) as RoutingFrame
    expect(parsed.model_status).toBe('demo')
    expect(parsed.recommended_venue).toBe('alpaca')
    expect(parsed.venues).toHaveLength(2)
    expect(parsed.venues[0]).toEqual(okVenue)
    expect(parsed.validation.slippage_mae).toBe(2.4)
    expect(parsed.validation.recommendation_hit_rate).toBeNull()
  })

  it('normalizes warming-up venues to null predictions', () => {
    const parsed = parseRoutingFrame(frame()) as RoutingFrame
    expect(parsed.venues[1]).toEqual({
      ...warmingVenue,
      predicted_slippage_bps: null,
      predicted_fill_probability: null,
      predicted_latency_ms_p95: null,
      routing_score: null,
    })
  })

  it('accepts a null fill probability when no classifier is served', () => {
    const parsed = parseRoutingFrame(
      frame({ venues: [{ ...okVenue, predicted_fill_probability: null }] }),
    ) as RoutingFrame
    expect(parsed.venues[0].predicted_fill_probability).toBeNull()
  })

  it('ignores venue_report, metric, and market prediction frames', () => {
    expect(parseRoutingFrame({ type: 'venue_report', data: { venue: 'alpaca' } })).toBeNull()
    expect(parseRoutingFrame({ type: 'metric', data: { order_id: 'x' } })).toBeNull()
    expect(parseRoutingFrame({ latest_price: 65_000, prediction: 'up' })).toBeNull()
    expect(parseRoutingFrame('execution_prediction')).toBeNull()
  })

  it('rejects frames with malformed venues or validation blocks', () => {
    expect(
      parseRoutingFrame(frame({ venues: [{ ...okVenue, routing_score: 'high' }] })),
    ).toBeNull()
    expect(parseRoutingFrame(frame({ model_status: 'production' }))).toBeNull()
    expect(parseRoutingFrame(frame({ validation: { window: 100 } }))).toBeNull()
  })
})
