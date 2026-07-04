import { describe, expect, it } from 'vitest'
import { buildLatencyHistogram } from './latency'

describe('buildLatencyHistogram', () => {
  it('places latency observations in stable dashboard buckets', () => {
    const bins = buildLatencyHistogram([1, 3, 7, 20, 40, 75, 130, -1, Number.NaN])
    expect(bins.map((bin) => bin.count)).toEqual([1, 1, 1, 1, 1, 1, 1])
  })
})
