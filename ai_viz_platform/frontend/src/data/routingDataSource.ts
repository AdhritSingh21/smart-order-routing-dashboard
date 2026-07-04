import { sharedMarketSocket } from '../lib/sharedSocket'
import type { ConnectionStatus } from '../types/market'

/** One venue's next-order prediction from the routing model. */
export interface RoutingVenuePrediction {
  venue: string
  status: 'ok' | 'warming_up'
  recommended: boolean
  orders_observed: number
  predicted_slippage_bps: number | null
  predicted_fill_probability: number | null
  predicted_latency_ms_p95: number | null
  routing_score: number | null
}

/** Rolling live-validation stats over the last `window` completed orders. */
export interface RoutingValidation {
  window: number
  orders_scored: number
  slippage_mae: number | null
  slippage_baseline_mae: number | null
  fill_accuracy: number | null
  fill_brier: number | null
  latency_p95_coverage: number | null
  recommendation_hit_rate: number | null
  recommendation_windows: number
}

export interface RoutingFrame {
  generated_at: number
  model_status: 'real' | 'demo'
  recommended_venue: string | null
  venues: RoutingVenuePrediction[]
  validation: RoutingValidation
}

export interface RoutingFeed {
  frame: RoutingFrame | null
  connection: ConnectionStatus
  /** Wall-clock ms of the most recently received prediction frame. */
  lastUpdateMs: number | null
}

export type RoutingFeedListener = (feed: RoutingFeed) => void

export interface RoutingDataSource {
  subscribe(listener: RoutingFeedListener): () => void
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value)
}

function isNullableFinite(value: unknown): value is number | null {
  return value === null || isFiniteNumber(value)
}

function parseVenue(value: unknown): RoutingVenuePrediction | null {
  if (!value || typeof value !== 'object') return null
  const row = value as Partial<RoutingVenuePrediction>
  if (
    typeof row.venue !== 'string' ||
    row.venue.length === 0 ||
    typeof row.recommended !== 'boolean' ||
    !isFiniteNumber(row.orders_observed)
  ) {
    return null
  }

  if (row.status === 'warming_up') {
    return {
      venue: row.venue,
      status: 'warming_up',
      recommended: row.recommended,
      orders_observed: row.orders_observed,
      predicted_slippage_bps: null,
      predicted_fill_probability: null,
      predicted_latency_ms_p95: null,
      routing_score: null,
    }
  }

  if (
    row.status !== 'ok' ||
    !isFiniteNumber(row.predicted_slippage_bps) ||
    !isNullableFinite(row.predicted_fill_probability) ||
    !isFiniteNumber(row.predicted_latency_ms_p95) ||
    !isFiniteNumber(row.routing_score)
  ) {
    return null
  }
  return {
    venue: row.venue,
    status: 'ok',
    recommended: row.recommended,
    orders_observed: row.orders_observed,
    predicted_slippage_bps: row.predicted_slippage_bps,
    predicted_fill_probability: row.predicted_fill_probability ?? null,
    predicted_latency_ms_p95: row.predicted_latency_ms_p95,
    routing_score: row.routing_score,
  }
}

function parseValidation(value: unknown): RoutingValidation | null {
  if (!value || typeof value !== 'object') return null
  const v = value as Partial<RoutingValidation>
  if (!isFiniteNumber(v.window) || !isFiniteNumber(v.orders_scored)) return null
  if (
    !isNullableFinite(v.slippage_mae) ||
    !isNullableFinite(v.slippage_baseline_mae) ||
    !isNullableFinite(v.fill_accuracy) ||
    !isNullableFinite(v.fill_brier) ||
    !isNullableFinite(v.latency_p95_coverage) ||
    !isNullableFinite(v.recommendation_hit_rate) ||
    !isFiniteNumber(v.recommendation_windows)
  ) {
    return null
  }
  return {
    window: v.window,
    orders_scored: v.orders_scored,
    slippage_mae: v.slippage_mae ?? null,
    slippage_baseline_mae: v.slippage_baseline_mae ?? null,
    fill_accuracy: v.fill_accuracy ?? null,
    fill_brier: v.fill_brier ?? null,
    latency_p95_coverage: v.latency_p95_coverage ?? null,
    recommendation_hit_rate: v.recommendation_hit_rate ?? null,
    recommendation_windows: v.recommendation_windows,
  }
}

export function parseRoutingFrame(value: unknown): RoutingFrame | null {
  if (!value || typeof value !== 'object') return null
  const envelope = value as { type?: unknown; data?: unknown }
  if (
    envelope.type !== 'execution_prediction' ||
    !envelope.data ||
    typeof envelope.data !== 'object'
  ) {
    return null
  }

  const data = envelope.data as Partial<RoutingFrame> & { venues?: unknown }
  if (
    !isFiniteNumber(data.generated_at) ||
    (data.model_status !== 'real' && data.model_status !== 'demo') ||
    (data.recommended_venue !== null && typeof data.recommended_venue !== 'string') ||
    !Array.isArray(data.venues)
  ) {
    return null
  }

  const venues: RoutingVenuePrediction[] = []
  for (const raw of data.venues) {
    const venue = parseVenue(raw)
    if (!venue) return null
    venues.push(venue)
  }

  const validation = parseValidation(data.validation)
  if (!validation) return null

  return {
    generated_at: data.generated_at,
    model_status: data.model_status,
    recommended_venue: data.recommended_venue ?? null,
    venues,
    validation,
  }
}

class WebSocketRoutingDataSource implements RoutingDataSource {
  private listeners = new Set<RoutingFeedListener>()
  private frame: RoutingFrame | null = null
  private lastUpdateMs: number | null = null
  private connection: ConnectionStatus = 'disconnected'
  private unsubscribeSocket: (() => void) | null = null

  subscribe(listener: RoutingFeedListener): () => void {
    this.listeners.add(listener)
    this.ensureConnected()
    queueMicrotask(() => {
      if (this.listeners.has(listener)) listener(this.feed())
    })

    return () => {
      this.listeners.delete(listener)
      if (this.listeners.size === 0) {
        this.unsubscribeSocket?.()
        this.unsubscribeSocket = null
      }
    }
  }

  private feed(): RoutingFeed {
    return {
      frame: this.frame,
      connection: this.connection,
      lastUpdateMs: this.lastUpdateMs,
    }
  }

  private ensureConnected(): void {
    if (this.unsubscribeSocket) return
    this.unsubscribeSocket = sharedMarketSocket.subscribe(
      (payload) => {
        // All frame types share this socket; keep execution predictions only.
        const frame = parseRoutingFrame(payload)
        if (!frame) return
        this.frame = frame
        this.lastUpdateMs = Date.now()
        this.emit()
      },
      (status) => {
        this.connection = status
        this.emit()
      },
    )
  }

  private emit(): void {
    const feed = this.feed()
    this.listeners.forEach((listener) => listener(feed))
  }
}

export const routingDataSource: RoutingDataSource = new WebSocketRoutingDataSource()
