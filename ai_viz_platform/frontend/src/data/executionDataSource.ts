import { sharedMarketSocket } from '../lib/sharedSocket'
import type { ConnectionStatus } from '../types/market'

export interface ExecutionMetric {
  venue: string
  fill_rate: number
  slippage_bps_p50: number
  slippage_bps_p95: number
  latency_ms_p50: number
  latency_ms_p95: number
  latency_ms_p99: number
  window_orders: number
  comparable: boolean
  timestamp: number
}

export interface ExecutionFeed {
  metrics: ExecutionMetric[]
  connection: ConnectionStatus
  /** Wall-clock ms of the most recently received venue report, null before the first one. */
  lastUpdateMs: number | null
}

export type ExecutionFeedListener = (feed: ExecutionFeed) => void

export interface ExecutionDataSource {
  subscribe(listener: ExecutionFeedListener): () => void
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value)
}

export function parseExecutionFrame(value: unknown): ExecutionMetric | null {
  if (!value || typeof value !== 'object') return null
  const envelope = value as { type?: unknown; data?: unknown }
  if (envelope.type !== 'venue_report' || !envelope.data || typeof envelope.data !== 'object') {
    return null
  }

  const data = envelope.data as Partial<ExecutionMetric>
  if (
    typeof data.venue !== 'string' ||
    data.venue.length === 0 ||
    !isFiniteNumber(data.fill_rate) ||
    data.fill_rate < 0 ||
    data.fill_rate > 1 ||
    !isFiniteNumber(data.slippage_bps_p50) ||
    !isFiniteNumber(data.slippage_bps_p95) ||
    !isFiniteNumber(data.latency_ms_p50) ||
    !isFiniteNumber(data.latency_ms_p95) ||
    !isFiniteNumber(data.latency_ms_p99) ||
    !isFiniteNumber(data.window_orders) ||
    !Number.isInteger(data.window_orders) ||
    data.window_orders < 0 ||
    typeof data.comparable !== 'boolean' ||
    !isFiniteNumber(data.timestamp)
  ) {
    return null
  }

  return {
    venue: data.venue,
    fill_rate: data.fill_rate,
    slippage_bps_p50: data.slippage_bps_p50,
    slippage_bps_p95: data.slippage_bps_p95,
    latency_ms_p50: data.latency_ms_p50,
    latency_ms_p95: data.latency_ms_p95,
    latency_ms_p99: data.latency_ms_p99,
    window_orders: data.window_orders,
    comparable: data.comparable,
    timestamp: data.timestamp,
  }
}

export function mergeExecutionMetric(
  snapshot: ExecutionMetric[],
  incoming: ExecutionMetric,
): ExecutionMetric[] {
  const next = snapshot.filter((metric) => metric.venue !== incoming.venue)
  next.push(incoming)
  return next.sort((a, b) => a.venue.localeCompare(b.venue))
}

class WebSocketExecutionDataSource implements ExecutionDataSource {
  private listeners = new Set<ExecutionFeedListener>()
  private metrics: ExecutionMetric[] = []
  private lastUpdateMs: number | null = null
  private connection: ConnectionStatus = 'disconnected'
  private unsubscribeSocket: (() => void) | null = null

  subscribe(listener: ExecutionFeedListener): () => void {
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

  private feed(): ExecutionFeed {
    return {
      metrics: [...this.metrics],
      connection: this.connection,
      lastUpdateMs: this.lastUpdateMs,
    }
  }

  private ensureConnected(): void {
    if (this.unsubscribeSocket) return
    this.unsubscribeSocket = sharedMarketSocket.subscribe(
      (payload) => {
        // Prediction and metric frames share this socket; keep venue reports only.
        const metric = parseExecutionFrame(payload)
        if (!metric) return
        this.metrics = mergeExecutionMetric(this.metrics, metric)
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

export const executionDataSource: ExecutionDataSource = new WebSocketExecutionDataSource()
