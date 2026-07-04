import type { ConnectionStatus } from '../types/market'

// The aggregator reports every ~5s; three missed windows means the analyzer
// is down or the bridge lost its connection.
export const STALE_AFTER_MS = 15_000

export type FeedState =
  | 'connecting'
  | 'reconnecting'
  | 'disconnected'
  | 'awaiting'
  | 'stale'
  | 'live'

/** Minimal shape shared by the execution and routing feeds. */
export interface FeedSnapshot {
  connection: ConnectionStatus
  lastUpdateMs: number | null
  metrics: readonly unknown[]
}

export function deriveFeedState(feed: FeedSnapshot, nowMs: number): FeedState {
  if (feed.connection === 'connecting') return 'connecting'
  if (feed.connection === 'reconnecting') return 'reconnecting'
  if (feed.connection === 'disconnected') return 'disconnected'
  if (feed.metrics.length === 0 || feed.lastUpdateMs === null) return 'awaiting'
  if (nowMs - feed.lastUpdateMs > STALE_AFTER_MS) return 'stale'
  return 'live'
}

export const FEED_LABELS: Record<FeedState, string> = {
  connecting: 'Connecting…',
  reconnecting: 'Reconnecting…',
  disconnected: 'Disconnected',
  awaiting: 'Awaiting ROS 2',
  stale: 'Stale — ROS 2 silent',
  live: 'Live ROS 2',
}
