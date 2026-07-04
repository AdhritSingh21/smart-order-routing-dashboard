export type Direction = 'up' | 'down'

export interface MarketPrediction {
  symbol: string
  event_time: string
  published_at: string
  source: string
  market_event: string
  latest_price: number
  prediction: Direction
  confidence: number
  pipeline_latency_ms: number
  features?: {
    rolling_return: number
    volatility: number
    momentum: number
  }
}

export interface MarketChartPoint {
  id: string
  timestamp: number
  timeLabel: string
  price: number
  prediction: Direction
  confidence: number
  upPrice: number | null
  downPrice: number | null
  latency: number
}

export type ConnectionStatus = 'connecting' | 'connected' | 'reconnecting' | 'disconnected'
