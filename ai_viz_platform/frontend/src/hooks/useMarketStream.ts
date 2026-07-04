import { useEffect, useState } from 'react'
import { sharedMarketSocket } from '../lib/sharedSocket'
import type {
  ConnectionStatus,
  MarketChartPoint,
  MarketPrediction,
} from '../types/market'

const MAX_PRICE_POINTS = 180
const MAX_LATENCY_SAMPLES = 240

function isMarketPrediction(value: unknown): value is MarketPrediction {
  if (!value || typeof value !== 'object') return false
  const candidate = value as Partial<MarketPrediction>
  return (
    typeof candidate.latest_price === 'number' &&
    Number.isFinite(candidate.latest_price) &&
    (candidate.prediction === 'up' || candidate.prediction === 'down') &&
    typeof candidate.confidence === 'number' &&
    typeof candidate.pipeline_latency_ms === 'number'
  )
}

export function useMarketStream() {
  const [status, setStatus] = useState<ConnectionStatus>('connecting')
  const [latest, setLatest] = useState<MarketPrediction | null>(null)
  const [priceHistory, setPriceHistory] = useState<MarketChartPoint[]>([])
  const [latencySamples, setLatencySamples] = useState<number[]>([])
  const [messageCount, setMessageCount] = useState(0)

  useEffect(() => {
    const unsubscribe = sharedMarketSocket.subscribe(
      (payload) => {
        // Venue reports and metrics share this socket; keep predictions only.
        if (!isMarketPrediction(payload)) return

        const eventTimestamp = Date.parse(payload.event_time)
        const timestamp = Number.isFinite(eventTimestamp) ? eventTimestamp : Date.now()
        const point: MarketChartPoint = {
          id: `${payload.published_at ?? timestamp}-${Math.random().toString(16).slice(2)}`,
          timestamp,
          timeLabel: new Intl.DateTimeFormat(undefined, {
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false,
          }).format(timestamp),
          price: payload.latest_price,
          prediction: payload.prediction,
          confidence: payload.confidence,
          upPrice: payload.prediction === 'up' ? payload.latest_price : null,
          downPrice: payload.prediction === 'down' ? payload.latest_price : null,
          latency: payload.pipeline_latency_ms,
        }

        setLatest(payload)
        setMessageCount((count) => count + 1)
        setPriceHistory((history) => [...history, point].slice(-MAX_PRICE_POINTS))
        setLatencySamples((samples) =>
          [...samples, payload.pipeline_latency_ms].slice(-MAX_LATENCY_SAMPLES),
        )
      },
      setStatus,
    )

    return () => {
      unsubscribe()
      setStatus('disconnected')
    }
  }, [])

  return { status, latest, priceHistory, latencySamples, messageCount }
}
