import {
  Bar,
  BarChart,
  CartesianGrid,
  ComposedChart,
  Line,
  RadialBar,
  RadialBarChart,
  ResponsiveContainer,
  Scatter,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { buildLatencyHistogram } from '../lib/latency'
import type {
  ConnectionStatus,
  Direction,
  MarketChartPoint,
  MarketPrediction,
} from '../types/market'
import { PanelHeader } from './PanelHeader'

interface PredictionPanelProps {
  status: ConnectionStatus
  latest: MarketPrediction | null
  priceHistory: MarketChartPoint[]
  latencySamples: number[]
  messageCount: number
}

interface SignalMarkerProps {
  cx?: number
  cy?: number
  payload?: MarketChartPoint
}

function SignalMarker({ cx = 0, cy = 0, payload }: SignalMarkerProps) {
  if (!payload) return null
  const isUp = payload.prediction === 'up'
  const points = isUp
    ? `${cx},${cy - 7} ${cx - 5},${cy + 4} ${cx + 5},${cy + 4}`
    : `${cx},${cy + 7} ${cx - 5},${cy - 4} ${cx + 5},${cy - 4}`
  return (
    <polygon
      points={points}
      fill={isUp ? '#35e0a1' : '#ff6b7a'}
      stroke="#081019"
      strokeWidth={1.5}
      opacity={Math.max(0.45, payload.confidence)}
    />
  )
}

function formatPrice(value: number | undefined): string {
  if (value === undefined || !Number.isFinite(value)) return '—'
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 2,
  }).format(value)
}

function formatDirection(direction: Direction | undefined): string {
  return direction ? direction.toUpperCase() : 'WAITING'
}

function PriceTooltip({ active, payload }: { active?: boolean; payload?: Array<{ payload: MarketChartPoint }> }) {
  if (!active || !payload?.length) return null
  const point = payload[0].payload
  return (
    <div className="chart-tooltip">
      <strong>{formatPrice(point.price)}</strong>
      <span>{point.timeLabel}</span>
      <span className={point.prediction === 'up' ? 'positive' : 'negative'}>
        {point.prediction.toUpperCase()} · {(point.confidence * 100).toFixed(1)}%
      </span>
    </div>
  )
}

export function PredictionPanel({
  status,
  latest,
  priceHistory,
  latencySamples,
  messageCount,
}: PredictionPanelProps) {
  const histogram = buildLatencyHistogram(latencySamples)
  const confidence = Math.min(100, Math.max(0, (latest?.confidence ?? 0) * 100))
  const direction = latest?.prediction
  const confidenceData = [{ name: 'confidence', value: confidence, fill: direction === 'down' ? '#ff6b7a' : '#35e0a1' }]
  const upSignals = priceHistory.filter((point) => point.prediction === 'up')
  const downSignals = priceHistory.filter((point) => point.prediction === 'down')

  return (
    <section className="panel prediction-panel">
      <PanelHeader
        eyebrow="Reference feed"
        title="Market context — BTC/USD"
        aside={
          <div className={`connection-pill ${status}`}>
            <span className="connection-dot" />
            {status}
          </div>
        }
      />

      <div className="prediction-kpis">
        <div className="hero-price">
          <span>BTC / USD</span>
          <strong>{formatPrice(latest?.latest_price)}</strong>
          <small>{messageCount.toLocaleString()} predictions received</small>
        </div>
        <div className={`signal-card ${direction ?? 'neutral'}`}>
          <span>Next-bar signal · reference only</span>
          <strong>{formatDirection(direction)}</strong>
          <small>
            {latest?.source ? `${latest.source} · ${latest.market_event} · ` : ''}
            near-random on price-only features — not used for routing
          </small>
        </div>
        <div className="latency-card">
          <span>Latest pipeline latency</span>
          <strong>{latest ? `${latest.pipeline_latency_ms.toFixed(2)} ms` : '—'}</strong>
          <small>tick arrival → published</small>
        </div>
      </div>

      <div className="chart-card price-chart-card">
        <div className="chart-title-row">
          <div>
            <span className="chart-kicker">Live market</span>
            <h3>Price with directional signal overlay</h3>
          </div>
          <div className="chart-legend">
            <span><i className="legend-line" />Price</span>
            <span><i className="legend-up" />Up</span>
            <span><i className="legend-down" />Down</span>
          </div>
        </div>
        <div className="price-chart">
          {priceHistory.length === 0 && <div className="chart-empty">Building the rolling feature window…</div>}
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={priceHistory} margin={{ top: 12, right: 10, left: 4, bottom: 0 }}>
              <defs>
                <linearGradient id="priceGlow" x1="0" y1="0" x2="1" y2="0">
                  <stop offset="0%" stopColor="#5bbcff" stopOpacity={0.45} />
                  <stop offset="100%" stopColor="#9a7bff" stopOpacity={1} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke="#1c2b38" strokeDasharray="3 5" vertical={false} />
              <XAxis
                dataKey="timeLabel"
                tick={{ fill: '#718396', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                minTickGap={45}
              />
              <YAxis
                dataKey="price"
                domain={['auto', 'auto']}
                orientation="right"
                tick={{ fill: '#718396', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                width={76}
                tickFormatter={(value: number) => `$${Math.round(value).toLocaleString()}`}
              />
              <Tooltip content={<PriceTooltip />} cursor={{ stroke: '#38506a', strokeDasharray: '3 3' }} />
              <Line
                type="monotone"
                dataKey="price"
                stroke="url(#priceGlow)"
                strokeWidth={2.2}
                dot={false}
                isAnimationActive={false}
              />
              <Scatter data={upSignals} dataKey="upPrice" shape={<SignalMarker />} isAnimationActive={false} />
              <Scatter data={downSignals} dataKey="downPrice" shape={<SignalMarker />} isAnimationActive={false} />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="prediction-bottom-grid">
        <div className="chart-card confidence-card">
          <div>
            <span className="chart-kicker">Model certainty</span>
            <h3>Confidence</h3>
          </div>
          <div className="confidence-gauge">
            <ResponsiveContainer width="100%" height="100%">
              <RadialBarChart
                cx="50%"
                cy="50%"
                innerRadius="72%"
                outerRadius="100%"
                barSize={10}
                data={confidenceData}
                startAngle={90}
                endAngle={-270}
              >
                <RadialBar dataKey="value" background={{ fill: '#172632' }} cornerRadius={8} />
              </RadialBarChart>
            </ResponsiveContainer>
            <div className="gauge-center">
              <strong>{latest ? `${confidence.toFixed(1)}%` : '—'}</strong>
              <span>{formatDirection(direction)}</span>
            </div>
          </div>
        </div>

        <div className="chart-card latency-histogram-card">
          <div className="chart-title-row">
            <div>
              <span className="chart-kicker">Observability</span>
              <h3>Pipeline latency distribution</h3>
            </div>
            <span className="unit-label">milliseconds</span>
          </div>
          <div className="latency-chart">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={histogram} margin={{ top: 8, right: 4, left: -24, bottom: 0 }}>
                <CartesianGrid stroke="#1c2b38" strokeDasharray="3 5" vertical={false} />
                <XAxis dataKey="label" tick={{ fill: '#718396', fontSize: 10 }} axisLine={false} tickLine={false} />
                <YAxis allowDecimals={false} tick={{ fill: '#718396', fontSize: 10 }} axisLine={false} tickLine={false} />
                <Tooltip
                  cursor={{ fill: 'rgba(91, 188, 255, 0.06)' }}
                  contentStyle={{ background: '#0d1822', border: '1px solid #273a49', borderRadius: 10 }}
                  labelFormatter={(label) => `${label} ms`}
                />
                <Bar dataKey="count" fill="#5bbcff" radius={[5, 5, 0, 0]} isAnimationActive={false} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </section>
  )
}
