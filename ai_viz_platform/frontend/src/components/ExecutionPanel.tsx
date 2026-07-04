import { useEffect, useMemo, useState } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  RadialBar,
  RadialBarChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import {
  executionDataSource,
  type ExecutionFeed,
  type ExecutionMetric,
} from '../data/executionDataSource'
import { deriveFeedState, FEED_LABELS } from '../data/executionFeedState'
import { PanelHeader } from './PanelHeader'

function formatClock(ms: number): string {
  return new Intl.DateTimeFormat(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(ms)
}

function venueScore(metric: ExecutionMetric): number {
  const fillComponent = metric.fill_rate * 100
  const slippagePenalty = metric.slippage_bps_p95 * 1.7
  const latencyPenalty = metric.latency_ms_p95 / 24
  return Math.max(0, Math.min(100, fillComponent - slippagePenalty - latencyPenalty))
}

function PercentileCard({
  label,
  p50,
  p95,
  p99,
  unit,
}: {
  label: string
  p50: number
  p95: number
  p99?: number
  unit: string
}) {
  return (
    <div className="percentile-card">
      <span>{label}</span>
      <div className="percentile-primary">
        <strong>{p50.toFixed(unit === 'bps' ? 1 : 0)}</strong>
        <small>{unit} p50</small>
      </div>
      <div className="percentile-tail">
        <span>p95 <b>{p95.toFixed(unit === 'bps' ? 1 : 0)}</b></span>
        {p99 !== undefined && <span>p99 <b>{p99.toFixed(0)}</b></span>}
      </div>
    </div>
  )
}

export function ExecutionPanel() {
  const [feed, setFeed] = useState<ExecutionFeed>({
    metrics: [],
    connection: 'connecting',
    lastUpdateMs: null,
  })
  const [nowMs, setNowMs] = useState(() => Date.now())

  useEffect(() => executionDataSource.subscribe(setFeed), [])

  // Re-evaluate staleness once a second even when no new report arrives.
  useEffect(() => {
    const ticker = window.setInterval(() => setNowMs(Date.now()), 1_000)
    return () => window.clearInterval(ticker)
  }, [])

  const metrics = feed.metrics
  const feedState = deriveFeedState(feed, nowMs)

  const ranked = useMemo(
    () =>
      metrics
        .filter((metric) => metric.comparable)
        .map((metric) => ({ ...metric, score: Number(venueScore(metric).toFixed(1)) }))
        .sort((a, b) => b.score - a.score),
    [metrics],
  )
  const best = ranked[0]
  // Non-comparable venues stay out of the ranking but remain visible below.
  const gaugeVenues = useMemo(
    () => [...ranked, ...metrics.filter((metric) => !metric.comparable)],
    [ranked, metrics],
  )

  return (
    <section className="panel execution-panel">
      <PanelHeader
        eyebrow="ROS 2 analyzer"
        title="Execution quality"
        aside={
          <div className="feed-status">
            <span className={`source-pill feed-${feedState}`}>{FEED_LABELS[feedState]}</span>
            {feed.lastUpdateMs !== null && (
              <small className="feed-updated">updated {formatClock(feed.lastUpdateMs)}</small>
            )}
          </div>
        }
      />

      <div className="execution-callout">
        <span>Recommended venue</span>
        <div>
          <strong>{best?.venue ?? '—'}</strong>
          <b>{best ? `${best.score.toFixed(1)} score` : '—'}</b>
        </div>
        <small>Composite of fill rate, p95 slippage, and p95 latency</small>
      </div>

      <div className="chart-card venue-ranking-card">
        <div className="chart-title-row">
          <div>
            <span className="chart-kicker">Smart order routing</span>
            <h3>Venue ranking</h3>
          </div>
          <span className="unit-label">quality score</span>
        </div>
        <div className="venue-chart">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={ranked} layout="vertical" margin={{ top: 8, right: 20, left: 4, bottom: 0 }}>
              <CartesianGrid stroke="#1c2b38" strokeDasharray="3 5" horizontal={false} />
              <XAxis type="number" domain={[0, 100]} hide />
              <YAxis
                type="category"
                dataKey="venue"
                width={72}
                tick={{ fill: '#aebdca', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
              />
              <Tooltip
                cursor={{ fill: 'rgba(91, 188, 255, 0.06)' }}
                contentStyle={{ background: '#0d1822', border: '1px solid #273a49', borderRadius: 10 }}
                formatter={(value) => [`${Number(value).toFixed(1)}`, 'Score']}
              />
              <Bar dataKey="score" fill="#9a7bff" radius={[0, 5, 5, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="fill-rate-section">
        <div className="chart-title-row compact">
          <div>
            <span className="chart-kicker">Order completion</span>
            <h3>Fill-rate gauges</h3>
          </div>
          <span className="unit-label">rolling window</span>
        </div>
        <div className="fill-gauge-grid">
          {gaugeVenues.map((venue) => (
            <div className="mini-gauge" key={venue.venue}>
              <div className="mini-gauge-chart">
                <ResponsiveContainer width="100%" height="100%">
                  <RadialBarChart
                    cx="50%"
                    cy="50%"
                    innerRadius="70%"
                    outerRadius="100%"
                    barSize={7}
                    data={[{ value: venue.fill_rate * 100, fill: venue.comparable ? '#35e0a1' : '#8fa1b1' }]}
                    startAngle={90}
                    endAngle={-270}
                  >
                    <RadialBar dataKey="value" background={{ fill: '#172632' }} cornerRadius={6} />
                  </RadialBarChart>
                </ResponsiveContainer>
                <strong>{(venue.fill_rate * 100).toFixed(1)}%</strong>
              </div>
              <span>{venue.venue}</span>
              <small>
                {venue.window_orders.toLocaleString()} orders
                {venue.comparable ? '' : ' · not ranked'}
              </small>
            </div>
          ))}
        </div>
      </div>

      {best && (
        <div className="percentile-grid">
          <PercentileCard
            label={`${best.venue} slippage`}
            p50={best.slippage_bps_p50}
            p95={best.slippage_bps_p95}
            unit="bps"
          />
          <PercentileCard
            label={`${best.venue} latency`}
            p50={best.latency_ms_p50}
            p95={best.latency_ms_p95}
            p99={best.latency_ms_p99}
            unit="ms"
          />
        </div>
      )}
    </section>
  )
}
