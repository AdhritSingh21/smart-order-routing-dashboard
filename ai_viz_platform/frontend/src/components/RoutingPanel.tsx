import { useEffect, useMemo, useState } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import {
  executionDataSource,
  type ExecutionFeed,
} from '../data/executionDataSource'
import { deriveFeedState, FEED_LABELS } from '../data/executionFeedState'
import {
  routingDataSource,
  type RoutingFeed,
  type RoutingValidation,
} from '../data/routingDataSource'
import { PanelHeader } from './PanelHeader'

function formatClock(ms: number): string {
  return new Intl.DateTimeFormat(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(ms)
}

function fmt(value: number | null | undefined, digits: number, suffix = ''): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—'
  return `${value.toFixed(digits)}${suffix}`
}

function fmtPct(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—'
  return `${(value * 100).toFixed(1)}%`
}

function ValidationStrip({ validation }: { validation: RoutingValidation }) {
  const scored = Math.min(validation.orders_scored, validation.window)
  return (
    <div className="validation-grid">
      <div className="stat-card">
        <span>Slippage MAE · last {scored || validation.window} orders</span>
        <strong>{fmt(validation.slippage_mae, 2, ' bps')}</strong>
        <small>naive baseline {fmt(validation.slippage_baseline_mae, 2, ' bps')}</small>
      </div>
      <div className="stat-card">
        <span>Fill prediction accuracy</span>
        <strong>{fmtPct(validation.fill_accuracy)}</strong>
        <small>Brier {fmt(validation.fill_brier, 3)}</small>
      </div>
      <div className="stat-card">
        <span>Best-venue hit rate</span>
        <strong>{fmtPct(validation.recommendation_hit_rate)}</strong>
        <small>{validation.recommendation_windows} scored windows</small>
      </div>
      <div className="stat-card">
        <span>Latency p95 coverage</span>
        <strong>{fmtPct(validation.latency_p95_coverage)}</strong>
        <small>target 95%</small>
      </div>
    </div>
  )
}

export function RoutingPanel() {
  const [feed, setFeed] = useState<RoutingFeed>({
    frame: null,
    connection: 'connecting',
    lastUpdateMs: null,
  })
  const [execFeed, setExecFeed] = useState<ExecutionFeed>({
    metrics: [],
    connection: 'connecting',
    lastUpdateMs: null,
  })
  const [nowMs, setNowMs] = useState(() => Date.now())

  useEffect(() => routingDataSource.subscribe(setFeed), [])
  useEffect(() => executionDataSource.subscribe(setExecFeed), [])
  useEffect(() => {
    const ticker = window.setInterval(() => setNowMs(Date.now()), 1_000)
    return () => window.clearInterval(ticker)
  }, [])

  const frame = feed.frame
  const feedState = deriveFeedState(
    {
      metrics: frame ? frame.venues : [],
      connection: feed.connection,
      lastUpdateMs: feed.lastUpdateMs,
    },
    nowMs,
  )
  const isDemo = frame?.model_status === 'demo'
  const recommended = frame?.venues.find((venue) => venue.recommended) ?? null

  // Predicted vs observed slippage, joined by venue: the observed p50 comes
  // from the analyzer's venue reports once realized metrics arrive.
  const slippageComparison = useMemo(() => {
    if (!frame) return []
    return frame.venues
      .filter((venue) => venue.status === 'ok')
      .map((venue) => ({
        venue: venue.venue,
        predicted: venue.predicted_slippage_bps,
        observed:
          execFeed.metrics.find((metric) => metric.venue === venue.venue)
            ?.slippage_bps_p50 ?? null,
      }))
  }, [frame, execFeed.metrics])

  return (
    <section className="panel routing-panel">
      <PanelHeader
        eyebrow="Routing model"
        title="ML venue selection"
        aside={
          <div className="feed-status">
            <span className={`source-pill feed-${feedState}`}>{FEED_LABELS[feedState]}</span>
            {feed.lastUpdateMs !== null && (
              <small className="feed-updated">updated {formatClock(feed.lastUpdateMs)}</small>
            )}
          </div>
        }
      />

      {frame && (
        <div className={`model-status-banner ${frame.model_status}`}>
          {isDemo
            ? 'DEMO MODEL — trained on simulated executions, not real market data'
            : 'Real-data model'}
        </div>
      )}

      <div className="execution-callout routing-callout">
        <span>Model-recommended venue</span>
        <div>
          <strong>{recommended?.venue ?? '—'}</strong>
          <b>{recommended ? `${fmt(recommended.routing_score, 1)} score` : '—'}</b>
        </div>
        <small>
          Ranked by predicted fill probability, expected slippage, and p95 latency
          for the next order
        </small>
      </div>

      <div className="chart-card routing-table-card">
        <div className="chart-title-row compact">
          <div>
            <span className="chart-kicker">Next-order forecast</span>
            <h3>Predicted execution quality by venue</h3>
          </div>
          <span className="unit-label">per venue</span>
        </div>
        {!frame && (
          <div className="routing-empty">
            Awaiting routing predictions — the model publishes after 10 observed
            orders per venue.
          </div>
        )}
        {frame && (
          <table className="routing-table">
            <thead>
              <tr>
                <th>Venue</th>
                <th>Slippage</th>
                <th>Fill prob</th>
                <th>Lat p95</th>
                <th>Score</th>
              </tr>
            </thead>
            <tbody>
              {frame.venues.map((venue) => (
                <tr key={venue.venue} className={venue.recommended ? 'recommended' : ''}>
                  <td>
                    {venue.venue}
                    {venue.recommended && <em className="route-badge">route</em>}
                  </td>
                  {venue.status === 'ok' ? (
                    <>
                      <td>{fmt(venue.predicted_slippage_bps, 2)} bps</td>
                      <td>{fmtPct(venue.predicted_fill_probability)}</td>
                      <td>{fmt(venue.predicted_latency_ms_p95, 0)} ms</td>
                      <td>{fmt(venue.routing_score, 1)}</td>
                    </>
                  ) : (
                    <td colSpan={4} className="warming-cell">
                      warming up · {venue.orders_observed}/10 orders observed
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {slippageComparison.length > 0 && (
        <div className="chart-card slippage-compare-card">
          <div className="chart-title-row compact">
            <div>
              <span className="chart-kicker">Model vs realized</span>
              <h3>Slippage — predicted vs observed p50</h3>
            </div>
            <span className="unit-label">bps</span>
          </div>
          <div className="slippage-compare-chart">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={slippageComparison}
                margin={{ top: 8, right: 8, left: -18, bottom: 0 }}
              >
                <CartesianGrid stroke="#1c2b38" strokeDasharray="3 5" vertical={false} />
                <XAxis
                  dataKey="venue"
                  tick={{ fill: '#718396', fontSize: 10 }}
                  axisLine={false}
                  tickLine={false}
                />
                <YAxis tick={{ fill: '#718396', fontSize: 10 }} axisLine={false} tickLine={false} />
                <Tooltip
                  cursor={{ fill: 'rgba(91, 188, 255, 0.06)' }}
                  contentStyle={{ background: '#0d1822', border: '1px solid #273a49', borderRadius: 10 }}
                  formatter={(value) => (value === null ? '—' : `${Number(value).toFixed(2)} bps`)}
                />
                <Legend wrapperStyle={{ fontSize: 10, color: '#718396' }} iconSize={8} />
                <Bar dataKey="predicted" name="Predicted" fill="#9a7bff" radius={[4, 4, 0, 0]} isAnimationActive={false} />
                <Bar dataKey="observed" name="Observed p50" fill="#5bbcff" radius={[4, 4, 0, 0]} isAnimationActive={false} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {frame && (
        <div className="validation-section">
          <div className="chart-title-row compact">
            <div>
              <span className="chart-kicker">Live validation</span>
              <h3>Rolling prediction error vs realized outcomes</h3>
            </div>
            <span className="unit-label">last {frame.validation.window} orders</span>
          </div>
          <ValidationStrip validation={frame.validation} />
        </div>
      )}
    </section>
  )
}
