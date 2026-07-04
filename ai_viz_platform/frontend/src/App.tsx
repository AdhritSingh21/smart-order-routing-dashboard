import { ExecutionPanel } from './components/ExecutionPanel'
import { PredictionPanel } from './components/PredictionPanel'
import { RoutingPanel } from './components/RoutingPanel'
import { useMarketStream } from './hooks/useMarketStream'
import './styles.css'

export default function App() {
  const market = useMarketStream()

  return (
    <main className="dashboard-shell">
      <header className="topbar">
        <div className="brand-lockup">
          <div className="brand-mark"><span /></div>
          <div>
            <h1>Smart Order Routing Dashboard</h1>
            <p>ML-driven venue selection fed by a live ROS 2 execution-quality pipeline</p>
          </div>
        </div>
        <div className="topbar-meta">
          <span>ROS 2 Humble</span>
          <span>Venue routing</span>
          <span>Realtime</span>
        </div>
      </header>

      <div className="dashboard-grid">
        <ExecutionPanel />
        <RoutingPanel />
      </div>

      <div className="market-row">
        <PredictionPanel {...market} />
      </div>

      <footer className="dashboard-footer">
        <span>ROS 2 analyzer → dashboard_bridge → /ingest → routing model → /ws → routing panel</span>
        <span>Per-order metrics → rolling venue features → slippage / fill / latency forecasts</span>
      </footer>
    </main>
  )
}
