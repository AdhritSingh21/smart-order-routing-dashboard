# Rolling-window venue statistics published by Aggregator.
#
# fill_rate counts ONLY fully filled orders; terminal partials
# (canceled_partial, timeout_partial, ...) count in the denominator.
# Slippage/latency percentiles include every order with executed quantity
# (fill_ratio > 0), so terminal partial executions keep contributing their
# measured execution quality.
string venue
uint32 window_orders
float64 fill_rate            # 0.0 - 1.0
float64 slippage_bps_p50
float64 slippage_bps_p95
float64 latency_ms_p50
float64 latency_ms_p95
float64 latency_ms_p99
bool comparable              # false -> exclude from cross-venue ranking
float64 timestamp
