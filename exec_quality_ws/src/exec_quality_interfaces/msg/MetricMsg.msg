# Per-order execution quality metrics computed by MetricsEngine
string order_id
string venue
string symbol
string status              # terminal status of the order (normalized model)
float64 slippage_bps       # positive = worse than reference, negative = improvement
float64 latency_ms         # >= 0, wall-clock submit -> last fill (or terminal event)
float64 fill_ratio         # filled_quantity / requested_quantity, in [0,1]
bool filled                # true only if fill_ratio reached 1.0
float64 requested_quantity
float64 filled_quantity    # cumulative executed quantity (preserved for terminal partials)
float64 avg_fill_price     # quantity-weighted average execution price (0.0 if no fills)
float64 reference_price    # reference quote captured at submission
string exchange_status     # raw venue status of the final update ("" if none)
string terminal_reason     # why the order reached its terminal state
string quote_mode          # "sim" | "venue" | "public"
string execution_mode      # "sim" | "sandbox" | "production"
bool comparable            # venue eligible for cross-venue ranking
float64 timestamp
