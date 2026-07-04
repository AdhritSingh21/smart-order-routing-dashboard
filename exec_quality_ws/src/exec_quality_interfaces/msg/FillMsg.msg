# An execution event reported back from a venue.
# filled_quantity is INCREMENTAL (this event only); the MetricsEngine
# accumulates increments into the cumulative fill for the order.
string order_id
string venue
string symbol
string status            # normalized status, see exec_quality_analyzer.order_status:
                         # non-terminal: "pending", "open", "partial"
                         # terminal: "filled", "canceled", "canceled_partial",
                         #   "rejected", "rejected_partial", "expired",
                         #   "expired_partial", "timeout", "timeout_partial",
                         #   "dry_run", "error", "quote_failed", "submit_failed"
float64 fill_price       # average fill price for THIS event (0.0 if none)
float64 filled_quantity  # quantity filled in THIS event (0.0 if none)
float64 fill_time        # epoch seconds when this event was confirmed
string error_reason      # populated for failure statuses / terminal reasons
string exchange_order_id # venue-assigned order id ("" if none)
string exchange_status   # raw venue status string ("" if none)
