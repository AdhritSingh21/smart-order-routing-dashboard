# A test order submitted to a venue
string order_id          # UUID assigned by OrderSubmitter
string venue             # e.g. "alpaca", "binance_testnet", "coinbase_sandbox"
string symbol            # venue-normalized symbol actually used
string side              # "buy" or "sell"
float64 quantity
float64 quoted_price     # reference price captured at submission
string reference_source  # "sim" | "ask" | "bid" | "last" | "none"
float64 quote_time       # epoch seconds of the reference quote (exchange-provided when available)
float64 submit_time      # epoch seconds at submission (wall clock)
string quote_venue       # venue the reference quote was fetched from
string quote_mode        # "sim" | "venue" | "public"
string execution_mode    # target venue's execution mode: "sim" | "sandbox" | "production"
string timestamp_source  # "exchange_order_book" | "exchange_ticker" | "local" | "sim"
float64 bid              # best bid at quote time (0.0 if unavailable)
float64 ask              # best ask at quote time (0.0 if unavailable)
bool comparable          # venue eligible for cross-venue ranking
