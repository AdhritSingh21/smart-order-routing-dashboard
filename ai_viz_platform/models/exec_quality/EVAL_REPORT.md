# Execution-Quality Model — Evaluation Report

- **Trained at:** 2026-07-03T02:12:42.182755+00:00
- **Data source:** `simulated-replay` (exec_ml.simulate, orders=30000, seed=7)
- **Demo model:** yes — trained on simulated executions, never presented as production-quality
- **Split:** time-ordered 80/20 (no shuffling)
- **Rows:** 29970 total → 23976 train / 5994 test
- **Train window:** 2023-11-14T22:13:24.965247+00:00 → 2023-11-14T23:19:55.126519+00:00
- **Test window:** 2023-11-14T23:19:55.277305+00:00 → 2023-11-14T23:36:41.731990+00:00

## Slippage (bps) — next-order regression

| | MAE | RMSE |
|---|---|---|
| Model | 2.732 | 4.037 |
| Baseline (recent venue average) | 2.999 | 4.516 |

MAE improvement vs baseline: **8.9%** on 5090 held-out orders.

## Fill probability — next-order classification

| | ROC-AUC | Accuracy | Brier |
|---|---|---|---|
| Model | 0.664 | 0.849 | 0.1213 |
| Baseline (recent fill rate) | 0.628 | 0.843 | 0.1286 |

Test positive (filled) rate: 0.849. With fills this common,
accuracy is dominated by the majority class — Brier score and ROC-AUC are the
honest comparison.

## Latency p95 bound — quantile regression

Share of orders at or under the predicted bound (target 0.95):

| | Coverage | Mean bound (ms) |
|---|---|---|
| Model | 0.909 | 258.4 |
| Baseline (rolling p95) | 0.937 | 374.1 |

## Per-venue (held-out test period)

| Venue | Test rows | Slippage MAE (model) | Slippage MAE (baseline) | Fill accuracy | Realized fill rate |
|---|---|---|---|---|---|
| alpaca | 1998 | 1.954 | 2.156 | 0.886 | 0.886 |
| binance_testnet | 1998 | 3.765 | 4.115 | 0.788 | 0.789 |
| coinbase_sandbox | 1998 | 2.586 | 2.845 | 0.872 | 0.872 |

---

*This model predicts execution quality (venue-level slippage, fill
probability, latency) — it does not predict market direction. Numbers
above come from a strictly time-ordered held-out split and are only as
meaningful as the underlying data source stated at the top.*
