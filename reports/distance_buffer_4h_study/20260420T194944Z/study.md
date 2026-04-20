# Distance buffer 4H research study

Research-only study. The frozen 1D execution layer is not changed.

## Assumptions

- Dataset 4H: derived from `data\raw\bitvavo\btc-eur_1h.csv` complete 1H buckets.
- 4H useful execution starts: 2022-11-11 16:00:00+00:00.
- 4H derived gaps: 22 gaps, 35 missing 4H intervals; 35 incomplete bins dropped.
- Dataset 1D: `data\raw\bitvavo\btc-eur_1d.csv`.
- 1D useful execution starts: 2019-07-11 00:00:00+00:00.
- 1D gaps: 2 gaps, 2 missing daily intervals.
- Signal: close > SMA125 * 1.03, otherwise OFF.
- Execution: t+1 next candle close.
- Deadband: 10%.
- Base costs: 0.20% fee plus 0.05% slippage per side.
- Stress costs: 0.25% fee plus 0.08% slippage per side.

## Full Sample Base

| name | timeframe | start | end | final_equity | total_return | cagr | sharpe | calmar | max_drawdown | fee_drag | turnover | rebalances | avg_exposure |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Distance buffer 4H | 4h | 2022-10-21 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 12973.728039 | 0.297373 | 0.077453 | 0.417222 | 0.232906 | -0.332549 | 0.837649 | 243.000000 | 243 | 0.324140 |
| Distance buffer 1D official | 1d | 2019-03-08 00:00:00+00:00 | 2026-04-19 00:00:00+00:00 | 118541.339185 | 10.854134 | 0.415520 | 1.068554 | 1.366556 | -0.304064 | 0.792976 | 62.000000 | 62 | 0.494226 |
| Buy & Hold BTC | 4h | 2022-10-21 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 33241.446285 | 2.324145 | 0.410864 | 0.978522 | 0.820391 | -0.500814 | 0.002500 | 1.000000 | 1 | 0.999869 |

## Full Sample Stress

| name | timeframe | start | end | final_equity | total_return | cagr | sharpe | calmar | max_drawdown | fee_drag | turnover | rebalances | avg_exposure |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Distance buffer 4H | 4h | 2022-10-21 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 10675.602336 | 0.067560 | 0.018910 | 0.202921 | 0.048036 | -0.393660 | 1.002125 | 243.000000 | 243 | 0.324140 |
| Distance buffer 1D official | 1d | 2019-03-08 00:00:00+00:00 | 2026-04-19 00:00:00+00:00 | 112788.850671 | 10.278885 | 0.405659 | 1.050941 | 1.319620 | -0.307406 | 1.010746 | 62.000000 | 62 | 0.494226 |
| Buy & Hold BTC | 4h | 2022-10-21 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 33214.786479 | 2.321479 | 0.410539 | 0.978021 | 0.819744 | -0.500814 | 0.003300 | 1.000000 | 1 | 0.999869 |

## Common Period Base Comparison

| name | timeframe | start | end | final_equity | total_return | cagr | sharpe | calmar | max_drawdown | fee_drag | turnover | rebalances | avg_exposure |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Distance buffer 4H | 4h | 2022-11-11 16:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 12973.728039 | 0.297373 | 0.078789 | 0.420690 | 0.236925 | -0.332549 | 0.837649 | 243.000000 | 243 | 0.329550 |
| Distance buffer 1D official | 1d | 2022-11-12 00:00:00+00:00 | 2026-04-18 00:00:00+00:00 | 30544.070751 | 2.054407 | 0.384702 | 1.139946 | 1.817238 | -0.211696 | 0.176555 | 36.000000 | 36 | 0.540303 |
| Buy & Hold BTC | 4h | 2022-11-11 16:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 39942.032560 | 2.994203 | 0.496927 | 1.111151 | 0.992238 | -0.500814 | 0.000000 | 0.000000 | 0 | 1.000000 |

## Temporal Robustness

| window | start | end | winner_by_calmar | both_distance_buffers_negative | 4h_total_return | 1d_total_return | buy_hold_total_return | 4h_cagr | 1d_cagr | 4h_calmar | 1d_calmar | 4h_max_drawdown | 1d_max_drawdown | 4h_rebalances | 1d_rebalances | 4h_fee_drag | 1d_fee_drag |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| w_01 | 2022-11-11 | 2023-05-12 | 4H | False | 0.320919 | 0.182507 | 0.455051 | 0.748164 | 0.399933 | 9.005533 | 2.182223 | -0.083078 | -0.183269 | 16 | 3 | 0.048925 | 0.007466 |
| w_02 | 2023-05-12 | 2023-11-10 | 1D | False | 0.144384 | 0.257962 | 0.416574 | 0.310827 | 0.584964 | 1.887367 | 7.318919 | -0.164688 | -0.079925 | 27 | 16 | 0.065021 | 0.039658 |
| w_03 | 2023-11-10 | 2024-05-10 | 1D | False | -0.127445 | 0.617681 | 0.694611 | -0.239360 | 1.625550 | -0.856743 | 8.776709 | -0.279384 | -0.185212 | 69 | 0 | 0.153939 | 0.000000 |
| w_04 | 2024-05-10 | 2024-11-08 | 1D | False | 0.012200 | 0.127176 | 0.205161 | 0.024634 | 0.271571 | 0.156903 | 1.700959 | -0.157004 | -0.159658 | 41 | 8 | 0.102154 | 0.020509 |
| w_05 | 2024-11-08 | 2025-05-09 | 1D | False | 0.173177 | 0.174480 | 0.295571 | 0.377854 | 0.380928 | 1.429525 | 2.142239 | -0.264322 | -0.177818 | 34 | 2 | 0.098017 | 0.005894 |
| w_06 | 2025-05-09 | 2025-11-07 | 1D | True | -0.047316 | -0.041205 | -0.031421 | -0.092694 | -0.080979 | -0.597740 | -0.495506 | -0.155075 | -0.163426 | 25 | 8 | 0.063547 | 0.020352 |

## Trade Forensics 4H

- complete_trades: 121
- trades_per_year: 34.672006
- win_rate: 0.223140
- loss_rate: 0.776860
- profit_factor: 1.272371
- expectancy_per_trade: 0.003604
- mean_return_per_trade: 0.003604
- median_return_per_trade: -0.009059
- winner_mean_holding_days: 10.913580
- loser_mean_holding_days: 1.189716
- longest_winning_streak: 2
- longest_losing_streak: 16
- best_trade: trade 77 net_return=0.323169
- worst_trade: trade 23 net_return=-0.056505
- top_1_concentration: net_sum=0.323169, share=0.741025
- top_3_concentration: net_sum=0.808491, share=1.853865
- top_5_concentration: net_sum=1.230090, share=2.820589
- top_10_concentration: net_sum=1.792670, share=4.110582

## Trade Forensics 1D

- complete_trades: 31
- trades_per_year: 4.356579
- win_rate: 0.387097
- loss_rate: 0.612903
- profit_factor: 8.516689
- expectancy_per_trade: 0.159007
- mean_return_per_trade: 0.159007
- median_return_per_trade: -0.006524
- winner_mean_holding_days: 88.166667
- loser_mean_holding_days: 11.894737
- longest_winning_streak: 2
- longest_losing_streak: 3
- best_trade: trade 10 net_return=3.383493
- worst_trade: trade 1 net_return=-0.139265
- top_1_concentration: net_sum=3.383493, share=0.686416
- top_3_concentration: net_sum=4.985403, share=1.011398
- top_5_concentration: net_sum=5.349732, share=1.085311
- top_10_concentration: net_sum=5.580191, share=1.132064

## Operating Read

- 4H rebalances/common period: 243 vs 1D 36.
- Rebalance ratio 4H/1D: 6.75.
- Complete-trade frequency ratio 4H/1D: 7.96.
- Paper/manual execution is only reasonable if the operator can review 4H closes consistently.
- Any CAGR edge must be discounted for missed 4H closes, weekend attention and higher decision fatigue.

## Verdict

**La version 4H no vale la pena frente a la 1D**

Razon principal: no mejora suficientemente el binomio CAGR/Calmar frente al ruido operativo.
