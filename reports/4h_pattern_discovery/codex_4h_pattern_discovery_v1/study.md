# 4H Deep Pattern Discovery & Strategy Improvement Study

Research-only. No cambia la estrategia oficial 1D ni la capa operativa congelada.

## Dataset y auditoria

- Fuente principal: `data\raw\bitvavo\btc-eur_1h.csv` derivado desde 1H Bitvavo BTC/EUR.
- Agregacion 1H -> 4H: open primer open, high maximo, low minimo, close ultimo close, volume suma.
- Solo se conservan buckets completos de cuatro velas 1H.
- Alineacion UTC: Unix epoch anchored 00:00/04:00/08:00/12:00/16:00/20:00 UTC.
- 4H: 2022-10-21 20:00:00+00:00 -> 2026-04-18 12:00:00+00:00, 7614 velas.
- Gaps 1H fuente: 22 gaps, 68 intervalos faltantes.
- Gaps 4H derivados: 22 gaps, 35 intervalos faltantes; 35 buckets incompletos descartados.
- Periodo comun de evaluacion: 2023-02-23 20:00:00+00:00 -> 2026-04-18 12:00:00+00:00.

## Patrones encontrados

- `body_pct` / p25-p50 0.001511..0.007645: 3 trades, win rate 0.666667, media neta 0.574069, edge vs universo 0.441511.
- `failed_attempts_90` / p25-p50 1.000000..2.000000: 3 trades, win rate 1.000000, media neta 0.505917, edge vs universo 0.373359.
- `atr_pctile_180` / >p75 0.900000: 3 trades, win rate 0.666667, media neta 0.498615, edge vs universo 0.366057.
- `atr_pct` / p50-p75 0.013238..0.014358: 3 trades, win rate 1.000000, media neta 0.435313, edge vs universo 0.302755.
- `body_to_range` / >p75 0.650185: 3 trades, win rate 0.666667, media neta 0.424533, edge vs universo 0.291975.
- `sma750_slope30_pct` / p50-p75 0.000726..0.003168: 3 trades, win rate 0.333333, media neta 0.406756, edge vs universo 0.274198.
- `sma750_slope30_atr` / p50-p75 0.067638..0.196635: 3 trades, win rate 0.333333, media neta 0.406756, edge vs universo 0.274198.
- `compression_20_120` / p50-p75 0.997321..1.258285: 3 trades, win rate 0.666667, media neta 0.376336, edge vs universo 0.243778.

### Evidencia cuantitativa principal

| feature | bucket | trade_count | win_rate | mean_net_return | median_net_return | total_net_return_sum | edge_vs_all |
| --- | --- | --- | --- | --- | --- | --- | --- |
| body_pct | p25-p50 0.001511..0.007645 | 3 | 0.666667 | 0.574069 | 0.491204 | 1.722208 | 0.441511 |
| failed_attempts_90 | p25-p50 1.000000..2.000000 | 3 | 1.000000 | 0.505917 | 0.263873 | 1.517750 | 0.373359 |
| atr_pctile_180 | >p75 0.900000 | 3 | 0.666667 | 0.498615 | 0.263873 | 1.495844 | 0.366057 |
| atr_pct | p50-p75 0.013238..0.014358 | 3 | 1.000000 | 0.435313 | 0.052063 | 1.305939 | 0.302755 |
| body_to_range | >p75 0.650185 | 3 | 0.666667 | 0.424533 | 0.052063 | 1.273598 | 0.291975 |
| sma750_slope30_pct | p50-p75 0.000726..0.003168 | 3 | 0.333333 | 0.406756 | -0.010736 | 1.220268 | 0.274198 |
| sma750_slope30_atr | p50-p75 0.067638..0.196635 | 3 | 0.333333 | 0.406756 | -0.010736 | 1.220268 | 0.274198 |
| compression_20_120 | p50-p75 0.997321..1.258285 | 3 | 0.666667 | 0.376336 | 0.052063 | 1.129009 | 0.243778 |
| dist_threshold | <=p25 0.002219 | 4 | 0.250000 | 0.299189 | -0.016944 | 1.196754 | 0.166631 |
| dist_sma750 | <=p25 0.037297 | 4 | 0.250000 | 0.299189 | -0.016944 | 1.196754 | 0.166631 |
| close_position | <=p25 0.296089 | 4 | 0.250000 | 0.299189 | -0.016944 | 1.196754 | 0.166631 |
| sma750_slope30_pct | <=p25 -0.000458 | 3 | 1.000000 | 0.269047 | 0.263873 | 0.807140 | 0.136489 |
| sma750_slope30_atr | <=p25 -0.014004 | 3 | 1.000000 | 0.269047 | 0.263873 | 0.807140 | 0.136489 |
| green_streak | <=p25 0.000000 | 5 | 0.200000 | 0.231360 | -0.017427 | 1.156802 | 0.098802 |
| green_streak | >p75 3.000000 | 3 | 1.000000 | 0.182904 | 0.052063 | 0.548712 | 0.050346 |
| break_prev_high_20 | false | 8 | 0.250000 | 0.151522 | -0.017608 | 1.212175 | 0.018964 |
| break_prev_high_30 | false | 8 | 0.250000 | 0.151522 | -0.017608 | 1.212175 | 0.018964 |
| break_prev_high_50 | false | 8 | 0.250000 | 0.151522 | -0.017608 | 1.212175 | 0.018964 |

## Salidas

| exit_type | trades | mean_net_return | median_exit_dist_threshold | mean_deterioration_3bar_return | mean_gave_back_from_prior_20_high | median_exit_atr_pctile_180 |
| --- | --- | --- | --- | --- | --- | --- |
| false_or_loss_exit | 8 | -0.042220 | -0.018038 | -0.022957 | -0.055465 | 0.855556 |
| winner_exit | 5 | 0.412203 | -0.007972 | -0.009628 | -0.031216 | 0.272222 |

## 2025 especifico

- Trades 2025 en 4H previa: 2 cerrados; 1 positivos y 1 no positivos.
- Retorno medio por trade 2025: 0.012583; mediana 0.012583.
- Slope SMA750 medio en entradas 2025: -0.073 ATR; intentos previos 90 velas medio: 2.00.
- Lectura: 2025 se evalua como chop si aparecen entradas cerca de threshold sin ruptura de maximos ni pendiente clara.

| trade_id | execution_entry_date | execution_exit_date | net_return | dist_threshold | sma750_slope30_atr | break_prev_high_30 | body_to_range | close_position | atr_pctile_180 | compression_20_120 | failed_attempts_90 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 12 | 2025-05-09 00:00:00+00:00 | 2025-08-25 00:00:00+00:00 | 0.052063 | 0.033734 | -0.160544 | True | 0.658730 | 0.669444 | 0.672222 | 1.229934 | 1.000000 |
| 13 | 2025-10-02 00:00:00+00:00 | 2025-10-11 08:00:00+00:00 | -0.026896 | 0.013899 | 0.014036 | True | 0.759608 | 0.943581 | 0.800000 | 1.272682 | 3.000000 |

## Candidatos construidos

- `4H slope recent-high quality`: Long threshold only when SMA slope is positive, close breaks prior high, and close is in the upper candle half. Nace de: Breakouts with positive SMA slope and recent-high confirmation had better follow-through.
- `4H compression quality breakout`: Long threshold only after relative range compression, a prior-high break, body quality, and close near the high. Nace de: Best baseline trades came after non-expanded volatility and decisive candles.
- `4H anti-whipsaw cooldown`: Require slope not clearly negative and skip entries after an elevated failed-attempt count. Nace de: Losses clustered after repeated failed threshold attempts.
- `4H asymmetric exit repair`: Keep normal threshold entry, but exit on first confirmed threshold loss to reduce give-back. Nace de: False exits and late exits showed deterioration before the 3/3 exit confirmed.

## Ranking de candidatos

| name | timeframe | sma_window | buffer | entry_persistence | exit_persistence | confirmation_price | start | end | final_equity | total_return | cagr | sharpe | calmar | max_drawdown | fee_drag | fee_only_drag | slippage_drag | turnover | rebalances | avg_exposure | candidate_family | variant | pattern_source | logic | complete_trades | trades_per_year | profit_factor | expectancy | top_1_concentration | top_3_concentration | top_5_concentration |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4H asymmetric exit repair \| SMA700 \| buffer 3.0% \| p3/1 | 4h | 700 | 0.030000 | 3 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 29951.780452 | 1.995178 | 0.416967 | 1.200774 | 2.099694 | -0.198585 | 0.246817 | 0.197453 | 0.049363 | 51.000000 | 51 | 0.529720 | 4H asymmetric exit repair | asymmetric_exit | False exits and late exits showed deterioration before the 3/3 exit confirmed. | Keep normal threshold entry, but exit on first confirmed threshold loss to reduce give-back. | 25 | 7.942520 | 7.217293 | 0.069228 | 0.578225 | 1.019509 | 1.151752 |
| 4H anti-whipsaw cooldown \| SMA750 \| buffer 3.0% \| p2/1 | 4h | 750 | 0.030000 | 2 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 30868.498826 | 2.086850 | 0.430604 | 1.226937 | 2.044887 | -0.210576 | 0.218848 | 0.175078 | 0.043770 | 44.000000 | 44 | 0.534674 | 4H anti-whipsaw cooldown | anti_whipsaw\|failed<=3.000000 | Losses clustered after repeated failed threshold attempts. | Require slope not clearly negative and skip entries after an elevated failed-attempt count. | 22 | 6.989417 | 4.849321 | 0.071388 | 0.621679 | 1.096520 | 1.242107 |
| 4H slope recent-high quality \| SMA700 \| buffer 3.0% \| p2/1 | 4h | 700 | 0.030000 | 2 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 25931.130504 | 1.593113 | 0.353541 | 1.131218 | 1.716842 | -0.205925 | 0.086933 | 0.069546 | 0.017387 | 20.000000 | 20 | 0.424097 | 4H slope recent-high quality | slope_recent_high | Breakouts with positive SMA slope and recent-high confirmation had better follow-through. | Long threshold only when SMA slope is positive, close breaks prior high, and close is in the upper candle half. | 10 | 3.177008 | 10.705228 | 0.122031 | 0.604884 | 1.024094 | 1.089157 |
| 4H compression quality breakout \| SMA750 \| buffer 2.5% \| p1/1 | 4h | 750 | 0.025000 | 1 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 21830.086936 | 1.183009 | 0.281499 | 0.953022 | 1.244365 | -0.226219 | 0.105522 | 0.084417 | 0.021104 | 30.000000 | 30 | 0.468531 | 4H compression quality breakout | compression_quality\|compression<=0.997321\|body>=0.468246\|closepos>=0.477725 | Best baseline trades came after non-expanded volatility and decisive candles. | Long threshold only after relative range compression, a prior-high break, body quality, and close near the high. | 15 | 4.765512 | 4.522340 | 0.075489 | 0.742059 | 1.261335 | 1.278552 |

## Comparacion base

| name | timeframe | sma_window | buffer | entry_persistence | exit_persistence | confirmation_price | start | end | final_equity | total_return | cagr | sharpe | calmar | max_drawdown | fee_drag | fee_only_drag | slippage_drag | turnover | rebalances | avg_exposure |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4H asymmetric exit repair \| SMA700 \| buffer 3.0% \| p3/1 | 4h | 700 | 0.030000 | 3 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 29951.780452 | 1.995178 | 0.416967 | 1.200774 | 2.099694 | -0.198585 | 0.246817 | 0.197453 | 0.049363 | 51.000000 | 51 | 0.529720 |
| 4H anti-whipsaw cooldown \| SMA750 \| buffer 3.0% \| p2/1 | 4h | 750 | 0.030000 | 2 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 30868.498826 | 2.086850 | 0.430604 | 1.226937 | 2.044887 | -0.210576 | 0.218848 | 0.175078 | 0.043770 | 44.000000 | 44 | 0.534674 |
| 4H slope recent-high quality \| SMA700 \| buffer 3.0% \| p2/1 | 4h | 700 | 0.030000 | 2 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 25931.130504 | 1.593113 | 0.353541 | 1.131218 | 1.716842 | -0.205925 | 0.086933 | 0.069546 | 0.017387 | 20.000000 | 20 | 0.424097 |
| 4H compression quality breakout \| SMA750 \| buffer 2.5% \| p1/1 | 4h | 750 | 0.025000 | 1 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 21830.086936 | 1.183009 | 0.281499 | 0.953022 | 1.244365 | -0.226219 | 0.105522 | 0.084417 | 0.021104 | 30.000000 | 30 | 0.468531 |
| Previous best 4H corrected | 4h | 750 | 0.035000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 31371.088867 | 2.137109 | 0.437963 | 1.236265 | 2.106807 | -0.207880 | 0.107655 | 0.086124 | 0.021531 | 26.000000 | 26 | 0.542395 |
| Distance buffer 1D official | 1d | 125 | 0.030000 | 1 | 1 | close | 2023-02-24 00:00:00+00:00 | 2026-04-18 00:00:00+00:00 | 26819.652697 | 1.681965 | 0.368355 | 1.099686 | 1.740019 | -0.211696 | 0.152831 | 0.122265 | 0.030566 | 35.000000 | 35 | 0.554395 |
| Buy & Hold BTC | 4h | 1 | 0.000000 | 1 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 28656.756039 | 1.865676 | 0.397209 | 0.954904 | 0.793127 | -0.500814 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0 | 1.000000 |

## Comparacion stress

| name | timeframe | sma_window | buffer | entry_persistence | exit_persistence | confirmation_price | start | end | final_equity | total_return | cagr | sharpe | calmar | max_drawdown | fee_drag | fee_only_drag | slippage_drag | turnover | rebalances | avg_exposure |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4H asymmetric exit repair \| SMA700 \| buffer 3.0% \| p3/1 | 4h | 700 | 0.030000 | 3 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 28750.929695 | 1.875093 | 0.398666 | 1.162128 | 1.899935 | -0.209832 | 0.317640 | 0.240637 | 0.077004 | 51.000000 | 51 | 0.529720 |
| 4H anti-whipsaw cooldown \| SMA750 \| buffer 3.0% \| p2/1 | 4h | 750 | 0.030000 | 2 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 29797.778080 | 1.979778 | 0.414649 | 1.193675 | 1.957351 | -0.211842 | 0.282690 | 0.214159 | 0.068531 | 44.000000 | 44 | 0.534674 |
| 4H slope recent-high quality \| SMA700 \| buffer 3.0% \| p2/1 | 4h | 700 | 0.030000 | 2 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 25518.346425 | 1.551835 | 0.346658 | 1.114738 | 1.673074 | -0.207198 | 0.113683 | 0.086124 | 0.027560 | 20.000000 | 20 | 0.424097 |
| 4H compression quality breakout \| SMA750 \| buffer 2.5% \| p1/1 | 4h | 750 | 0.025000 | 1 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 21310.914285 | 1.131091 | 0.271737 | 0.928374 | 1.157007 | -0.234862 | 0.137326 | 0.104035 | 0.033291 | 30.000000 | 30 | 0.468531 |
| Previous best 4H corrected | 4h | 750 | 0.035000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 30723.450886 | 2.072345 | 0.428465 | 1.216708 | 2.005430 | -0.213652 | 0.140298 | 0.106287 | 0.034012 | 26.000000 | 26 | 0.542395 |
| Distance buffer 1D official | 1d | 125 | 0.030000 | 1 | 1 | close | 2023-02-24 00:00:00+00:00 | 2026-04-18 00:00:00+00:00 | 26076.994548 | 1.607699 | 0.356195 | 1.072812 | 1.638672 | -0.217368 | 0.198343 | 0.150260 | 0.048083 | 35.000000 | 35 | 0.554395 |
| Buy & Hold BTC | 4h | 1 | 0.000000 | 1 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 28656.756039 | 1.865676 | 0.397209 | 0.954904 | 0.793127 | -0.500814 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0 | 1.000000 |

## Robustez temporal rolling

| window | start | end | winner_by_calmar | all_negative_return | new_4h_total_return | previous_4h_total_return | official_1d_total_return | buy_hold_total_return | new_4h_calmar | previous_4h_calmar | official_1d_calmar | buy_hold_calmar | new_4h_rebalances | previous_4h_rebalances | official_1d_rebalances |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| rolling_01 | 2023-02-23 | 2024-02-23 | buy_hold | False | 0.672776 | 0.676250 | 0.706038 | 1.050200 | 3.439746 | 3.284931 | 4.420697 | 5.558489 | 24 | 15 | 18 |
| rolling_02 | 2023-08-24 | 2024-08-23 | official_1d | False | 1.147625 | 1.103802 | 1.128671 | 1.239162 | 5.784680 | 5.314955 | 6.099895 | 4.110779 | 10 | 8 | 6 |
| rolling_03 | 2024-02-22 | 2025-02-21 | new_4h | False | 0.846102 | 0.882226 | 0.732167 | 0.960918 | 4.264560 | 4.247837 | 3.956651 | 3.187557 | 12 | 6 | 8 |
| rolling_04 | 2024-08-22 | 2025-08-22 | previous_4h | False | 0.501039 | 0.588690 | 0.414626 | 0.792062 | 2.646587 | 3.030507 | 1.970041 | 2.265110 | 14 | 3 | 5 |
| rolling_05 | 2025-02-20 | 2026-02-20 | previous_4h | True | -0.050029 | -0.010571 | -0.141131 | -0.384447 | -0.342248 | -0.080000 | -0.864127 | -0.768052 | 16 | 5 | 9 |

## Robustez temporal expanding

| window | start | end | winner_by_calmar | all_negative_return | new_4h_total_return | previous_4h_total_return | official_1d_total_return | buy_hold_total_return | new_4h_calmar | previous_4h_calmar | official_1d_calmar | buy_hold_calmar | new_4h_rebalances | previous_4h_rebalances | official_1d_rebalances |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| expanding_01 | 2023-02-23 | 2024-02-23 | buy_hold | False | 0.704304 | 0.676250 | 0.752004 | 1.088842 | 3.611610 | 3.294563 | 4.912155 | 5.801862 | 24 | 15 | 18 |
| expanding_02 | 2023-02-23 | 2024-08-23 | official_1d | False | 1.134172 | 1.054943 | 1.144644 | 1.410849 | 3.324763 | 2.976576 | 3.595615 | 2.655446 | 31 | 20 | 23 |
| expanding_03 | 2023-02-23 | 2025-02-21 | official_1d | False | 2.176775 | 2.185622 | 2.067980 | 3.135696 | 3.956268 | 3.791354 | 4.076089 | 3.440916 | 36 | 21 | 26 |
| expanding_04 | 2023-02-23 | 2025-08-22 | new_4h | False | 2.203476 | 2.264669 | 2.033868 | 3.308734 | 2.998888 | 2.923295 | 2.664349 | 2.277369 | 45 | 23 | 28 |
| expanding_05 | 2023-02-23 | 2026-02-20 | previous_4h | False | 1.995178 | 2.137109 | 1.681965 | 1.533758 | 2.231826 | 2.240359 | 1.846700 | 0.728169 | 51 | 26 | 35 |

## Bootstrap block

| name | timeframe | sharpe_p05 | sharpe_median | sharpe_p95 | calmar_p05 | calmar_median | calmar_p95 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| new_4h | 4h | 0.271337 | 1.223216 | 2.113781 | 0.081147 | 1.578633 | 4.241007 |
| previous_4h | 4h | 0.334515 | 1.256285 | 2.120844 | 0.149677 | 1.652449 | 4.353296 |
| official_1d | 1d | 0.138597 | 1.137220 | 2.005444 | -0.010053 | 1.328775 | 4.236096 |
| buy_hold | 4h | 0.036701 | 0.968684 | 1.955597 | -0.136422 | 0.908548 | 3.352972 |

## Trade log analysis mejor candidato

Mejor candidato nuevo: `4H asymmetric exit repair | SMA700 | buffer 3.0% | p3/1`.

- complete_trades: 25
- trades_per_year: 7.942520
- win_rate: 0.320000
- loss_rate: 0.680000
- profit_factor: 7.217293
- expectancy_per_trade: 0.069228
- mean_return_per_trade: 0.069228
- median_return_per_trade: -0.009035
- std_return_per_trade: 0.227407
- winner_mean_holding_days: 64.312500
- loser_mean_holding_days: 4.539216
- longest_winning_streak: 2
- longest_losing_streak: 5
- best_trade: trade 13, net_return 1.000730, holding_days 195.833333.
- worst_trade: trade 20, net_return -0.052724, holding_days 44.833333.
- top_1_concentration: net_sum 1.000730, share 0.578225.
- top_3_concentration: net_sum 1.764459, share 1.019509.
- top_5_concentration: net_sum 1.993331, share 1.151752.
- top_10_concentration: net_sum 1.999207, share 1.155147.

### Concentracion vs 4H previa

- Dependencia extrema reducida: si. Top 1 nuevo 0.578225 vs previo 0.724462; top 3 nuevo 1.019509 vs previo 1.162631.

## Simulacion practica 2025

| name | timeframe | final_value | invested_capital | return_on_contributions | time_weighted_nav_drawdown | invested_capital_drawdown | fee_drag |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 4H asymmetric exit repair \| SMA700 \| buffer 3.0% \| p3/1 | 4h | 6189.404049 | 6500.000000 | -0.047784 | -0.199754 | -0.074430 | 0.026644 |
| Previous best 4H corrected | 4h | 6470.354921 | 6500.000000 | -0.004561 | -0.202251 | -0.077319 | 0.008899 |
| Distance buffer 1D official | 1d | 6033.225015 | 6500.000000 | -0.071812 | -0.184087 | -0.120309 | 0.015998 |
| Buy & Hold BTC | 4h | 5543.259692 | 6500.000000 | -0.147191 | -0.324875 | -0.219145 | 0.002308 |

- El candidato nuevo no mejora 2025 frente a la 4H previa: -0.047784 vs -0.004561.

## Veredicto final

**No encontramos una mejora real sobre la 4H corregida previa**

Razon principal: los filtros derivados de patrones no superan a la 4H previa en el conjunto operativo relevante.
