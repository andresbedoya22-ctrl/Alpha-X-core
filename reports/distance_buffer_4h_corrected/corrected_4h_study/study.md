# Distance Buffer 4H Corrected Study

Research-only parallel study. The frozen 1D execution layer is not changed.

## Assumptions

- Primary dataset: BTC/EUR Bitvavo from `data\raw\bitvavo\btc-eur_1h.csv`.
- 1H -> 4H aggregation: open first, high max, low min, close last, volume sum; only complete 4-hour buckets retained.
- 4H bucket alignment: Unix epoch anchored 00:00/04:00/08:00/12:00/16:00/20:00 UTC.
- 4H range: 2022-10-21 20:00:00+00:00 -> 2026-04-18 12:00:00+00:00 (7614 rows).
- 4H gaps after aggregation: 22 gaps, 35 missing 4H intervals; 35 incomplete buckets dropped.
- Source 1H gaps: 22 gaps, 68 missing 1H intervals.
- 1D official reference dataset: `data\raw\bitvavo\btc-eur_1d.csv`, 2019-03-08 00:00:00+00:00 -> 2026-04-19 00:00:00+00:00.
- Official 1D reference: close > SMA125 * 1.03, t+1, 100%/0%, deadband 10%.
- Corrected 4H grid: SMA700/750/800, buffers 2.5%/3.0%/3.5%, persistence 1/2/3.
- Base costs: 0.20% fee plus 0.05% slippage per side.
- Stress costs: 0.25% fee plus 0.08% slippage per side.

## Ranking 4H Base

| name | timeframe | sma_window | buffer | entry_persistence | exit_persistence | confirmation_price | start | end | final_equity | total_return | cagr | sharpe | calmar | max_drawdown | fee_drag | fee_only_drag | slippage_drag | turnover | rebalances | avg_exposure |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Distance buffer 4H corrected | 4h | 750 | 0.035000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 31371.088867 | 2.137109 | 0.437963 | 1.236265 | 2.106807 | -0.207880 | 0.107655 | 0.086124 | 0.021531 | 26.000000 | 26 | 0.542395 |
| Distance buffer 4H corrected | 4h | 800 | 0.030000 | 2 | 2 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 30754.589490 | 2.075459 | 0.428925 | 1.216792 | 1.957911 | -0.219073 | 0.193579 | 0.154863 | 0.038716 | 36.000000 | 36 | 0.553322 |
| Distance buffer 4H corrected | 4h | 800 | 0.035000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 28473.849164 | 1.847385 | 0.394370 | 1.151123 | 1.849786 | -0.213197 | 0.144019 | 0.115215 | 0.028804 | 30.000000 | 30 | 0.544289 |
| Distance buffer 4H corrected | 4h | 750 | 0.035000 | 1 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 28431.129579 | 1.843113 | 0.393705 | 1.146203 | 1.802906 | -0.218372 | 0.357648 | 0.286118 | 0.071530 | 76.000000 | 76 | 0.541667 |
| Distance buffer 4H corrected | 4h | 750 | 0.030000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 29232.497613 | 1.923250 | 0.406067 | 1.166827 | 1.802344 | -0.225299 | 0.139716 | 0.111773 | 0.027943 | 32.000000 | 32 | 0.552739 |
| Distance buffer 4H corrected | 4h | 750 | 0.035000 | 2 | 2 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 29471.234091 | 1.947123 | 0.409705 | 1.178901 | 1.780380 | -0.230122 | 0.157851 | 0.126280 | 0.031570 | 38.000000 | 38 | 0.542104 |
| Distance buffer 4H corrected | 4h | 700 | 0.035000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 28448.614522 | 1.844861 | 0.393977 | 1.151344 | 1.767957 | -0.222843 | 0.142226 | 0.113781 | 0.028445 | 33.000000 | 33 | 0.525495 |
| Distance buffer 4H corrected | 4h | 700 | 0.030000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 29264.864427 | 1.926486 | 0.406561 | 1.174414 | 1.767117 | -0.230070 | 0.169997 | 0.135998 | 0.033999 | 39.000000 | 39 | 0.541230 |
| Distance buffer 4H corrected | 4h | 800 | 0.030000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 31011.443263 | 2.101144 | 0.432705 | 1.222797 | 1.762478 | -0.245510 | 0.162543 | 0.130035 | 0.032509 | 30.000000 | 30 | 0.553176 |
| Distance buffer 4H corrected | 4h | 800 | 0.025000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 30143.526639 | 2.014353 | 0.419843 | 1.194811 | 1.749288 | -0.240008 | 0.141608 | 0.113287 | 0.028322 | 32.000000 | 32 | 0.565705 |
| Distance buffer 4H corrected | 4h | 750 | 0.030000 | 2 | 2 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 29579.993712 | 1.957999 | 0.411356 | 1.179705 | 1.743253 | -0.235970 | 0.221159 | 0.176927 | 0.044232 | 50.000000 | 50 | 0.551719 |
| Distance buffer 4H corrected | 4h | 750 | 0.025000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 27584.116053 | 1.758412 | 0.380377 | 1.110403 | 1.736305 | -0.219073 | 0.160178 | 0.128143 | 0.032036 | 36.000000 | 36 | 0.566725 |
| Distance buffer 4H corrected | 4h | 800 | 0.035000 | 1 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 29372.678163 | 1.937268 | 0.408205 | 1.177067 | 1.720544 | -0.237254 | 0.285940 | 0.228752 | 0.057188 | 52.000000 | 52 | 0.544726 |
| Distance buffer 4H corrected | 4h | 700 | 0.025000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 29087.133906 | 1.908713 | 0.403842 | 1.161983 | 1.701273 | -0.237376 | 0.190449 | 0.152360 | 0.038090 | 41.000000 | 41 | 0.560023 |
| Distance buffer 4H corrected | 4h | 700 | 0.035000 | 2 | 2 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 27515.977996 | 1.751598 | 0.379293 | 1.120087 | 1.695479 | -0.223708 | 0.209222 | 0.167377 | 0.041844 | 49.000000 | 49 | 0.526078 |
| Distance buffer 4H corrected | 4h | 700 | 0.025000 | 2 | 2 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 29271.799978 | 1.927180 | 0.406667 | 1.168344 | 1.670554 | -0.243433 | 0.250194 | 0.200155 | 0.050039 | 53.000000 | 53 | 0.558566 |
| Distance buffer 4H corrected | 4h | 800 | 0.035000 | 2 | 2 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 27181.349106 | 1.718135 | 0.373941 | 1.107869 | 1.651984 | -0.226359 | 0.167935 | 0.134348 | 0.033587 | 34.000000 | 34 | 0.544872 |
| Distance buffer 4H corrected | 4h | 800 | 0.025000 | 2 | 2 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 27894.464724 | 1.789446 | 0.385292 | 1.123038 | 1.556251 | -0.247577 | 0.282379 | 0.225904 | 0.056476 | 56.000000 | 56 | 0.566434 |
| Distance buffer 4H corrected | 4h | 750 | 0.025000 | 2 | 2 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 26761.512803 | 1.676151 | 0.367164 | 1.081960 | 1.549713 | -0.236924 | 0.228695 | 0.182956 | 0.045739 | 48.000000 | 48 | 0.566579 |
| Distance buffer 4H corrected | 4h | 700 | 0.025000 | 1 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 25939.456974 | 1.593946 | 0.353679 | 1.057334 | 1.503761 | -0.235196 | 0.471471 | 0.377177 | 0.094294 | 103.000000 | 103 | 0.557984 |
| Distance buffer 4H corrected | 4h | 700 | 0.030000 | 2 | 2 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 27136.779403 | 1.713678 | 0.373225 | 1.104198 | 1.468010 | -0.254239 | 0.235349 | 0.188279 | 0.047070 | 57.000000 | 57 | 0.541230 |
| Distance buffer 4H corrected | 4h | 700 | 0.030000 | 1 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 26242.536210 | 1.624254 | 0.358684 | 1.073155 | 1.386184 | -0.258756 | 0.450479 | 0.360383 | 0.090096 | 107.000000 | 107 | 0.542104 |
| Distance buffer 4H corrected | 4h | 750 | 0.030000 | 1 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 25835.135858 | 1.583514 | 0.351947 | 1.053880 | 1.337636 | -0.263111 | 0.387010 | 0.309608 | 0.077402 | 94.000000 | 94 | 0.552302 |
| Distance buffer 4H corrected | 4h | 750 | 0.025000 | 1 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 25273.782032 | 1.527378 | 0.342544 | 1.029193 | 1.299539 | -0.263589 | 0.439132 | 0.351305 | 0.087826 | 94.000000 | 94 | 0.566142 |
| Distance buffer 4H corrected | 4h | 700 | 0.035000 | 1 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 25455.299411 | 1.545530 | 0.345600 | 1.047758 | 1.217931 | -0.283760 | 0.352749 | 0.282199 | 0.070550 | 91.000000 | 91 | 0.526661 |
| Distance buffer 4H corrected | 4h | 800 | 0.030000 | 1 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 26178.236977 | 1.617824 | 0.357626 | 1.065475 | 1.201733 | -0.297591 | 0.396463 | 0.317170 | 0.079293 | 74.000000 | 74 | 0.554633 |
| Distance buffer 4H corrected | 4h | 800 | 0.025000 | 1 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 24245.234193 | 1.424523 | 0.324940 | 0.992940 | 1.116899 | -0.290930 | 0.456271 | 0.365017 | 0.091254 | 96.000000 | 96 | 0.566434 |

## Ranking 4H Stress

| name | timeframe | sma_window | buffer | entry_persistence | exit_persistence | confirmation_price | start | end | final_equity | total_return | cagr | sharpe | calmar | max_drawdown | fee_drag | fee_only_drag | slippage_drag | turnover | rebalances | avg_exposure |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Distance buffer 4H corrected | 4h | 750 | 0.035000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 30723.450886 | 2.072345 | 0.428465 | 1.216708 | 2.005430 | -0.213652 | 0.140298 | 0.106287 | 0.034012 | 26.000000 | 26 | 0.542395 |
| Distance buffer 4H corrected | 4h | 800 | 0.030000 | 2 | 2 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 29878.987387 | 1.987899 | 0.415872 | 1.189777 | 1.847040 | -0.225156 | 0.251066 | 0.190201 | 0.060864 | 36.000000 | 36 | 0.553322 |
| Distance buffer 4H corrected | 4h | 800 | 0.035000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 27796.671662 | 1.779667 | 0.383748 | 1.128461 | 1.757749 | -0.218318 | 0.187358 | 0.141938 | 0.045420 | 30.000000 | 30 | 0.544289 |
| Distance buffer 4H corrected | 4h | 800 | 0.025000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 29379.458441 | 1.937946 | 0.408309 | 1.170824 | 1.692636 | -0.241227 | 0.183993 | 0.139388 | 0.044604 | 32.000000 | 32 | 0.565705 |
| Distance buffer 4H corrected | 4h | 750 | 0.030000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 28491.521878 | 1.849152 | 0.394644 | 1.142800 | 1.686857 | -0.233952 | 0.181534 | 0.137526 | 0.044008 | 32.000000 | 32 | 0.552739 |
| Distance buffer 4H corrected | 4h | 800 | 0.030000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 30273.915591 | 2.027392 | 0.421791 | 1.200279 | 1.668881 | -0.252739 | 0.211445 | 0.160185 | 0.051259 | 30.000000 | 30 | 0.553176 |
| Distance buffer 4H corrected | 4h | 700 | 0.035000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 27705.270798 | 1.770527 | 0.382300 | 1.126249 | 1.651237 | -0.231524 | 0.184718 | 0.139938 | 0.044780 | 33.000000 | 33 | 0.525495 |
| Distance buffer 4H corrected | 4h | 750 | 0.035000 | 2 | 2 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 28586.262035 | 1.858626 | 0.396116 | 1.150231 | 1.642539 | -0.241161 | 0.204478 | 0.154907 | 0.049570 | 38.000000 | 38 | 0.542104 |
| Distance buffer 4H corrected | 4h | 700 | 0.030000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 28363.323522 | 1.836332 | 0.392648 | 1.144992 | 1.628502 | -0.241110 | 0.220108 | 0.166748 | 0.053359 | 39.000000 | 39 | 0.541230 |
| Distance buffer 4H corrected | 4h | 750 | 0.025000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 26798.779281 | 1.679878 | 0.367768 | 1.083564 | 1.619186 | -0.227131 | 0.207758 | 0.157392 | 0.050365 | 36.000000 | 36 | 0.566725 |

## Best 4H vs 1D vs Buy & Hold Base

| name | timeframe | sma_window | buffer | entry_persistence | exit_persistence | confirmation_price | start | end | final_equity | total_return | cagr | sharpe | calmar | max_drawdown | fee_drag | fee_only_drag | slippage_drag | turnover | rebalances | avg_exposure |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Distance buffer 4H corrected | 4h | 750 | 0.035000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 31371.088867 | 2.137109 | 0.437963 | 1.236265 | 2.106807 | -0.207880 | 0.107655 | 0.086124 | 0.021531 | 26.000000 | 26 | 0.542395 |
| Distance buffer 1D official | 1d | 125 | 0.030000 | 1 | 1 | close | 2023-02-24 00:00:00+00:00 | 2026-04-18 00:00:00+00:00 | 26819.652697 | 1.681965 | 0.368355 | 1.099686 | 1.740019 | -0.211696 | 0.152831 | 0.122265 | 0.030566 | 35.000000 | 35 | 0.554395 |
| Buy & Hold BTC | 4h | 1 | 0.000000 | 1 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 28656.756039 | 1.865676 | 0.397209 | 0.954904 | 0.793127 | -0.500814 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0 | 1.000000 |

## Best 4H vs 1D vs Buy & Hold Stress

| name | timeframe | sma_window | buffer | entry_persistence | exit_persistence | confirmation_price | start | end | final_equity | total_return | cagr | sharpe | calmar | max_drawdown | fee_drag | fee_only_drag | slippage_drag | turnover | rebalances | avg_exposure |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Distance buffer 4H corrected | 4h | 750 | 0.035000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 30723.450886 | 2.072345 | 0.428465 | 1.216708 | 2.005430 | -0.213652 | 0.140298 | 0.106287 | 0.034012 | 26.000000 | 26 | 0.542395 |
| Distance buffer 1D official | 1d | 125 | 0.030000 | 1 | 1 | close | 2023-02-24 00:00:00+00:00 | 2026-04-18 00:00:00+00:00 | 26076.994548 | 1.607699 | 0.356195 | 1.072812 | 1.638672 | -0.217368 | 0.198343 | 0.150260 | 0.048083 | 35.000000 | 35 | 0.554395 |
| Buy & Hold BTC | 4h | 1 | 0.000000 | 1 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 28656.756039 | 1.865676 | 0.397209 | 0.954904 | 0.793127 | -0.500814 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0 | 1.000000 |

## Rolling Robustness

| window | start | end | winner_by_calmar | both_distance_buffers_negative | 4h_total_return | 1d_total_return | buy_hold_total_return | 4h_cagr | 1d_cagr | 4h_calmar | 1d_calmar | 4h_max_drawdown | 1d_max_drawdown | 4h_rebalances | 1d_rebalances | 4h_fee_drag | 1d_fee_drag |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| rolling_01 | 2023-02-23 | 2024-02-23 | 1D | False | 0.676250 | 0.706038 | 1.050200 | 0.676844 | 0.706662 | 3.284931 | 4.420697 | -0.206045 | -0.159853 | 15 | 18 | 0.036542 | 0.044344 |
| rolling_02 | 2023-08-24 | 2024-08-23 | 1D | False | 1.103802 | 1.128671 | 1.239162 | 1.104874 | 1.129773 | 5.314955 | 6.099895 | -0.207880 | -0.185212 | 8 | 6 | 0.034683 | 0.029970 |
| rolling_03 | 2024-02-22 | 2025-02-21 | 4H | False | 0.882226 | 0.732167 | 0.960918 | 0.883041 | 0.732818 | 4.247837 | 3.956651 | -0.207880 | -0.185212 | 6 | 8 | 0.018771 | 0.024467 |
| rolling_04 | 2024-08-22 | 2025-08-22 | 4H | False | 0.588690 | 0.414626 | 0.792062 | 0.589194 | 0.414962 | 3.030507 | 1.970041 | -0.194421 | -0.210636 | 3 | 5 | 0.009965 | 0.013847 |
| rolling_05 | 2025-02-20 | 2026-02-20 | 4H | True | -0.010571 | -0.141131 | -0.384447 | -0.010579 | -0.141221 | -0.080000 | -0.864127 | -0.132234 | -0.163426 | 5 | 9 | 0.012408 | 0.020482 |

## Expanding Robustness

| window | start | end | winner_by_calmar | 4h_cagr | 1d_cagr | 4h_calmar | 1d_calmar | 4h_max_drawdown | 1d_max_drawdown | 4h_rebalances | 1d_rebalances |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| expanding_01 | 2023-02-23 | 2024-02-23 | 1D | 0.678828 | 0.755382 | 3.294563 | 4.912155 | -0.206045 | -0.153778 | 15 | 18 |
| expanding_02 | 2023-02-23 | 2024-08-23 | 1D | 0.618771 | 0.665950 | 2.976576 | 3.595615 | -0.207880 | -0.185212 | 20 | 23 |
| expanding_03 | 2023-02-23 | 2025-02-21 | 1D | 0.788148 | 0.754940 | 3.791354 | 4.076089 | -0.207880 | -0.185212 | 21 | 26 |
| expanding_04 | 2023-02-23 | 2025-08-22 | 4H | 0.607695 | 0.561208 | 2.923295 | 2.664349 | -0.207880 | -0.210636 | 23 | 28 |
| expanding_05 | 2023-02-23 | 2026-02-20 | 4H | 0.465726 | 0.390939 | 2.240359 | 1.846700 | -0.207880 | -0.211696 | 26 | 35 |

## Block Bootstrap

| name | timeframe | sharpe_p05 | sharpe_median | sharpe_p95 | calmar_p05 | calmar_median | calmar_p95 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Distance buffer 4H corrected | 4h | 0.421994 | 1.289937 | 2.192811 | 0.268720 | 1.713897 | 4.910550 |
| Distance buffer 1D official | 1d | 0.183033 | 1.121125 | 2.107073 | 0.028567 | 1.335980 | 4.689289 |

## Pro Layer Test

| name | timeframe | sma_window | buffer | entry_persistence | exit_persistence | confirmation_price | start | end | final_equity | total_return | cagr | sharpe | calmar | max_drawdown | fee_drag | fee_only_drag | slippage_drag | turnover | rebalances | avg_exposure | layer |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Distance buffer 4H corrected | 4h | 750 | 0.035000 | 3 | 3 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 31371.088867 | 2.137109 | 0.437963 | 1.236265 | 2.106807 | -0.207880 | 0.107655 | 0.086124 | 0.021531 | 26.000000 | 26 | 0.542395 | base |
| Distance buffer 4H corrected pro asymmetric_exit | 4h | 750 | 0.035000 | 3 | 1 | close | 2023-02-23 20:00:00+00:00 | 2026-04-18 12:00:00+00:00 | 28381.945254 | 1.838195 | 0.392938 | 1.147973 | 1.826696 | -0.215109 | 0.218301 | 0.174641 | 0.043660 | 46.000000 | 46 | 0.532197 | pro_asymmetric_exit_p1 |

Pro layer discarded: asymmetric exit persistence p1 did not improve CAGR, Calmar, drawdown and trade profile together.

## Trade Forensics 4H

- complete_trades: 13
- trades_per_year: 4.130110
- win_rate: 0.384615
- loss_rate: 0.615385
- profit_factor: 6.101964
- expectancy_per_trade: 0.132558
- mean_return_per_trade: 0.132558
- median_return_per_trade: -0.016461
- std_return_per_trade: 0.373932
- winner_mean_holding_days: 116.633333
- loser_mean_holding_days: 4.666667
- longest_winning_streak: 2
- longest_losing_streak: 2
- best_trade: trade 8 net_return=1.248431, holding_days=243.00
- worst_trade: trade 1 net_return=-0.171485, holding_days=14.33
- top_1_concentration: net_sum=1.248431, share=0.724462
- top_3_concentration: net_sum=2.003508, share=1.162631
- top_5_concentration: net_sum=2.061016, share=1.196003
- top_10_concentration: net_sum=1.971707, share=1.144177

## Trade Forensics 1D

- complete_trades: 17
- trades_per_year: 5.400913
- win_rate: 0.352941
- loss_rate: 0.647059
- profit_factor: 8.678235
- expectancy_per_trade: 0.095960
- mean_return_per_trade: 0.095960
- median_return_per_trade: -0.004331
- std_return_per_trade: 0.311668
- winner_mean_holding_days: 95.500000
- loser_mean_holding_days: 4.454545
- longest_winning_streak: 2
- longest_losing_streak: 3
- best_trade: trade 23 net_return=1.238547, holding_days=246.00
- worst_trade: trade 31 net_return=-0.062694, holding_days=9.00
- top_1_concentration: net_sum=1.238547, share=0.759226
- top_3_concentration: net_sum=1.790965, share=1.097857
- top_5_concentration: net_sum=1.839050, share=1.127333
- top_10_concentration: net_sum=1.825209, share=1.118848

## Psychological Read

- 4H is not materially harder than 1D by trade count.
- 4H rebalances/common period: 26 vs 1D 35; ratio 0.74.
- Complete-trade frequency ratio 4H/1D: 0.76.
- Manual or semi-manual paper trading is only reasonable with disciplined 4H close checks.

## Practical 2025 Simulation

| name | timeframe | final_value | invested_capital | return_on_contributions | time_weighted_nav_drawdown | invested_capital_drawdown | fee_drag |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Distance buffer 4H corrected | 4h | 6496.840882 | 6500.000000 | -0.000486 | -0.182745 | -0.054758 | 0.007425 |
| Distance buffer 1D official | 1d | 6085.256633 | 6500.000000 | -0.063807 | -0.142441 | -0.075408 | 0.014660 |
| Buy & Hold BTC | 4h | 6322.424244 | 6500.000000 | -0.027319 | -0.227584 | -0.106619 | 0.000385 |

## Verdict

**La 4H corregida supera de forma defendible a la 1D y merece paper trading paralelo**

Razon principal: mejora CAGR y Calmar en el periodo comun y gana mas ventanas temporales.
