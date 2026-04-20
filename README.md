# ALPHA-X CORE

## Estado actual

El roadmap vigente cambio. El foco principal del repositorio ya no es el track previo de labeling/modeling/policies.

La etapa activa es **Fase 1 - Truth Engine**:

- Exchange base: `Bitvavo`
- Universo base: `crypto-only`
- Sesgo estructural: `long-only`
- Timeframe principal de investigacion: `1D`
- Frecuencia natural de decision: `weekly review`
- Objetivo: descubrir si existe edge economico real, robusto y reproducible antes de volver a modelado mas complejo

## Flujo principal actual

1. Ingerir o validar OHLCV diario de Bitvavo para el universo oficial.
2. Evaluar elegibilidad minima por activo.
3. Construir senales sobrias y regimen BTC diario.
4. Comparar familias oficiales del Truth Engine contra benchmarks con fees y slippage explicitos.
5. Exportar resultados reproducibles a `reports/truth_engine/<run_id>/`.

## Modulos y scripts vigentes para Fase 1

### Capa nueva

- `src/alpha_x/truth_engine/universe.py`
- `src/alpha_x/truth_engine/eligibility.py`
- `src/alpha_x/truth_engine/signals.py`
- `src/alpha_x/truth_engine/scoring.py`
- `src/alpha_x/truth_engine/regimes.py`
- `src/alpha_x/truth_engine/weighting.py`
- `src/alpha_x/truth_engine/rebalance.py`
- `src/alpha_x/truth_engine/families.py`
- `src/alpha_x/truth_engine/comparison.py`
- `src/alpha_x/truth_engine/metrics.py`
- `src/alpha_x/truth_engine/reporting.py`

### Scripts principales

- `python .\scripts\run_truth_engine.py`
- `python .\scripts\run_truth_robustness.py`
- `python .\scripts\fetch_truth_engine_1d.py`

## Datos 1D del universo oficial

Poblar OHLCV diario del universo oficial en `data/raw/bitvavo/`:

```powershell
.\.venv\Scripts\python.exe .\scripts\fetch_truth_engine_1d.py --run-id truth-data --target-rows 2500
```

Validar cobertura sin descargar de nuevo:

```powershell
.\.venv\Scripts\python.exe .\scripts\fetch_truth_engine_1d.py --run-id truth-data-validate --validate-only
```

Verificar cobertura en:

- `reports/truth_engine_data/<run_id>/summary.json`
- `reports/truth_engine_data/<run_id>/asset_coverage.csv`

Luego correr Truth Engine:

```powershell
.\.venv\Scripts\python.exe .\scripts\run_truth_engine.py --run-id truth-base --cost-scenario base
```

## Costes activos en Fase 1

- Escenario `base`: fee `0.25%` por lado + slippage configurable
- Escenario `mid`: mezcla maker/taker conservadora con fee medio menor
- Escenario `stress`: fee y slippage mas altos
- No se aceptan resultados sin costes

## Benchmarks obligatorios activos

- Buy & Hold BTC
- DCA BTC
- SMA baseline reutilizable
- Equal-weight basket del universo con rebalanceo trimestral
- BTC/ETH `60/40` con rebalanceo trimestral

## Legacy / Previous Research Track

## Alcance de V2 / F2.1

- Capa inicial de hipotesis simples long/flat sobre BTC-EUR 1h
- Familias base: trend following, momentum, breakout, mean reversion y volatility filter
- Senales 0/1 reproducibles y sin look-ahead adicional al motor actual
- Runner `scripts/run_hypotheses.py` para ejecutar y comparar hipotesis contra benchmarks
- Export reproducible a `reports/hypotheses/<run_id>/`

## Alcance de V2 / F2.2

- Capa base de labeling auditable para eventos operables sobre BTC-EUR 1h
- Tres enfoques: next-bar, fixed horizon y triple barrier
- Manejo explicito de gaps reales y bordes del dataset como filas descartadas
- Runner `scripts/run_labeling.py` para comparar distribuciones de labels y parametros
- Export reproducible a `reports/labeling/<run_id>/`

## Alcance de V2 / F2.3

- Capa base de validacion temporal robusta para hipotesis y benchmark minimo
- Split temporal `train/validation/test` sin shuffle ni leakage
- Walk-forward expanding para revisar estabilidad OOS
- Sensibilidad local de parametros para pocas hipotesis candidatas
- Export reproducible a `reports/validation/<run_id>/`

## Alcance de V2 / F2.4

- Capa corta de refinamientos operativos para reducir ruido y churn antes de V3
- Resample explicito y auditable de 1h a 4h
- Reglas simples: minimum holding, cooldown y confirmacion minima
- Comparacion 1h vs 4h y baseline vs refinado con foco en OOS y churn
- Export reproducible a `reports/refinements/<run_id>/`

## Alcance de V3 / F3.1

- Feature engine multi-horizonte corto, medio y largo sobre BTC-EUR
- Catalogo oficial y auditable de features con definicion, parametros y warmup explicitos
- Familias iniciales: returns, trend, volatility, compression y price structure
- Integracion conservadora con el dataset OHLCV actual sin romper labeling ni backtest
- Runner `scripts/run_feature_engine.py` con export reproducible a `reports/features/<run_id>/`

## Alcance de V3 / F3.2a

- Detector simple de regimen basado en reglas explicitas y reproducibles
- Taxonomia corta de 6 regimenes por tendencia y volatilidad
- `compression_state` exportado como contexto auxiliar para no inflar categorias
- Cruce de regimen con triple barrier labels, Hypothesis 5 y SMA baseline
- Runner `scripts/run_regime_analysis.py` con export reproducible a `reports/regime/<run_id>/`

## Alcance de V3 / F3.3

- Dataset supervisado auditable con features, labels triple barrier y regimen
- Primera formulacion binaria sobria: `triple_barrier == +1` frente a `no positivo`
- Modelos base: Logistic Regression regularizada y Random Forest pequeno
- Validacion temporal estricta `train/validation/test` sin shuffle
- Traduccion operacional minima a senal long/flat con backtest reproducible en `test`

## Alcance de V3 / F3.4

- Decision policies sobrias sobre scores del mejor modelo base de F3.3
- Cuatro variantes fijas de alta conviccion y filtro opcional por regimen
- Evaluacion OOS solo en el mismo periodo `test`
- Comparacion contra Hypothesis 5, SMA baseline y Buy & Hold
- Export reproducible a `reports/model_policies/<run_id>/`

## Alcance de V4 / F4 — Multi-Asset + External Data Layer

Pivote estructural tras V3: no hay edge robusto demostrado en BTC/EUR con features
tecnicas puras. Esta fase prepara la base para investigacion real multi-activo y
enriquecida con datos contextuales externos.

**Objetivo:** Construir un entorno serio, auditable y honesto de investigacion
comparativa sin demostrar edge todavia.

### Activos oficiales

- BTC-EUR, ETH-EUR, XRP-EUR, SOL-EUR (exchange: Bitvavo, EUR spot)

### Modulos nuevos

- `src/alpha_x/multi_asset/` — Registro oficial de mercados, carga multi-activo,
  ventana comun y reporte de paridad de profundidad.
- `src/alpha_x/external_data/` — Funding rates (Binance FAPI, gratuito, sin auth) y
  proxy de ETF (Yahoo Finance, precio/volumen de IBIT/ETHA como señal institucional).
  Capa de alineacion temporal sin leakage con politica de forward-fill explicita y limitada.

### Scripts nuevos

- `scripts/fetch_multi_asset_ohlcv.py` — Backfill para los 4 activos.
- `scripts/fetch_external_context.py` — Descarga funding rates y proxy ETF.
- `scripts/run_multi_asset_data_audit.py` — Auditoria reproducible con salida
  en `reports/multi_asset_data/<run_id>/` (summary.json + asset_coverage.csv).

### Decisiones de diseno documentadas

- Funding rates: señal global de mercado de derivados, no del par EUR spot.
- ETF proxy: precio/volumen de IBIT/ETHA, NO flujos netos reales (requieren API key).
- XRP y SOL no tienen spot ETF; usan IBIT como contexto institucional global.
- Alineacion temporal: as-of backward join (sin leakage). Forward-fill limitado a
  8 bars para funding (1 ciclo de 8h) y 24 bars para ETF proxy (1 dia).
- Filas sin contexto externo quedan como NaN, no se descartan automaticamente.
- La ventana comun multi-activo se reporta explicitamente como referencia para
  analisis comparativos futuros.

### Limitaciones conocidas (documentadas en summary.json)

- ETH, XRP y SOL pueden tener menos historia OHLCV que BTC.
- ETF proxy cubre solo desde Jan 2024 (BTC) y Jul 2024 (ETH).
- Comparaciones cross-activo solo son validas dentro de la ventana OHLCV comun.
- Funding history de XRP/SOL empieza ~2021, mas corta que BTC/ETH.

## V4 / F4 notas actualizadas

- Funding rates: Bybit V5 linear perps, usados como contexto global de derivados crypto.
- ETF flows: BTC spot ETF daily flows desde Bitbo, alineados a 1h desde el siguiente dia UTC.
- ETH ETF flows no se ingieren en esta fase por falta de una fuente libre estable confirmada.
- XRP y SOL reutilizan el BTC ETF flow como contexto institucional crypto global, de forma explicita.
- La auditoria exporta tres ventanas comunes: `OHLCV`, `OHLCV + funding` y
  `OHLCV + funding + ETF flows`.
- `summary.json` incluye `known_limitations` con cobertura, comparabilidad y reglas de fill.

## V4 / F4.1 Comparacion multi-activo

- Reejecucion controlada del pipeline supervisado sobre la ventana comun enriquecida.
- Misma ventana, mismo target, mismos modelos base y misma policy comun para BTC-EUR,
  ETH-EUR, XRP-EUR y SOL-EUR.
- Features tecnicas existentes + funding del activo + ETF flows BTC como contexto global.
- Export reproducible a `reports/multi_asset_comparison/<run_id>/`.

## Alcance de V3 / F3.5

- Stress test minimo de la policy condicional hallada en F3.4
- Misma base supervisada, mismo modelo, mismo split y mismo regimen
- Stress local solo sobre `trend_up_high_vol` con umbrales `0.60`, `0.65` y `0.70`
- Segmentacion del `test` en 3 subtramos temporales ordenados
- Export reproducible a `reports/policy_stress/<run_id>/`

## Alcance de F1.1

- Estructura profesional de proyecto Python con `src/`
- Configuracion centralizada con `pydantic-settings`
- Logging basico a consola y archivo
- Healthcheck local
- Testing inicial con `pytest`
- Linting con `ruff`

## Alcance de F1.2

- Cliente REST minimo para OHLCV historico desde Bitvavo
- Pipeline de descarga, normalizacion, merge, dedupe y persistencia en CSV
- Validacion temporal de orden, unicidad y huecos por timeframe
- Script `scripts/fetch_ohlcv.py` con modo de descarga y modo `--validate-only`
- Backfill historico incremental y reproducible con `--backfill --target-rows`
- Diagnostico y reparacion conservadora de gaps con `--report-gaps` y `--repair-gaps`

## Alcance de F1.3

- Benchmark Engine base sobre CSV OHLCV validado
- Benchmark A: Buy & Hold BTC/EUR
- Benchmark B: DCA mensual BTC/EUR
- Benchmark C: baseline cuantitativa simple con SMA crossover long/flat
- Equity curve y metricas comparables minimas
- Script `scripts/run_benchmarks.py` con defaults conservadores, `--fee-bps` preferido y `--fee` por compatibilidad

## Alcance de F1.4

- Motor de backtesting honesto, simple y auditable para estrategias long/flat
- Carga del CSV OHLCV actual con deteccion explicita de gaps residuales
- Ejecucion conservadora: la senal observada en `close[t]` se ejecuta en `close[t+1]`
- Fees configurables y slippage configurable
- Equity curve, trades cerrados y metricas base
- Script `scripts/run_backtest.py` para correr una estrategia de prueba sobre el dataset actual
- Comparacion de la estrategia F1.4 contra benchmarks ya disponibles en F1.3

## Alcance de F1.5

- Capa base de reporting reproducible para benchmarks y backtests
- Export a `reports/` en formatos simples y auditables: JSON y CSV
- `run_id` por corrida para identificar y guardar resultados
- Contexto persistente suficiente para auditar dataset, rows, gaps, fees y parametros
- Scripts `scripts/export_benchmark_report.py` y `scripts/export_backtest_report.py`

## Requisitos

- Python 3.11 o superior
- PowerShell en Windows

## Instalacion rapida

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e .[dev]
Copy-Item .env.example .env
python .\scripts\healthcheck.py
python .\scripts\fetch_ohlcv.py --market BTC-EUR --interval 1h --limit 500
pytest
ruff check .
python .\scripts\run_benchmarks.py
```

## Estructura

```text
src/alpha_x/
  benchmarks/
  features/
  labeling/
  modeling/
  regime/
  refinements/
  strategies/
  validation/
  config/
  data/
  backtest/
  reporting/
  utils/
```

## Fuera de alcance en F1.1

- Logica de trading
- Pipeline de datos
- Backtesting completo
- Docker
- PostgreSQL
- CI
- Notebooks
- CCXT

## F1.2 Uso rapido

Descarga OHLCV inicial:

```powershell
python .\scripts\fetch_ohlcv.py --market BTC-EUR --interval 1h --limit 500
```

Validacion del CSV persistido:

```powershell
python .\scripts\fetch_ohlcv.py --market BTC-EUR --interval 1h --validate-only
```

Backfill historico incremental:

```powershell
python .\scripts\fetch_ohlcv.py --market BTC-EUR --interval 1h --backfill --target-rows 10000
```

Reporte de gaps:

```powershell
python .\scripts\fetch_ohlcv.py --market BTC-EUR --interval 1h --report-gaps
```

Intento conservador de reparacion de gaps:

```powershell
python .\scripts\fetch_ohlcv.py --market BTC-EUR --interval 1h --repair-gaps
```

CSV generado:

```text
data/raw/bitvavo/btc-eur_1h.csv
```

Notas de backfill:

- el script encadena multiples requests REST contra Bitvavo
- cada bloque se fusiona por `timestamp` y no duplica filas existentes
- repetir el comando no hace crecer artificialmente el CSV si no hay filas nuevas
- al final se informa el rango temporal y el `gap count`
- `--repair-gaps` solo reconsulta ventanas faltantes reales; no rellena velas sinteticas

## F1.3 Uso rapido

Ejecutar benchmarks con defaults:

```powershell
python .\scripts\run_benchmarks.py
```

Ejemplo con parametros:

```powershell
python .\scripts\run_benchmarks.py --market BTC-EUR --timeframe 1h --fee-bps 25 --sma-fast 20 --sma-slow 50
```

El script:

- carga `data/raw/bitvavo/btc-eur_1h.csv` por defecto
- valida columnas y coherencia temporal antes de calcular benchmarks
- imprime una tabla comparativa para Buy & Hold, DCA mensual y SMA crossover
- acepta `--fee-bps` como opcion preferida; `25` significa `0.25%`
- mantiene `--fee` por compatibilidad y lo interpreta como tasa decimal; `0.0025` significa `25` bps

## F1.4 Uso rapido

Ejecutar el backtest honesto con la estrategia de prueba:

```powershell
python .\scripts\run_backtest.py
```

Ejemplo con parametros:

```powershell
python .\scripts\run_backtest.py --market BTC-EUR --timeframe 1h --fee-bps 10 --slippage-bps 5 --sma-fast 20 --sma-slow 50
```

El script:

- carga el CSV OHLCV persistido y reporta gaps residuales si existen
- genera una senal long/flat simple basada en SMA crossover
- aplica ejecucion conservadora sin look-ahead
- descuenta fees y slippage en cada cambio de posicion
- imprime metricas netas y una comparativa con benchmarks F1.3

## F1.5 Uso rapido

Exportar benchmarks a `reports/benchmarks/<run_id>/`:

```powershell
python .\scripts\export_benchmark_report.py
```

Exportar backtest a `reports/backtests/<run_id>/`:

```powershell
python .\scripts\export_backtest_report.py
```

Los scripts:

- crean una carpeta de salida por corrida dentro de `reports/`
- guardan `summary.json` y `summary.csv`
- exportan equity curves en CSV
- exportan `trades.csv` y `manifest.json` para backtests
- imprimen la ruta del reporte, archivos creados y un resumen corto de la corrida

## V2 / F2.1 Uso rapido

Ejecutar las hipotesis base:

```powershell
python .\scripts\run_hypotheses.py
```

El script:

- carga el dataset OHLCV persistido actual de BTC-EUR 1h
- genera cinco hipotesis simples y explicitas con senal 0/1
- reutiliza el backtester honesto long/flat existente
- compara cada hipotesis contra Buy & Hold, DCA mensual y SMA baseline
- imprime una tabla final con retorno total, annualized, max drawdown, profit factor, trades, exposure y final equity
- exporta `summary.json`, `summary.csv`, `equity_curves.csv`, `signals.csv` y `manifest.json` en `reports/hypotheses/<run_id>/`

## V2 / F2.2 Uso rapido

Ejecutar labeling base:

```powershell
python .\scripts\run_labeling.py
```

El script:

- carga el dataset OHLCV persistido actual de BTC-EUR 1h
- ejecuta next-bar, fixed horizon y triple barrier con parametros explicitos
- descarta filas cuyo futuro cruza gaps reales o no llega limpio al final del dataset
- imprime por metodo el total etiquetado, distribucion de clases, filas descartadas y rango cubierto
- exporta `summary.json`, `summary.csv`, `labels.csv` y `manifest.json` en `reports/labeling/<run_id>/`

## V2 / F2.3 Uso rapido

Ejecutar validacion robusta:

```powershell
python .\scripts\run_validation.py
```

El script:

- carga el dataset OHLCV persistido actual de BTC-EUR 1h
- valida Benchmark C, Hypothesis 1 y Hypothesis 5 con split temporal y walk-forward
- usa fees y slippage agresivos por defecto para no suavizar OOS
- corre una sensibilidad local de parametros sobre el tramo `test`
- reporta gaps residuales por tramo y agrega resultados OOS sin mezclar tiempos
- exporta `summary.json`, `validation_rows.csv`, `oos_aggregate.csv` y `manifest.json` en `reports/validation/<run_id>/`

## V2 / F2.4 Uso rapido

Ejecutar refinamientos operativos:

```powershell
python .\scripts\run_refinements.py
```

El script:

- deriva 4h de forma explicita y auditable a partir del dataset 1h
- prueba pocas variantes defendibles sobre Volatility Filter y un control SMA 4h
- aplica minimum holding, cooldown y una confirmacion minima solo donde corresponde
- compara resultados OOS agregados y deltas de churn frente a sus baselines
- exporta `summary.json`, `validation_rows.csv`, `oos_summary.csv`, `comparisons.csv` y `manifest.json` en `reports/refinements/<run_id>/`

## V3 / F3.1 Uso rapido

Ejecutar el feature engine:

```powershell
python .\scripts\run_feature_engine.py
```

Ejemplo uniendo labels de triple barrier:

```powershell
python .\scripts\run_feature_engine.py --join-triple-barrier-labels
```

El script:

- carga el dataset OHLCV persistido actual de BTC-EUR 1h
- genera un set oficial y corto de features multi-horizonte con warmup visible
- conserva `timestamp`, `datetime` y OHLCV base en la tabla final
- marca `valid_feature_row` solo cuando todas las features oficiales ya son validas
- puede unir de forma opcional los labels de triple barrier ya existentes
- exporta `summary.json`, `feature_table.csv`, `feature_catalog.csv` y `manifest.json` en `reports/features/<run_id>/`

## V3 / F3.2a Uso rapido

Ejecutar el analisis de regimen:

```powershell
python .\scripts\run_regime_analysis.py
```

El script:

- reutiliza el feature engine y el labeling triple barrier ya disponibles
- asigna un regimen simple por fila valida con reglas visibles en codigo
- usa `dist_sma_72` y `sma_24_slope_4` para tendencia
- usa `atr_pct_24` frente a su mediana rolling de 168 barras para volatilidad relativa
- exporta `compression_state` a partir de `range_pct_24_rank_72` como contexto auxiliar
- cruza regimen con labels y con dos referencias simples: Hypothesis 5 y SMA baseline
- exporta `summary.json`, `regime_table.csv`, `regime_summary.csv`, `regime_label_table.csv`, `regime_strategy_table.csv` y `manifest.json` en `reports/regime/<run_id>/`

## V3 / F3.3 Uso rapido

Ejecutar los modelos base:

```powershell
python .\scripts\run_model_baselines.py
```

El script:

- construye un dataset supervisado con las 24 features, triple barrier y regimen
- usa target binario inicial: `1` cuando triple barrier es `+1`, `0` en otro caso
- descarta filas invalidas de forma explicita por warmup, regimen o label no disponible
- entrena dos modelos base con split temporal `train/validation/test`
- selecciona el mejor modelo por `balanced_accuracy` en validation con `macro_f1` como desempate
- convierte el mejor baseline supervisado a una senal long/flat por umbral de probabilidad
- compara ese backtest OOS contra Hypothesis 5 y SMA baseline
- exporta `summary.json`, `supervised_dataset.csv`, `model_metrics.csv`, `regime_metrics.csv`, `backtest_comparison.csv` y `manifest.json` en `reports/modeling/<run_id>/`

## V3 / F3.4 Uso rapido

Ejecutar las decision policies:

```powershell
python .\scripts\run_model_policies.py
```

El script:

- reutiliza el mismo dataset supervisado y el mejor modelo base reproducible de F3.3
- reentrena el modelo con `train + validation` y scorea solo el periodo `test`
- ejecuta exactamente cuatro variantes de policy:
  - A: `p > 0.65`
  - B: `p > 0.70`
  - C: `regime == trend_up_high_vol` y `p > 0.60`
  - D: `regime == trend_up_high_vol` y `p > 0.65`
- mantiene el resto del tiempo en `flat`
- compara cada variante contra Hypothesis 5, SMA baseline y Buy & Hold en el mismo tramo `test`
- exporta `summary.json`, `scored_test_frame.csv`, `policy_signals.csv`, `policy_summary.csv`, `backtest_comparison.csv` y `manifest.json` en `reports/model_policies/<run_id>/`

## V3 / F3.5 Uso rapido

Ejecutar el stress test de policy:

```powershell
python .\scripts\run_policy_stress.py
```

El script:

- reutiliza el mismo mejor modelo base reproducible de F3.3/F3.4
- scorea exactamente el mismo periodo `test`
- compara solo tres variantes locales sobre la policy condicional:
  - `trend_up_high_vol` y `p > 0.60`
  - `trend_up_high_vol` y `p > 0.65`
  - `trend_up_high_vol` y `p > 0.70`
- divide el `test` en 3 subtramos temporales ordenados para revisar estabilidad minima
- reporta activacion, trades, exposure, retorno y drawdown por variante y por subtramo
- compara contra Hypothesis 5, la mejor policy global de F3.4 y Buy & Hold
- exporta `summary.json`, `scored_test_frame.csv`, `stress_policy_signals.csv`, `stress_summary.csv`, `subperiod_stability.csv`, `comparison.csv` y `manifest.json` en `reports/policy_stress/<run_id>/`
