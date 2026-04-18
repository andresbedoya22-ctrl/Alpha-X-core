# ALPHA-X CORE

Base reproducible para la etapa V1 del proyecto ALPHA-X CORE.

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
  labeling/
  strategies/
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
