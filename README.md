# ALPHA-X CORE

Base reproducible para la etapa V1 del proyecto ALPHA-X CORE.

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
