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
```

## Estructura

```text
src/alpha_x/
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

CSV generado:

```text
data/raw/bitvavo/btc-eur_1h.csv
```
