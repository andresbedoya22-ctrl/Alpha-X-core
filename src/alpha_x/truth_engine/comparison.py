from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from alpha_x.backtest.models import BacktestResult
from alpha_x.benchmarks import BenchmarkResult
from alpha_x.benchmarks.allocations import run_equal_weight_basket, run_fixed_mix_benchmark
from alpha_x.benchmarks.buy_and_hold import run_buy_and_hold
from alpha_x.benchmarks.dca import run_monthly_dca
from alpha_x.benchmarks.sma_baseline import run_sma_baseline
from alpha_x.data.ohlcv_storage import build_ohlcv_csv_path, load_ohlcv_csv
from alpha_x.data.truth_engine_data import load_latest_truth_engine_data_summary
from alpha_x.reporting.io import build_run_id
from alpha_x.truth_engine.eligibility import EligibilityConfig, build_eligibility_table
from alpha_x.truth_engine.families import OFFICIAL_FAMILIES, FamilyDefinition
from alpha_x.truth_engine.metrics import TruthMetrics, calculate_truth_metrics
from alpha_x.truth_engine.rebalance import PortfolioSimulationResult, simulate_family_portfolio
from alpha_x.truth_engine.regimes import RegimeConfig, build_regime_frame
from alpha_x.truth_engine.reporting import export_truth_engine_report, metrics_to_frame
from alpha_x.truth_engine.scoring import build_family_scores
from alpha_x.truth_engine.signals import SignalConfig, build_signal_frame
from alpha_x.truth_engine.universe import OFFICIAL_UNIVERSE
from alpha_x.validation.splits import build_temporal_splits, slice_frame_for_split


@dataclass(frozen=True)
class TruthEngineConfig:
    timeframe: str = "1d"
    initial_capital: float = 10_000.0
    fee_rate: float = 0.0025
    slippage_rate: float = 0.0005
    dca_amount: float = 250.0
    sma_fast: int = 50
    sma_slow: int = 200
    min_required_assets: int = 2


@dataclass(frozen=True)
class TruthEngineRunResult:
    run_id: str
    config: TruthEngineConfig
    report_dir: Path | None
    summary_frame: pd.DataFrame
    comparison_frame: pd.DataFrame
    split_frame: pd.DataFrame
    eligibility_frame: pd.DataFrame
    family_metrics: dict[str, TruthMetrics]
    benchmark_metrics: dict[str, TruthMetrics]
    skipped_markets: list[str]
    eligible_markets: list[str]
    family_results: dict[str, PortfolioSimulationResult]
    benchmark_results: dict[str, BenchmarkResult | BacktestResult]
    manifest: dict[str, Any]


def run_truth_engine(
    *,
    raw_data_dir: Path,
    reports_dir: Path,
    run_id: str | None = None,
    created_at: pd.Timestamp | None = None,
    universe: tuple[str, ...] | list[str] = OFFICIAL_UNIVERSE,
    config: TruthEngineConfig | None = None,
    export_report: bool = True,
) -> TruthEngineRunResult:
    cfg = config or TruthEngineConfig()
    created = created_at or pd.Timestamp.now(tz="UTC").floor("s")
    resolved_run_id = run_id or build_run_id(created)
    resolved_universe, data_summary = _resolve_input_universe(
        reports_dir=reports_dir,
        requested_universe=list(universe),
        timeframe=cfg.timeframe,
    )
    _validate_required_csvs(
        raw_data_dir=raw_data_dir,
        timeframe=cfg.timeframe,
        markets=resolved_universe,
        used_data_summary=bool(data_summary and data_summary.get("effective_universe_final")),
    )

    market_frames = _load_market_frames(
        raw_data_dir=raw_data_dir, timeframe=cfg.timeframe, markets=resolved_universe
    )
    eligibility_config = EligibilityConfig(timeframe=cfg.timeframe)
    eligibility_frame = build_eligibility_table(market_frames, eligibility_config)
    eligible_markets = eligibility_frame.loc[eligibility_frame["eligible"], "market"].tolist()
    skipped_markets = eligibility_frame.loc[~eligibility_frame["eligible"], "market"].tolist()
    if len(eligible_markets) < cfg.min_required_assets:
        raise ValueError(
            "Truth Engine requires at least "
            f"{cfg.min_required_assets} eligible assets; found {len(eligible_markets)}."
        )
    if "BTC-EUR" not in eligible_markets:
        raise ValueError("BTC-EUR must remain eligible to build the operating regime.")

    aligned_frames = _align_market_frames(
        {market: market_frames[market] for market in eligible_markets}
    )
    signal_panel = pd.concat(
        [
            build_signal_frame(market, frame, SignalConfig())
            for market, frame in aligned_frames.items()
        ],
        ignore_index=True,
    )
    regime_frame = build_regime_frame(aligned_frames["BTC-EUR"], RegimeConfig())
    score_panel = build_family_scores(signal_panel, regime_frame)
    return_frame = _build_return_frame(aligned_frames)

    family_results: dict[str, PortfolioSimulationResult] = {}
    family_metrics: dict[str, TruthMetrics] = {}
    for family in OFFICIAL_FAMILIES:
        rebalance = family.rebalance
        rebalance = type(rebalance)(
            review_weekday=rebalance.review_weekday,
            min_net_advantage=rebalance.min_net_advantage,
            no_trade_buffer=rebalance.no_trade_buffer,
            turnover_cap=rebalance.turnover_cap,
            fee_rate=cfg.fee_rate,
            slippage_rate=cfg.slippage_rate,
        )
        simulation = simulate_family_portfolio(
            score_panel=score_panel,
            return_frame=return_frame,
            score_column=family.score_column,
            family_name=family.name,
            weighting_config=family.weighting,
            rebalance_config=rebalance,
        )
        family_results[family.family_id] = simulation
        family_metrics[family.family_id] = calculate_truth_metrics(
            simulation.equity_curve,
            name=family.name,
            source_type="family",
            rebalance_count=int(simulation.metadata["rebalance_count"]),
            trade_count=int(simulation.metadata["trade_count"]),
        )

    benchmark_results = _run_benchmarks(aligned_frames, cfg)
    benchmark_metrics = _build_benchmark_metrics(benchmark_results)

    summary_frame = metrics_to_frame(
        list(family_metrics.values()) + list(benchmark_metrics.values())
    )
    comparison_frame = summary_frame.sort_values(
        ["source_type", "sharpe", "cagr"], ascending=[True, False, False]
    )
    split_frame = _build_split_frame(
        score_panel=score_panel,
        return_frame=return_frame,
        family_definitions=list(OFFICIAL_FAMILIES),
        fee_rate=cfg.fee_rate,
        slippage_rate=cfg.slippage_rate,
    )

    manifest = {
        "run_id": resolved_run_id,
        "timeframe": cfg.timeframe,
        "universe_requested": list(universe),
        "universe_resolved": resolved_universe,
        "eligible_markets": eligible_markets,
        "skipped_markets": skipped_markets,
        "data_batch_run_id": data_summary.get("run_id") if data_summary else None,
        "costs": {
            "fee_rate": cfg.fee_rate,
            "slippage_rate": cfg.slippage_rate,
            "one_way_total": cfg.fee_rate + cfg.slippage_rate,
            "round_trip_fee_only": cfg.fee_rate * 2.0,
        },
        "weekly_review_weekday": OFFICIAL_FAMILIES[0].rebalance.review_weekday,
    }

    report_dir = None
    if export_report:
        report_dir = export_truth_engine_report(
            reports_dir=reports_dir,
            run_id=resolved_run_id,
            created_at=created,
            summary_frame=summary_frame,
            manifest_payload=manifest,
            eligibility_frame=eligibility_frame,
            comparison_frame=comparison_frame,
            split_frame=split_frame,
            family_curves={
                family_id: result.equity_curve for family_id, result in family_results.items()
            },
            benchmark_curves={
                name: result.equity_curve for name, result in benchmark_results.items()
            },
            decision_logs={
                family_id: result.decisions for family_id, result in family_results.items()
            },
        )

    return TruthEngineRunResult(
        run_id=resolved_run_id,
        config=cfg,
        report_dir=report_dir,
        summary_frame=summary_frame,
        comparison_frame=comparison_frame,
        split_frame=split_frame,
        eligibility_frame=eligibility_frame,
        family_metrics=family_metrics,
        benchmark_metrics=benchmark_metrics,
        skipped_markets=skipped_markets,
        eligible_markets=eligible_markets,
        family_results=family_results,
        benchmark_results=benchmark_results,
        manifest=manifest,
    )


def _resolve_input_universe(
    *,
    reports_dir: Path,
    requested_universe: list[str],
    timeframe: str,
) -> tuple[list[str], dict[str, Any] | None]:
    data_summary = load_latest_truth_engine_data_summary(reports_dir)
    if not data_summary:
        return requested_universe, None

    if str(data_summary.get("timeframe", "")).lower() != timeframe.lower():
        return requested_universe, data_summary

    effective_universe = data_summary.get("effective_universe_final") or []
    if not effective_universe:
        return requested_universe, data_summary

    return list(effective_universe), data_summary


def _load_market_frames(
    *,
    raw_data_dir: Path,
    timeframe: str,
    markets: list[str],
) -> dict[str, pd.DataFrame]:
    results: dict[str, pd.DataFrame] = {}
    for market in markets:
        path = build_ohlcv_csv_path(
            raw_data_dir, exchange="bitvavo", market=market, timeframe=timeframe
        )
        frame = load_ohlcv_csv(path)
        if frame.empty:
            results[market] = pd.DataFrame(
                columns=["timestamp", "datetime", "open", "high", "low", "close", "volume"]
            )
            continue
        prepared = frame.copy()
        prepared["datetime"] = pd.to_datetime(prepared["timestamp"], unit="ms", utc=True)
        results[market] = prepared.loc[
            :, ["timestamp", "datetime", "open", "high", "low", "close", "volume"]
        ]
    return results


def _validate_required_csvs(
    *,
    raw_data_dir: Path,
    timeframe: str,
    markets: list[str],
    used_data_summary: bool,
) -> None:
    missing_paths = []
    for market in markets:
        path = build_ohlcv_csv_path(
            raw_data_dir=raw_data_dir,
            exchange="bitvavo",
            market=market,
            timeframe=timeframe,
        )
        if not path.exists():
            missing_paths.append(str(path))

    if not missing_paths:
        return

    if used_data_summary:
        raise FileNotFoundError(
            "Truth Engine data batch summary exists, but required CSV files are missing: "
            f"{missing_paths}"
        )

    raise FileNotFoundError(
        "Missing required Truth Engine CSV files. Run scripts/fetch_truth_engine_1d.py first. "
        f"Missing: {missing_paths}"
    )


def _align_market_frames(market_frames: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    timestamps = None
    for frame in market_frames.values():
        current = set(pd.to_numeric(frame["timestamp"], errors="coerce").astype("int64"))
        timestamps = current if timestamps is None else timestamps & current
    if not timestamps:
        raise ValueError("Eligible assets do not share a common 1D window.")
    common = sorted(timestamps)
    aligned: dict[str, pd.DataFrame] = {}
    for market, frame in market_frames.items():
        subset = (
            frame.loc[frame["timestamp"].isin(common)]
            .copy()
            .sort_values("timestamp")
            .reset_index(drop=True)
        )
        aligned[market] = subset
    return aligned


def _build_return_frame(market_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    dates = next(iter(market_frames.values())).loc[:, ["timestamp", "datetime"]].copy()
    for market, frame in market_frames.items():
        dates[market] = (
            pd.to_numeric(frame["close"], errors="coerce").pct_change().fillna(0.0).values
        )
    return dates


def _build_close_frame(market_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    dates = next(iter(market_frames.values())).loc[:, ["timestamp", "datetime"]].copy()
    for market, frame in market_frames.items():
        dates[market] = frame["close"].values
    return dates


def _run_benchmarks(
    market_frames: dict[str, pd.DataFrame],
    cfg: TruthEngineConfig,
) -> dict[str, BenchmarkResult | BacktestResult]:
    close_frame = _build_close_frame(market_frames)
    return {
        "buy_and_hold_btc": run_buy_and_hold(
            market_frames["BTC-EUR"], fee_rate=cfg.fee_rate, initial_capital=cfg.initial_capital
        ),
        "dca_btc": run_monthly_dca(
            market_frames["BTC-EUR"], fee_rate=cfg.fee_rate, contribution=cfg.dca_amount
        ),
        "sma_btc": run_sma_baseline(
            market_frames["BTC-EUR"],
            fee_rate=cfg.fee_rate,
            initial_capital=cfg.initial_capital,
            fast_window=cfg.sma_fast,
            slow_window=cfg.sma_slow,
        ),
        "equal_weight_basket": run_equal_weight_basket(
            close_frame,
            fee_rate=cfg.fee_rate,
            slippage_rate=cfg.slippage_rate,
            initial_capital=cfg.initial_capital,
        ),
        "btc_eth_60_40": run_fixed_mix_benchmark(
            close_frame,
            allocations={"BTC-EUR": 0.60, "ETH-EUR": 0.40},
            fee_rate=cfg.fee_rate,
            slippage_rate=cfg.slippage_rate,
            initial_capital=cfg.initial_capital,
        ),
    }


def _build_benchmark_metrics(
    benchmark_results: dict[str, BenchmarkResult | BacktestResult],
) -> dict[str, TruthMetrics]:
    metrics: dict[str, TruthMetrics] = {}
    for key, result in benchmark_results.items():
        equity_curve = result.equity_curve.copy()
        if "bar_return" not in equity_curve.columns:
            equity_curve["bar_return"] = equity_curve["equity"].pct_change().fillna(0.0)
        if "trade_fee" not in equity_curve.columns:
            equity_curve["trade_fee"] = 0.0
        if "turnover" not in equity_curve.columns:
            equity_curve["turnover"] = 0.0
        metrics[key] = calculate_truth_metrics(
            equity_curve,
            name=result.name,
            source_type="benchmark",
            rebalance_count=int(result.metadata.get("trades", 0)),
            trade_count=int(result.metadata.get("trades", 0)),
        )
    return metrics


def _build_split_frame(
    *,
    score_panel: pd.DataFrame,
    return_frame: pd.DataFrame,
    family_definitions: list[FamilyDefinition],
    fee_rate: float,
    slippage_rate: float,
) -> pd.DataFrame:
    dates = (
        score_panel.loc[:, ["timestamp", "datetime"]]
        .drop_duplicates()
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    splits = build_temporal_splits(dates)
    rows: list[dict[str, Any]] = []
    for split in splits:
        split_dates = slice_frame_for_split(dates, split)
        split_timestamps = set(split_dates["timestamp"].tolist())
        split_scores = score_panel.loc[score_panel["timestamp"].isin(split_timestamps)].copy()
        split_returns = return_frame.loc[
            return_frame["timestamp"].isin(split_timestamps)
        ].reset_index(drop=True)
        for family in family_definitions:
            rebalance = family.rebalance
            rebalance = type(rebalance)(
                review_weekday=rebalance.review_weekday,
                min_net_advantage=rebalance.min_net_advantage,
                no_trade_buffer=rebalance.no_trade_buffer,
                turnover_cap=rebalance.turnover_cap,
                fee_rate=fee_rate,
                slippage_rate=slippage_rate,
            )
            result = simulate_family_portfolio(
                score_panel=split_scores,
                return_frame=split_returns,
                score_column=family.score_column,
                family_name=family.name,
                weighting_config=family.weighting,
                rebalance_config=rebalance,
            )
            metrics = calculate_truth_metrics(
                result.equity_curve,
                name=family.name,
                source_type=split.segment,
                rebalance_count=int(result.metadata["rebalance_count"]),
                trade_count=int(result.metadata["trade_count"]),
            )
            row = metrics.__dict__.copy()
            row["split_id"] = split.split_id
            rows.append(row)
    return pd.DataFrame(rows)
