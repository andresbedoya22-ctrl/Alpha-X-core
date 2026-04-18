from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def load_run_benchmarks_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "run_benchmarks.py"
    spec = importlib.util.spec_from_file_location("run_benchmarks", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load run_benchmarks.py")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_fee_bps() -> None:
    module = load_run_benchmarks_module()

    args = module.parse_args(["--fee-bps", "25", "--sma-fast", "20", "--sma-slow", "50"])

    assert args.fee_bps == pytest.approx(25.0)
    assert args.fee is None


def test_resolve_fee_rate_prefers_basis_points() -> None:
    module = load_run_benchmarks_module()

    args = module.parse_args(["--fee-bps", "25"])

    assert module.resolve_fee_rate(args, default_fee_rate=0.001) == pytest.approx(0.0025)


def test_resolve_fee_rate_supports_decimal_fee() -> None:
    module = load_run_benchmarks_module()

    args = module.parse_args(["--fee", "0.001"])

    assert module.resolve_fee_rate(args, default_fee_rate=0.0025) == pytest.approx(0.001)
