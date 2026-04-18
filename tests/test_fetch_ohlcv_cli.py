from __future__ import annotations

import importlib.util
from pathlib import Path


def load_fetch_ohlcv_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "fetch_ohlcv.py"
    spec = importlib.util.spec_from_file_location("fetch_ohlcv", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load fetch_ohlcv.py")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_build_parser_accepts_backfill_arguments() -> None:
    module = load_fetch_ohlcv_module()

    args = module.build_parser().parse_args(["--backfill", "--target-rows", "10000"])

    assert args.backfill is True
    assert args.target_rows == 10000


def test_build_parser_accepts_gap_arguments() -> None:
    module = load_fetch_ohlcv_module()

    args = module.build_parser().parse_args(["--report-gaps"])

    assert args.report_gaps is True
    assert args.repair_gaps is False
