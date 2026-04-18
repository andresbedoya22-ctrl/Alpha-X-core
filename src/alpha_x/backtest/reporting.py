from __future__ import annotations

from collections.abc import Iterable

from alpha_x.backtest.metrics import PerformanceRow
from alpha_x.backtest.models import LoadedBacktestDataset


def _format_percent(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value * 100:.2f}%"


def _format_number(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:.2f}"


def _format_int(value: int | None) -> str:
    if value is None:
        return "N/A"
    return str(value)


def build_gap_status(dataset: LoadedBacktestDataset) -> str:
    summary = dataset.gap_summary
    if summary.gap_count == 0:
        return "Gaps residuales: no"

    return (
        "Gaps residuales: si | "
        f"gap_count={summary.gap_count} | "
        f"missing_intervals={summary.total_missing_intervals} | "
        f"largest_gap={summary.largest_gap}"
    )


def build_dataset_summary(dataset: LoadedBacktestDataset) -> str:
    info = dataset.dataset_info
    return (
        f"Dataset: {info.path} | market={info.market} | timeframe={info.timeframe} | "
        f"rows={info.row_count}"
    )


def build_performance_table(rows: Iterable[PerformanceRow]) -> str:
    table_rows = [
        [
            row.name,
            _format_percent(row.total_return),
            _format_percent(row.annualized_return),
            _format_percent(row.max_drawdown),
            _format_number(row.profit_factor),
            _format_int(row.trades),
            _format_percent(row.exposure),
            _format_number(row.final_equity),
        ]
        for row in rows
    ]
    headers = [
        "Strategy",
        "Total Return",
        "Annualized",
        "Max Drawdown",
        "Profit Factor",
        "Trades",
        "Exposure",
        "Final Equity",
    ]
    widths = [len(header) for header in headers]
    for row in table_rows:
        widths = [max(width, len(cell)) for width, cell in zip(widths, row, strict=True)]

    def format_row(cells: list[str]) -> str:
        return " | ".join(cell.ljust(width) for cell, width in zip(cells, widths, strict=True))

    separator = "-+-".join("-" * width for width in widths)
    lines = [format_row(headers), separator]
    lines.extend(format_row(row) for row in table_rows)
    return "\n".join(lines)
