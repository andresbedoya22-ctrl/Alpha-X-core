from __future__ import annotations

from collections.abc import Iterable

from alpha_x.benchmarks import DatasetInfo
from alpha_x.benchmarks.metrics import BenchmarkMetrics


def _format_percent(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value * 100:.2f}%"


def _format_trades(value: int | None) -> str:
    if value is None:
        return "N/A"
    return str(value)


def build_comparative_table(metrics_list: Iterable[BenchmarkMetrics]) -> str:
    rows = [
        [
            metrics.name,
            _format_percent(metrics.total_return),
            _format_percent(metrics.annualized_return),
            _format_percent(metrics.max_drawdown),
            _format_trades(metrics.trades if metrics.benchmark_id == "sma_crossover" else None),
            _format_percent(metrics.exposure),
        ]
        for metrics in metrics_list
    ]

    headers = ["Benchmark", "Total Return", "Annualized", "Max Drawdown", "Trades", "Exposure"]
    widths = [len(header) for header in headers]
    for row in rows:
        widths = [max(width, len(cell)) for width, cell in zip(widths, row, strict=True)]

    def format_row(cells: list[str]) -> str:
        return " | ".join(cell.ljust(width) for cell, width in zip(cells, widths, strict=True))

    separator = "-+-".join("-" * width for width in widths)
    lines = [format_row(headers), separator]
    lines.extend(format_row(row) for row in rows)
    return "\n".join(lines)


def build_summary(dataset_info: DatasetInfo, metrics_list: list[BenchmarkMetrics]) -> str:
    highest_return = max(metrics_list, key=lambda item: item.total_return)
    lowest_drawdown = max(metrics_list, key=lambda item: item.max_drawdown)
    baseline = next(item for item in metrics_list if item.benchmark_id == "sma_crossover")

    return (
        f"Dataset: {dataset_info.path} | market={dataset_info.market} | "
        f"timeframe={dataset_info.timeframe} | rows={dataset_info.row_count}\n"
        f"Mayor retorno: {highest_return.name} ({_format_percent(highest_return.total_return)})\n"
        f"Menor drawdown: {lowest_drawdown.name} "
        f"({_format_percent(lowest_drawdown.max_drawdown)})\n"
        f"Baseline cuantitativa simple: {baseline.name}"
    )
