from __future__ import annotations

from dataclasses import asdict

import pandas as pd

from alpha_x.validation.base import ValidationResultRow


def validation_rows_to_frame(rows: list[ValidationResultRow]) -> pd.DataFrame:
    return pd.DataFrame([asdict(row) for row in rows])


def build_oos_aggregate(rows: list[ValidationResultRow]) -> pd.DataFrame:
    frame = validation_rows_to_frame(rows)
    if frame.empty:
        return frame

    oos = frame.loc[frame["segment"].isin(["validation", "test", "walk_forward_test"])].copy()
    if oos.empty:
        return pd.DataFrame()

    grouped = (
        oos.groupby(["candidate_id", "candidate_name", "family", "source_type"], as_index=False)
        .agg(
            oos_segments=("segment", "count"),
            avg_total_return=("total_return", "mean"),
            median_total_return=("total_return", "median"),
            worst_total_return=("total_return", "min"),
            avg_max_drawdown=("max_drawdown", "mean"),
            avg_profit_factor=("profit_factor", "mean"),
            avg_trades=("trades", "mean"),
            avg_exposure=("exposure", "mean"),
            total_gap_count=("gap_count", "sum"),
            total_missing_intervals=("total_missing_intervals", "sum"),
        )
        .sort_values(["avg_total_return", "worst_total_return"], ascending=[False, False])
        .reset_index(drop=True)
    )
    return grouped


def build_validation_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "No validation rows."

    display = frame.copy()
    for column in ["total_return", "annualized_return", "max_drawdown", "exposure"]:
        display[column] = display[column].map(
            lambda value: "N/A" if pd.isna(value) else f"{float(value) * 100:.2f}%"
        )
    display["profit_factor"] = display["profit_factor"].map(
        lambda value: "N/A" if pd.isna(value) else f"{float(value):.2f}"
    )
    display["final_equity"] = display["final_equity"].map(lambda value: f"{float(value):.2f}")
    display["trades"] = display["trades"].map(
        lambda value: "N/A" if pd.isna(value) else str(int(value))
    )
    display["gap_count"] = display["gap_count"].map(str)
    display["missing"] = display["total_missing_intervals"].map(str)

    reduced = display.loc[
        :,
        [
            "candidate_name",
            "mode",
            "segment",
            "parameter_set",
            "total_return",
            "max_drawdown",
            "profit_factor",
            "trades",
            "exposure",
            "final_equity",
            "gap_count",
            "missing",
        ],
    ]
    headers = [
        "Candidate",
        "Mode",
        "Segment",
        "Params",
        "Total Return",
        "Max DD",
        "Profit Factor",
        "Trades",
        "Exposure",
        "Final Equity",
        "Gaps",
        "Missing",
    ]
    table_rows = reduced.values.tolist()
    widths = [len(header) for header in headers]
    for row in table_rows:
        widths = [max(width, len(str(cell))) for width, cell in zip(widths, row, strict=True)]

    def format_row(cells: list[str]) -> str:
        return " | ".join(str(cell).ljust(width) for cell, width in zip(cells, widths, strict=True))

    separator = "-+-".join("-" * width for width in widths)
    lines = [format_row(headers), separator]
    lines.extend(format_row([str(cell) for cell in row]) for row in table_rows)
    return "\n".join(lines)


def build_oos_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "No OOS aggregate rows."

    display = frame.copy()
    for column in [
        "avg_total_return",
        "median_total_return",
        "worst_total_return",
        "avg_max_drawdown",
    ]:
        display[column] = display[column].map(lambda value: f"{float(value) * 100:.2f}%")
    display["avg_profit_factor"] = display["avg_profit_factor"].map(
        lambda value: "N/A" if pd.isna(value) else f"{float(value):.2f}"
    )
    display["avg_trades"] = display["avg_trades"].map(lambda value: f"{float(value):.1f}")
    display["avg_exposure"] = display["avg_exposure"].map(
        lambda value: f"{float(value) * 100:.2f}%"
    )

    reduced = display.loc[
        :,
        [
            "candidate_name",
            "oos_segments",
            "avg_total_return",
            "median_total_return",
            "worst_total_return",
            "avg_max_drawdown",
            "avg_profit_factor",
            "avg_trades",
            "avg_exposure",
            "total_gap_count",
            "total_missing_intervals",
        ],
    ]
    headers = [
        "Candidate",
        "OOS Segments",
        "Avg Return",
        "Median Return",
        "Worst Return",
        "Avg Max DD",
        "Avg PF",
        "Avg Trades",
        "Avg Exposure",
        "Gap Count",
        "Missing",
    ]
    table_rows = reduced.values.tolist()
    widths = [len(header) for header in headers]
    for row in table_rows:
        widths = [max(width, len(str(cell))) for width, cell in zip(widths, row, strict=True)]

    def format_row(cells: list[str]) -> str:
        return " | ".join(str(cell).ljust(width) for cell, width in zip(cells, widths, strict=True))

    separator = "-+-".join("-" * width for width in widths)
    lines = [format_row(headers), separator]
    lines.extend(format_row([str(cell) for cell in row]) for row in table_rows)
    return "\n".join(lines)
