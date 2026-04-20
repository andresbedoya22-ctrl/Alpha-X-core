from __future__ import annotations

from alpha_x.execution.distance_buffer_v1 import ExecutionState, OperationalSignal


def _pct(value: float) -> str:
    return f"{value:.2%}"


def _price(value: float) -> str:
    return f"{value:,.2f}"


def distance_to_trigger(signal: OperationalSignal) -> str:
    if signal.regime == "OFF":
        distance = (signal.threshold / signal.close) - 1.0
        return f"Falta {_pct(max(distance, 0.0))} para activar entrada."
    cushion = (signal.close / signal.threshold) - 1.0
    return f"Colchon sobre trigger: {_pct(max(cushion, 0.0))}."


def format_daily_message(signal: OperationalSignal) -> str:
    return "\n".join(
        [
            "Distance buffer v1 - revision diaria",
            f"Fecha senal: {signal.signal_date}",
            f"Regimen: {signal.regime}",
            f"Accion: {signal.action}",
            f"Exposicion actual: {_pct(signal.current_exposure)}",
            f"Exposicion objetivo: {_pct(signal.target_exposure)}",
            f"Close: {_price(signal.close)}",
            f"SMA125: {_price(signal.sma125)}",
            f"Threshold: {_price(signal.threshold)}",
            f"Motivo: {signal.reason}",
        ]
    )


def format_rebalance_message(signal: OperationalSignal) -> str:
    return "\n".join(
        [
            "Distance buffer v1 - REBALANCE",
            f"Fecha senal: {signal.signal_date}",
            f"Ejecucion t+1: {signal.execution_date}",
            f"Regimen: {signal.regime}",
            f"Exposicion: {_pct(signal.current_exposure)} -> {_pct(signal.target_exposure)}",
            f"Close: {_price(signal.close)}",
            f"Threshold: {_price(signal.threshold)}",
            f"Motivo: {signal.reason}",
        ]
    )


def format_no_operation_message(signal: OperationalSignal) -> str:
    return "\n".join(
        [
            "Distance buffer v1 - sin operacion",
            f"Regimen actual: {signal.regime}",
            f"No se opera: {signal.reason}",
            f"Close: {_price(signal.close)}",
            f"Threshold: {_price(signal.threshold)}",
            distance_to_trigger(signal),
        ]
    )


def format_status_message(signal: OperationalSignal, state: ExecutionState) -> str:
    return "\n".join(
        [
            "Distance buffer v1 - status",
            f"Ultima senal: {state.last_signal_date or signal.signal_date}",
            f"Regimen actual: {signal.regime}",
            f"Exposicion actual: {_pct(state.current_exposure)}",
            f"Close: {_price(signal.close)}",
            f"SMA125: {_price(signal.sma125)}",
            f"Threshold: {_price(signal.threshold)}",
            f"Ultima accion: {state.last_action or 'N/A'}",
            f"Motivo: {state.last_reason or signal.reason}",
        ]
    )
