from __future__ import annotations

from alpha_x.validation.sensitivity import (
    get_parameter_sensitivity_grid,
    get_validation_candidates,
)


def test_sensitivity_grid_is_small_and_defined_for_all_candidates() -> None:
    candidates = get_validation_candidates()

    for candidate in candidates:
        grid = get_parameter_sensitivity_grid(candidate)
        assert 2 <= len(grid) <= 3
        assert all(isinstance(item, dict) for item in grid)
