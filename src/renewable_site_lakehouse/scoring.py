"""Pure, explainable scoring functions shared by local tests and Databricks jobs."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ScoreWeights:
    grid: float = 0.40
    land: float = 0.35
    data_quality: float = 0.15
    planning: float = 0.10

    def validate(self) -> None:
        values = (self.grid, self.land, self.data_quality, self.planning)
        if any(value < 0 for value in values):
            raise ValueError("Score weights must be non-negative")
        if abs(sum(values) - 1.0) > 1e-9:
            raise ValueError("Score weights must sum to 1.0")


def _bounded(value: float) -> float:
    return max(0.0, min(100.0, float(value)))


def calculate_total_score(
    *,
    grid_score: float,
    land_score: float,
    data_quality_score: float,
    planning_score: float,
    weights: ScoreWeights = ScoreWeights(),
) -> float:
    """Return a transparent weighted score in the inclusive range 0..100."""

    weights.validate()
    total = (
        weights.grid * _bounded(grid_score)
        + weights.land * _bounded(land_score)
        + weights.data_quality * _bounded(data_quality_score)
        + weights.planning * _bounded(planning_score)
    )
    return round(_bounded(total), 2)

