import pytest

from renewable_site_lakehouse.scoring import ScoreWeights, calculate_total_score


def test_default_weights_produce_expected_score() -> None:
    score = calculate_total_score(
        grid_score=80,
        land_score=70,
        data_quality_score=60,
        planning_score=50,
    )

    assert score == 70.5


def test_score_inputs_are_bounded() -> None:
    score = calculate_total_score(
        grid_score=200,
        land_score=-10,
        data_quality_score=100,
        planning_score=100,
    )

    assert score == 65.0


def test_invalid_weights_are_rejected() -> None:
    weights = ScoreWeights(grid=0.5, land=0.5, data_quality=0.5, planning=0.0)

    with pytest.raises(ValueError, match="sum to 1.0"):
        calculate_total_score(
            grid_score=50,
            land_score=50,
            data_quality_score=50,
            planning_score=50,
            weights=weights,
        )
