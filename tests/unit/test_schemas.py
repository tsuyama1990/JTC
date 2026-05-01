import pytest
from pydantic import ValidationError

from src.domain_models import BacktestMetrics, StatResult


def test_stat_result_valid() -> None:
    result = StatResult(
        target_day=0,
        t_statistic=2.5,
        p_value=0.01,
        is_significant=True,
    )
    assert result.target_day == 0
    assert result.t_statistic == 2.5
    assert result.p_value == 0.01
    assert result.is_significant is True


def test_stat_result_invalid_p_value() -> None:
    with pytest.raises(ValidationError):
        StatResult(
            target_day=0,
            t_statistic=2.5,
            p_value=1.5,
            is_significant=True,
        )

    with pytest.raises(ValidationError):
        StatResult(
            target_day=0,
            t_statistic=2.5,
            p_value=-0.1,
            is_significant=True,
        )


def test_backtest_metrics_valid() -> None:
    result = BacktestMetrics(
        total_return=10.5,
        annualized_return=5.2,
        max_drawdown=-2.5,
        win_rate=55.0,
        sharpe_ratio=1.2,
    )
    assert result.total_return == 10.5
    assert result.annualized_return == 5.2
    assert result.max_drawdown == -2.5
    assert result.win_rate == 55.0
    assert result.sharpe_ratio == 1.2


def test_backtest_metrics_invalid_max_drawdown() -> None:
    with pytest.raises(ValidationError):
        BacktestMetrics(
            total_return=10.5,
            annualized_return=5.2,
            max_drawdown=2.5,  # Invalid: positive
            win_rate=55.0,
            sharpe_ratio=1.2,
        )


def test_backtest_metrics_invalid_win_rate() -> None:
    with pytest.raises(ValidationError):
        BacktestMetrics(
            total_return=10.5,
            annualized_return=5.2,
            max_drawdown=-2.5,
            win_rate=105.0,  # Invalid: > 100
            sharpe_ratio=1.2,
        )

    with pytest.raises(ValidationError):
        BacktestMetrics(
            total_return=10.5,
            annualized_return=5.2,
            max_drawdown=-2.5,
            win_rate=-5.0,  # Invalid: < 0
            sharpe_ratio=1.2,
        )


def test_backtest_metrics_forbid_extra() -> None:
    with pytest.raises(ValidationError):
        BacktestMetrics(
            total_return=10.5,
            annualized_return=5.2,
            max_drawdown=-2.5,
            win_rate=55.0,
            sharpe_ratio=1.2,
            extra_field="invalid",
        )  # type: ignore[call-arg]


def test_stat_result_forbid_extra() -> None:
    with pytest.raises(ValidationError):
        StatResult(
            target_day=0,
            t_statistic=2.5,
            p_value=0.01,
            is_significant=True,
            extra_field="invalid",
        )  # type: ignore[call-arg]
