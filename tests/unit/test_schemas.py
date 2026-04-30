import pytest
from pydantic import ValidationError

from src.domain_models.backtest import BacktestMetrics
from src.domain_models.statistics import StatResult


def test_stat_result_valid() -> None:
    result = StatResult(
        target_day=1,
        t_statistic=2.5,
        p_value=0.04,
        is_significant=True,
    )
    assert result.target_day == 1
    assert result.t_statistic == 2.5
    assert result.p_value == 0.04
    assert result.is_significant is True


def test_stat_result_invalid_p_value() -> None:
    with pytest.raises(ValidationError):
        StatResult(
            target_day=1,
            t_statistic=2.5,
            p_value=1.5,
            is_significant=False,
        )

    with pytest.raises(ValidationError):
        StatResult(
            target_day=1,
            t_statistic=2.5,
            p_value=-0.1,
            is_significant=False,
        )


def test_stat_result_extra_forbid() -> None:
    with pytest.raises(ValidationError):
        StatResult(  # type: ignore[call-arg]
            target_day=1,
            t_statistic=2.5,
            p_value=0.5,
            is_significant=False,
            extra_field="test",
        )


def test_backtest_metrics_valid() -> None:
    metrics = BacktestMetrics(
        total_return=10.5,
        annualized_return=5.2,
        max_drawdown=-2.1,
        win_rate=55.5,
        sharpe_ratio=1.2,
    )
    assert metrics.total_return == 10.5
    assert metrics.annualized_return == 5.2
    assert metrics.max_drawdown == -2.1
    assert metrics.win_rate == 55.5
    assert metrics.sharpe_ratio == 1.2


def test_backtest_metrics_invalid_max_drawdown() -> None:
    with pytest.raises(ValidationError):
        BacktestMetrics(
            total_return=10.5,
            annualized_return=5.2,
            max_drawdown=1.0,
            win_rate=55.5,
            sharpe_ratio=1.2,
        )


def test_backtest_metrics_invalid_win_rate() -> None:
    with pytest.raises(ValidationError):
        BacktestMetrics(
            total_return=10.5,
            annualized_return=5.2,
            max_drawdown=-1.0,
            win_rate=105.0,
            sharpe_ratio=1.2,
        )

    with pytest.raises(ValidationError):
        BacktestMetrics(
            total_return=10.5,
            annualized_return=5.2,
            max_drawdown=-1.0,
            win_rate=-5.0,
            sharpe_ratio=1.2,
        )


def test_backtest_metrics_extra_forbid() -> None:
    with pytest.raises(ValidationError):
        BacktestMetrics(  # type: ignore[call-arg]
            total_return=10.5,
            annualized_return=5.2,
            max_drawdown=-1.0,
            win_rate=50.0,
            sharpe_ratio=1.2,
            extra=123,
        )
