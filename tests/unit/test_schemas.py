import pytest
from pydantic import ValidationError

from src.domain_models import BacktestMetrics, StatResult


def test_stat_result_valid() -> None:
    data = {
        "target_day": 0,
        "t_statistic": 2.5,
        "p_value": 0.04,
        "is_significant": True,
    }
    model = StatResult.model_validate(data)
    assert model.target_day == 0
    assert model.p_value == 0.04


def test_stat_result_invalid_p_value_high() -> None:
    data = {
        "target_day": 0,
        "t_statistic": 2.5,
        "p_value": 1.5,
        "is_significant": True,
    }
    with pytest.raises(ValidationError) as exc_info:
        StatResult.model_validate(data)
    assert "p_value must be between 0.0 and 1.0 inclusive" in str(exc_info.value)


def test_stat_result_invalid_p_value_low() -> None:
    data = {
        "target_day": 0,
        "t_statistic": 2.5,
        "p_value": -0.1,
        "is_significant": True,
    }
    with pytest.raises(ValidationError) as exc_info:
        StatResult.model_validate(data)
    assert "p_value must be between 0.0 and 1.0 inclusive" in str(exc_info.value)


def test_stat_result_extra_forbid() -> None:
    data = {
        "target_day": 0,
        "t_statistic": 2.5,
        "p_value": 0.05,
        "is_significant": True,
        "extra_field": "not allowed",
    }
    with pytest.raises(ValidationError):
        StatResult.model_validate(data)


def test_backtest_metrics_valid() -> None:
    data = {
        "total_return": 15.5,
        "annualized_return": 10.2,
        "max_drawdown": -5.0,
        "win_rate": 60.0,
        "sharpe_ratio": 1.5,
    }
    model = BacktestMetrics.model_validate(data)
    assert model.win_rate == 60.0


def test_backtest_metrics_invalid_max_drawdown() -> None:
    data = {
        "total_return": 15.5,
        "annualized_return": 10.2,
        "max_drawdown": 5.0,
        "win_rate": 60.0,
        "sharpe_ratio": 1.5,
    }
    with pytest.raises(ValidationError) as exc_info:
        BacktestMetrics.model_validate(data)
    assert "max_drawdown must be a negative number or zero" in str(exc_info.value)


def test_backtest_metrics_invalid_win_rate_high() -> None:
    data = {
        "total_return": 15.5,
        "annualized_return": 10.2,
        "max_drawdown": -5.0,
        "win_rate": 105.0,
        "sharpe_ratio": 1.5,
    }
    with pytest.raises(ValidationError) as exc_info:
        BacktestMetrics.model_validate(data)
    assert "win_rate must be between 0.0 and 100.0 inclusive" in str(exc_info.value)


def test_backtest_metrics_extra_forbid() -> None:
    data = {
        "total_return": 15.5,
        "annualized_return": 10.2,
        "max_drawdown": -5.0,
        "win_rate": 50.0,
        "sharpe_ratio": 1.5,
        "extra_field": 123,
    }
    with pytest.raises(ValidationError):
        BacktestMetrics.model_validate(data)
