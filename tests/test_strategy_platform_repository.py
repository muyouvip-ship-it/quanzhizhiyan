import pytest

from tests.postgres_test_utils import isolated_postgres_session
from api.models.strategy_models import Base
from api.services.factor_registry import get_factor_registry_item, list_factor_registry
from api.services.strategy_platform_repository import (
    get_latest_completed_platform_backtest,
    get_platform_evolution_candidate,
    get_platform_evolution_experiment,
    get_platform_backtest_run,
    get_platform_strategy,
    list_platform_strategies,
    save_platform_evolution_experiment,
    save_platform_backtest_run,
    save_platform_strategy,
    update_platform_evolution_candidate_status,
    update_platform_strategy_metrics,
)


@pytest.fixture
def db():
    with isolated_postgres_session(Base, schema_prefix="ta_strategy_repo") as session:
        yield session


def test_strategy_platform_repository_persists_strategy_payload(db):
    saved = save_platform_strategy(
        db,
        {
            "id": "strategy_repo_test",
            "name": "仓储测试策略",
            "strategy_type": "portfolio",
            "status": "draft",
            "description": "测试策略持久化",
            "source": "manual",
            "current_version_id": "version_1",
            "version": 1,
            "is_active": False,
            "run_count": 0,
            "created_at": "2026-04-20T00:00:00+00:00",
            "updated_at": "2026-04-20T00:00:00+00:00",
            "performance": None,
            "current_version": {
                "id": "version_1",
                "strategy_id": "strategy_repo_test",
                "version": 1,
                "dsl": {"schema_version": "1.0", "strategy_type": "portfolio"},
                "compile_status": "passed",
                "compiled_hash": "abc123",
                "change_summary": "init",
                "created_at": "2026-04-20T00:00:00+00:00",
            },
            "tags": ["测试"],
        },
    )

    fetched = get_platform_strategy(db, saved["id"])
    items = list_platform_strategies(db)

    assert fetched is not None
    assert fetched["name"] == "仓储测试策略"
    assert fetched["current_version"]["id"] == "version_1"
    assert items == []
    assert len(list_platform_strategies(db, include_test=True)) == 1


def test_strategy_platform_repository_hides_test_strategies_by_default(db):
    base_payload = {
        "id": "strategy_repo_test_hidden",
        "name": "实时测试策略-abcdef",
        "strategy_type": "trading",
        "status": "active",
        "description": "实时监控测试策略",
        "source": "test",
        "current_version_id": "version_1",
        "version": 1,
        "is_active": True,
        "run_count": 0,
        "performance": None,
        "current_version": {
            "id": "version_1",
            "strategy_id": "strategy_repo_test_hidden",
            "version": 1,
            "dsl": {"schema_version": "1.0", "strategy_type": "trading"},
            "compile_status": "passed",
            "compiled_hash": "abc123",
            "change_summary": "init",
            "created_at": "2026-04-20T00:00:00+00:00",
        },
        "tags": ["AI创建", "待回测"],
    }
    save_platform_strategy(db, base_payload)

    assert list_platform_strategies(db) == []
    assert len(list_platform_strategies(db, include_test=True)) == 1


def test_strategy_platform_repository_hides_named_test_artifacts_by_default(db):
    base_payload = {
        "strategy_type": "portfolio",
        "status": "draft",
        "description": "策略平台接口测试数据",
        "current_version_id": "version_1",
        "version": 1,
        "is_active": False,
        "run_count": 0,
        "performance": None,
        "current_version": {
            "id": "version_1",
            "strategy_id": "strategy_repo_named_test",
            "version": 1,
            "dsl": {"schema_version": "1.0", "strategy_type": "portfolio"},
            "compile_status": "passed",
            "compiled_hash": "abc123",
            "change_summary": "init",
            "created_at": "2026-04-20T00:00:00+00:00",
        },
        "tags": ["AI创建", "待回测", "模板策略"],
    }
    save_platform_strategy(
        db,
        {
            **base_payload,
            "id": "strategy_repo_template_test",
            "name": "模板策略保存测试",
            "source": "template",
        },
    )
    save_platform_strategy(
        db,
        {
            **base_payload,
            "id": "strategy_repo_clone_test",
            "name": "克隆策略测试",
            "source": "manual",
        },
    )

    assert list_platform_strategies(db) == []
    assert len(list_platform_strategies(db, include_test=True)) == 2


def test_strategy_platform_repository_persists_backtest_and_updates_metrics(db):
    save_platform_strategy(
        db,
        {
            "id": "strategy_repo_test",
            "name": "仓储测试策略",
            "strategy_type": "portfolio",
            "status": "active",
            "description": "测试策略持久化",
            "source": "manual",
            "current_version_id": "version_1",
            "version": 1,
            "is_active": True,
            "run_count": 0,
            "created_at": "2026-04-20T00:00:00+00:00",
            "updated_at": "2026-04-20T00:00:00+00:00",
            "performance": None,
            "current_version": {
                "id": "version_1",
                "strategy_id": "strategy_repo_test",
                "version": 1,
                "dsl": {"schema_version": "1.0", "strategy_type": "portfolio"},
                "compile_status": "passed",
                "compiled_hash": "abc123",
                "change_summary": "init",
                "created_at": "2026-04-20T00:00:00+00:00",
            },
            "tags": ["测试"],
        },
    )

    saved_run = save_platform_backtest_run(
        db,
        {
            "id": "run_repo_test",
            "strategy_id": "strategy_repo_test",
            "strategy_version_id": "version_1",
            "status": "completed",
            "progress": 1.0,
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "initial_capital": 1000000,
            "frequency": "daily_minute",
            "benchmark": "沪深300",
            "artifact_root": "data/artifacts/backtests/run_repo_test",
            "created_at": "2026-04-20T00:00:00+00:00",
            "started_at": "2026-04-20T00:00:00+00:00",
            "completed_at": "2026-04-20T00:10:00+00:00",
            "result": {
                "metrics": {
                    "total_return": 0.21,
                    "annual_return": 0.24,
                    "sharpe_ratio": 1.5,
                    "max_drawdown": -0.08,
                    "win_rate": 0.61,
                    "profit_factor": 1.8,
                    "volatility": 0.18,
                    "final_capital": 1210000,
                    "calmar_ratio": 3.0,
                },
                "summary": {"engine_mode": "true_engine"},
            },
        },
    )
    updated = update_platform_strategy_metrics(
        db,
        "strategy_repo_test",
        saved_run["metrics"],
    )
    latest = get_latest_completed_platform_backtest(db, "strategy_repo_test")
    fetched_run = get_platform_backtest_run(db, "run_repo_test")

    assert fetched_run is not None
    assert fetched_run["frequency"] == "daily_minute"
    assert fetched_run["artifact_root"] == "data/artifacts/backtests/run_repo_test"
    assert latest is not None
    assert latest["id"] == "run_repo_test"
    assert updated is not None
    assert updated["performance"]["total_return"] == 0.21


def test_factor_registry_syncs_builtin_metadata(db):
    items = list_factor_registry(db)
    money_flow = get_factor_registry_item(db, "money_flow_strength_20d")

    assert len(items) >= 8
    assert money_flow is not None
    assert money_flow["display_name"] == "20日资金强度"
    assert money_flow["source_column"] == "money_flow_strength_20d"
    assert "amount" in money_flow["required_fields"]
    assert "polars" in money_flow["backend_support"]


def test_evolution_experiment_repository_persists_candidates(db):
    save_platform_strategy(
        db,
        {
            "id": "strategy_evolution_repo_test",
            "name": "进化仓储测试策略",
            "strategy_type": "portfolio",
            "status": "active",
            "description": "测试进化实验落库",
            "source": "manual",
            "current_version_id": "version_1",
            "version": 1,
            "is_active": True,
            "run_count": 0,
            "created_at": "2026-04-20T00:00:00+00:00",
            "updated_at": "2026-04-20T00:00:00+00:00",
            "performance": None,
            "current_version": {
                "id": "version_1",
                "strategy_id": "strategy_evolution_repo_test",
                "version": 1,
                "dsl": {"schema_version": "1.0", "strategy_type": "portfolio"},
                "compile_status": "passed",
                "compiled_hash": "abc123",
                "change_summary": "init",
                "created_at": "2026-04-20T00:00:00+00:00",
            },
            "tags": ["测试"],
        },
    )

    saved = save_platform_evolution_experiment(
        db,
        {
            "id": "experiment_repo_test",
            "strategy_id": "strategy_evolution_repo_test",
            "objective": "calmar_then_win_rate",
            "status": "completed",
            "progress": 1.0,
            "search_space": {"mutations": ["factor_weight"]},
            "base_backtest_run_id": "run_base",
            "created_at": "2026-04-20T00:00:00+00:00",
            "candidates": [
                {
                    "id": "candidate_repo_test",
                    "experiment_id": "experiment_repo_test",
                    "name": "资金流增强版",
                    "score": 88.5,
                    "status": "candidate",
                    "improvement_summary": "提高资金流权重",
                    "risk_flags": ["换手率上升"],
                    "metrics": {
                        "total_return": 0.25,
                        "annual_return": 0.3,
                        "sharpe_ratio": 1.8,
                        "max_drawdown": -0.08,
                        "win_rate": 0.63,
                        "profit_factor": 1.9,
                        "volatility": 0.18,
                        "final_capital": 1250000,
                        "calmar_ratio": 3.2,
                    },
                    "dsl_patch": {"factor_model.factors.money_flow_strength_20d.weight": 0.32},
                }
            ],
        },
    )
    fetched = get_platform_evolution_experiment(db, "experiment_repo_test")
    candidate = get_platform_evolution_candidate(db, "candidate_repo_test")
    updated = update_platform_evolution_candidate_status(db, "candidate_repo_test", status="accepted")

    assert saved["id"] == "experiment_repo_test"
    assert fetched is not None
    assert fetched["candidates"][0]["name"] == "资金流增强版"
    assert candidate is not None
    assert candidate["dsl_patch"]["factor_model.factors.money_flow_strength_20d.weight"] == 0.32
    assert updated is not None
    assert updated["status"] == "accepted"
