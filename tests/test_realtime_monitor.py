from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm.attributes import flag_modified

from api.main import app
from api.core import rate_limit as rate_limit_module
from api.core.strategy_db import get_strategy_db_ctx
from api.database import get_db_ctx
from api.models.strategy_models import (
    RealtimeApprovalDB,
    RealtimeEventDB,
    RealtimeMonitorDB,
    RealtimeSignalExecutionDB,
    StrategyDB,
    StrategyStatus,
    StrategyType,
)
from api.routes.strategy_platform import _default_dsl
from api.services import catalyst_selection_service, qmt_minute_subscription_service, qmt_virtual_account_service, realtime_monitor_service
from api.services.qmt_virtual_account_service import QmtRuntimeConfig


@pytest.fixture(autouse=True)
def _cleanup_realtime_monitor_test_records():
    with get_strategy_db_ctx() as db:
        existing_monitor_ids = {row[0] for row in db.query(RealtimeMonitorDB.id).all()}

    yield

    with get_strategy_db_ctx() as db:
        query = db.query(RealtimeMonitorDB.id)
        if existing_monitor_ids:
            query = query.filter(RealtimeMonitorDB.id.notin_(existing_monitor_ids))
        new_monitor_ids = [row[0] for row in query.all()]
        if new_monitor_ids:
            realtime_monitor_service.delete_monitor_records(db, new_monitor_ids)
            db.commit()


def _client() -> TestClient:
    return TestClient(app, raise_server_exceptions=False)


def _auth(client: TestClient, email: str | None = None) -> str:
    with rate_limit_module._LOCK:
        rate_limit_module._WINDOWS.clear()
    target = email or f"realtime-{uuid4().hex[:8]}@test.com"
    response = client.post("/v1/auth/request-code", json={"email": target})
    code = response.json()["dev_code"]
    verified = client.post("/v1/auth/verify-code", json={"email": target, "code": code})
    return verified.json()["access_token"]


def _create_strategy(client: TestClient, name: str) -> str:
    response = client.post(
        "/v1/strategies",
        json={
            "name": name,
            "strategy_type": "trading",
            "description": "实时监控测试策略",
            "dsl": _default_dsl("trading").model_dump(),
            "status": "active",
            "source": "test",
        },
    )
    assert response.status_code == 200
    return response.json()["id"]


def _closed_bar_end_for_test(timeframe: str = "5m") -> datetime:
    return realtime_monitor_service._latest_closed_bar_end(datetime.now(), timeframe)


def _mock_common(monkeypatch, account_key: str = "paper_sim", role: str = "paper"):
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda *args, **kwargs: [
            QmtRuntimeConfig(
                key=account_key,
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="39027628" if role == "paper" else "8886186680",
                account_type="STOCK",
                account_name="实时监控测试账户",
                userdata_path="D:/qmt/userdata_mini",
                role=role,
                bridge_base_url="http://127.0.0.1:8710" if role == "paper" else "http://127.0.0.1:8711",
                bridge_token="bridge-token",
                refresh_interval_seconds=10,
            )
        ],
    )
    monkeypatch.setattr(
        "api.services.realtime_monitor_service.watchlist_service.list_watchlist",
        lambda db, user_id: [],
    )
    monkeypatch.setattr(
        "api.services.realtime_monitor_service.qmt_virtual_account_service.get_qmt_virtual_account_overview",
        lambda db, user_id, account_key=None, **kwargs: {
            "account": {
                "account_id": "39027628",
                "total_asset": 1_000_000.0,
                "available_cash": 900_000.0,
                "cash": 900_000.0,
            },
            "positions": [],
            "connection": {"account_key": account_key or "paper_sim", "connected": True},
            "fetched_at": "2026-04-23T10:00:00+08:00",
            "data_source": "live",
            "is_stale": False,
        },
    )
    monkeypatch.setattr(
        "api.services.realtime_monitor_service.qmt_virtual_account_service._fetch_live_quotes",
        lambda symbols, **kwargs: {symbol: {"price": 10.5, "close": 10.4, "source": "mock"} for symbol in symbols},
    )
    monkeypatch.setattr(
        "api.services.realtime_monitor_service.capture_today_minute_bars",
        lambda **kwargs: {
            "success": True,
            "rows": len(kwargs.get("symbols") or []) * 20,
            "trade_date": kwargs.get("trade_date"),
            "source": "mock_qmt",
            "captured_symbols": list(kwargs.get("symbols") or []),
            "missing_symbols": [],
            "symbol_rows": {symbol: 20 for symbol in (kwargs.get("symbols") or [])},
            "symbol_latest_trade_times": {
                symbol: datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                for symbol in (kwargs.get("symbols") or [])
            },
        },
    )
    monkeypatch.setattr(
        "api.services.realtime_monitor_service.evaluate_intraday_confirmation",
        lambda symbols, trade_date, timeframe="30m", **kwargs: type(
            "MinuteResult",
            (),
            {
                "timeframe": timeframe,
                "source": "mock",
                "items": [{"symbol": symbol, "confirmed": True, "timeframe": timeframe, "bar_end": _closed_bar_end_for_test(timeframe)} for symbol in symbols[:2]],
                "missing_symbols": [],
            },
        )(),
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service.submit_qmt_order",
        lambda db, user_id, **kwargs: {
            "message": "QMT 委托已提交",
            "account_key": kwargs.get("account_key"),
            "request_id": "mock-request-id",
            "order_result": {
                "success": True,
                "order_id": f"mock-{kwargs['symbol']}",
                "bridge": {"account_key": kwargs.get("account_key")},
            },
            "overview": {
                "orders": [
                    {
                        "order_id": f"mock-{kwargs['symbol']}",
                        "symbol": kwargs["symbol"],
                        "side": kwargs["side"],
                        "status": "submitted",
                        "quantity": kwargs["quantity"],
                    }
                ],
                "trades": [],
            },
        },
    )


def _create_paused_qmt_recovery_monitor(*, append_manual_pause: bool = False) -> str:
    user_id = f"qmt-auto-resume-{uuid4().hex}"
    strategy_id = uuid4().hex
    monitor_id = uuid4().hex
    with get_strategy_db_ctx() as db:
        db.add(
            StrategyDB(
                id=strategy_id,
                name=f"QMT恢复测试策略-{uuid4().hex[:6]}",
                strategy_type=StrategyType.TRADING,
                status=StrategyStatus.ACTIVE,
                is_active=True,
            )
        )
        monitor = RealtimeMonitorDB(
            id=monitor_id,
            user_id=user_id,
            name=f"QMT恢复测试监控-{uuid4().hex[:6]}",
            account_key="paper_sim",
            account_role="paper",
            strategy_id=strategy_id,
            status="paused",
            execution_mode="auto",
            auto_trade_enabled=True,
            quote_source="qmt",
            monitor_pool_json={"symbols": ["300520.SZ"], "resolved_symbols": ["300520.SZ"]},
            config_json={"poll_interval_seconds": 20},
            risk_config_json={},
            state_json={},
            updated_at=realtime_monitor_service._now_dt(),
        )
        db.add(monitor)
        db.flush()
        realtime_monitor_service._append_event(
            db,
            monitor,
            "monitor_fused",
            error_payload={"reason": "QMT 账户实时快照不可用，实时监控禁止使用缓存：QMT bridge不可达：http://127.0.0.1:8710"},
        )
        realtime_monitor_service._append_event(db, monitor, "fuse_reset", payload={"status": "paused"})
        if append_manual_pause:
            realtime_monitor_service._append_event(db, monitor, "monitor_paused", payload={"status": "paused"})
        db.commit()
    return monitor_id


def test_create_and_start_paper_monitor(monkeypatch):
    client = _client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}
    strategy_id = _create_strategy(client, f"实时测试策略-{uuid4().hex[:6]}")
    _mock_common(monkeypatch, account_key="paper_sim", role="paper")

    created = client.post(
        "/v1/realtime/monitors",
        headers=headers,
        json={
            "name": "虚拟仓自动监控",
            "strategy_id": strategy_id,
            "account_key": "paper_sim",
            "execution_mode": "auto",
            "monitor_pool": {"symbols": ["000001.SZ"]},
            "config": {"poll_interval_seconds": 10},
        },
    )
    assert created.status_code == 200
    monitor_id = created.json()["id"]
    assert created.json()["status"] == "ready"

    started = client.post(f"/v1/realtime/monitors/{monitor_id}/start", headers=headers)
    assert started.status_code == 200
    assert started.json()["status"] == "running"

    events = client.get(f"/v1/realtime/monitors/{monitor_id}/events", headers=headers)
    assert events.status_code == 200
    event_types = [item["event_type"] for item in events.json()["items"]]
    assert "monitor_created" in event_types
    assert "monitor_started" in event_types

    runtime_events = client.get(f"/v1/realtime/monitors/{monitor_id}/events?since_started=true&limit=10000", headers=headers)
    assert runtime_events.status_code == 200
    runtime_event_types = [item["event_type"] for item in runtime_events.json()["items"]]
    assert "monitor_created" not in runtime_event_types
    assert "monitor_started" in runtime_event_types


def test_live_monitor_auto_trade_is_downgraded_to_monitor_only(monkeypatch):
    client = _client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}
    strategy_id = _create_strategy(client, f"实盘监控策略-{uuid4().hex[:6]}")
    _mock_common(monkeypatch, account_key="live_real", role="live")

    created = client.post(
        "/v1/realtime/monitors",
        headers=headers,
        json={
            "name": "实盘只读监控",
            "strategy_id": strategy_id,
            "account_key": "live_real",
            "execution_mode": "auto",
            "monitor_pool": {"symbols": ["600519.SH"]},
        },
    )
    assert created.status_code == 200
    monitor_id = created.json()["id"]

    started = client.post(f"/v1/realtime/monitors/{monitor_id}/start", headers=headers)
    assert started.status_code == 200
    payload = started.json()
    assert payload["status"] == "running"
    assert payload["execution_mode"] == "monitor_only"
    assert payload["auto_trade_enabled"] is False

    events = client.get(f"/v1/realtime/monitors/{monitor_id}/events", headers=headers)
    event_types = [item["event_type"] for item in events.json()["items"]]
    assert "live_readonly_guard" in event_types


def test_obsolete_pending_approval_is_archived_and_does_not_block_auto_order(monkeypatch):
    client = _client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}
    strategy_id = _create_strategy(client, f"旧确认归档策略-{uuid4().hex[:6]}")
    _mock_common(monkeypatch, account_key="paper_sim", role="paper")
    monkeypatch.setattr("api.services.realtime_monitor_service._is_monitor_trading_window", lambda value: True)

    created = client.post(
        "/v1/realtime/monitors",
        headers=headers,
        json={
            "name": "旧确认不阻断自动执行",
            "strategy_id": strategy_id,
            "account_key": "paper_sim",
            "execution_mode": "auto",
            "monitor_pool": {"symbols": ["000001.SZ"]},
            "config": {"poll_interval_seconds": 20, "max_signals_per_cycle": 1},
        },
    )
    monitor = created.json()
    assert created.status_code == 200

    with get_strategy_db_ctx() as db:
        approval = RealtimeApprovalDB(
            id=uuid4().hex,
            monitor_id=monitor["id"],
            user_id=monitor["user_id"],
            account_key=monitor["account_key"],
            strategy_id=monitor["strategy_id"],
            symbol="000001.SZ",
            side="buy",
            status="pending",
            reason="历史人工确认任务",
            order_intent_json={
                "account_key": monitor["account_key"],
                "symbol": "000001.SZ",
                "side": "buy",
                "quantity": 100,
                "price_type": "opponent",
                "strategy_name": "RealtimeMonitor-Test",
                "order_remark": "obsolete-approval-test",
            },
        )
        db.add(approval)
        db.commit()
        approval_id = approval.id

    run_once = client.post(f"/v1/realtime/monitors/{monitor['id']}/run-once", headers=headers)
    assert run_once.status_code == 200
    event_types = [item["event_type"] for item in run_once.json()["events"]]
    assert "order_submitted" in event_types
    assert "approval_created" not in event_types

    with get_strategy_db_ctx() as db:
        archived = db.query(RealtimeApprovalDB).filter(RealtimeApprovalDB.id == approval_id).one()
        assert archived.status == "rejected"
        assert archived.decision_json["reason"] == "manual_approval_flow_removed"
        assert db.query(RealtimeApprovalDB).filter(
            RealtimeApprovalDB.monitor_id == monitor["id"],
            RealtimeApprovalDB.status == "pending",
        ).count() == 0


def test_realtime_config_strips_manual_approval_routes():
    config = realtime_monitor_service._default_config(
        {
            "signal_routes": [
                {
                    "side": "sell",
                    "timeframe": "5m",
                    "action": "approval_required",
                    "require_approval": True,
                }
            ]
        }
    )

    route = config["signal_routes"][0]
    assert route["action"] == "reduce_position"
    assert route["require_approval"] is False


def test_run_monitor_once_generates_signal_and_order_events(monkeypatch):
    client = _client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}
    strategy_id = _create_strategy(client, f"单轮执行策略-{uuid4().hex[:6]}")
    _mock_common(monkeypatch, account_key="paper_sim", role="paper")
    monkeypatch.setattr("api.services.realtime_monitor_service._is_monitor_trading_window", lambda value: True)

    created = client.post(
        "/v1/realtime/monitors",
        headers=headers,
        json={
            "name": "立即跑一轮测试",
            "strategy_id": strategy_id,
            "account_key": "paper_sim",
            "execution_mode": "auto",
            "monitor_pool": {"symbols": ["000001.SZ"]},
            "config": {"poll_interval_seconds": 20, "max_signals_per_cycle": 1},
        },
    )
    assert created.status_code == 200
    monitor_id = created.json()["id"]

    run_once = client.post(f"/v1/realtime/monitors/{monitor_id}/run-once", headers=headers)
    assert run_once.status_code == 200
    payload = run_once.json()
    assert payload["monitor"]["id"] == monitor_id
    event_types = [item["event_type"] for item in payload["events"]]
    assert "manual_cycle_requested" in event_types
    assert "cycle_started" in event_types
    assert "signal_generated" in event_types
    assert "order_submitted" in event_types


def test_run_monitor_once_deduplicates_same_bar_signal(monkeypatch):
    client = _client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}
    strategy_id = _create_strategy(client, f"同K线去重策略-{uuid4().hex[:6]}")
    _mock_common(monkeypatch, account_key="paper_sim", role="paper")
    monkeypatch.setattr("api.services.realtime_monitor_service._is_monitor_trading_window", lambda value: True)

    submit_count = {"value": 0}

    def submit_order(db, user_id, **kwargs):
        submit_count["value"] += 1
        return {
            "message": "QMT 委托已提交",
            "account_key": kwargs.get("account_key"),
            "request_id": f"request-{submit_count['value']}",
            "order_result": {
                "success": True,
                "order_id": f"dedupe-{submit_count['value']}-{kwargs['symbol']}",
            },
            "overview": {"orders": [], "trades": []},
        }

    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service.submit_qmt_order",
        submit_order,
    )

    created = client.post(
        "/v1/realtime/monitors",
        headers=headers,
        json={
            "name": "同K线信号只执行一次",
            "strategy_id": strategy_id,
            "account_key": "paper_sim",
            "execution_mode": "auto",
            "monitor_pool": {"symbols": ["000001.SZ"]},
            "config": {"poll_interval_seconds": 20, "max_signals_per_cycle": 1},
        },
    )
    assert created.status_code == 200
    monitor_id = created.json()["id"]

    first_run = client.post(f"/v1/realtime/monitors/{monitor_id}/run-once", headers=headers)
    second_run = client.post(f"/v1/realtime/monitors/{monitor_id}/run-once", headers=headers)

    assert first_run.status_code == 200
    assert second_run.status_code == 200
    assert submit_count["value"] == 1
    second_event_types = [item["event_type"] for item in second_run.json()["events"]]
    assert "signal_deduplicated" not in second_event_types
    assert second_event_types.count("order_submitted") == 1

    with get_strategy_db_ctx() as db:
        rows = db.query(RealtimeSignalExecutionDB).filter(RealtimeSignalExecutionDB.monitor_id == monitor_id).all()
        assert len(rows) == 1
        assert rows[0].status == "submitted"


def test_max_signals_does_not_reserve_deferred_same_bar_signals(monkeypatch):
    client = _client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}
    strategy_id = _create_strategy(client, f"同K线延迟信号策略-{uuid4().hex[:6]}")
    _mock_common(monkeypatch, account_key="paper_sim", role="paper")
    monkeypatch.setattr("api.services.realtime_monitor_service._is_monitor_trading_window", lambda value: True)

    submit_count = {"value": 0}

    def submit_order(db, user_id, **kwargs):
        submit_count["value"] += 1
        return {
            "message": "QMT 委托已提交",
            "account_key": kwargs.get("account_key"),
            "request_id": f"request-{submit_count['value']}",
            "order_result": {
                "success": True,
                "order_id": f"deferred-{submit_count['value']}-{kwargs['symbol']}",
            },
            "overview": {"orders": [], "trades": []},
        }

    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service.submit_qmt_order",
        submit_order,
    )

    created = client.post(
        "/v1/realtime/monitors",
        headers=headers,
        json={
            "name": "每轮最多信号不污染账本",
            "strategy_id": strategy_id,
            "account_key": "paper_sim",
            "execution_mode": "auto",
            "monitor_pool": {"symbols": ["000001.SZ", "000002.SZ"]},
            "config": {"poll_interval_seconds": 20, "max_signals_per_cycle": 1},
        },
    )
    assert created.status_code == 200
    monitor_id = created.json()["id"]

    first_run = client.post(f"/v1/realtime/monitors/{monitor_id}/run-once", headers=headers)
    assert first_run.status_code == 200
    assert submit_count["value"] == 1
    with get_strategy_db_ctx() as db:
        assert db.query(RealtimeSignalExecutionDB).filter(RealtimeSignalExecutionDB.monitor_id == monitor_id).count() == 1

    second_run = client.post(f"/v1/realtime/monitors/{monitor_id}/run-once", headers=headers)
    assert second_run.status_code == 200
    assert submit_count["value"] == 2
    with get_strategy_db_ctx() as db:
        rows = db.query(RealtimeSignalExecutionDB).filter(RealtimeSignalExecutionDB.monitor_id == monitor_id).order_by(RealtimeSignalExecutionDB.symbol).all()
        assert [row.symbol for row in rows] == ["000001.SZ", "000002.SZ"]


def test_run_monitor_once_blocks_unsellable_sell_signal_without_rejection(monkeypatch):
    client = _client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}
    strategy_id = _create_strategy(client, f"不可卖卖出策略-{uuid4().hex[:6]}")
    _mock_common(monkeypatch, account_key="paper_sim", role="paper")
    monkeypatch.setattr("api.services.realtime_monitor_service._is_monitor_trading_window", lambda value: True)

    def overview(db, user_id, account_key=None, **kwargs):
        return {
            "account": {
                "account_id": "39027628",
                "total_asset": 1_000_000.0,
                "available_cash": 900_000.0,
                "cash": 900_000.0,
            },
            "positions": [
                {
                    "symbol": "300520.SZ",
                    "name": "科大国创",
                    "current_position": 100.0,
                    "available_position": 0.0,
                    "average_cost": 10.0,
                    "current_price": 10.2,
                    "market_value": 1020.0,
                    "total_pnl": 20.0,
                    "total_pnl_pct": 2.0,
                }
            ],
            "orders": [],
            "trades": [],
            "connection": {"account_key": account_key or "paper_sim", "connected": True},
            "fetched_at": "2026-04-23T14:25:00+08:00",
            "data_source": "live",
            "is_stale": False,
        }

    monkeypatch.setattr(
        "api.services.realtime_monitor_service.qmt_virtual_account_service.get_qmt_virtual_account_overview",
        overview,
    )
    monkeypatch.setattr(
        "api.services.realtime_monitor_service.capture_today_minute_bars",
        lambda **kwargs: {
            "success": True,
            "rows": 5,
            "trade_date": kwargs.get("trade_date"),
            "source": "mock_qmt",
            "captured_symbols": list(kwargs.get("symbols") or []),
            "missing_symbols": [],
            "symbol_rows": {symbol: 5 for symbol in (kwargs.get("symbols") or [])},
            "symbol_latest_trade_times": {
                symbol: datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                for symbol in (kwargs.get("symbols") or [])
            },
        },
    )
    monkeypatch.setattr(
        "api.services.realtime_monitor_service.evaluate_first_day_band_signals",
        lambda symbols, trade_date, timeframe="5m", supplement_frame=None, supplement_source=None: type(
            "MinuteResult",
            (),
            {
                "timeframe": timeframe,
                "source": "mock",
                "items": [
                    {
                        "symbol": "300520.SZ",
                        "signal": "sell",
                        "close": 10.2,
                        "cross_below": True,
                            "bar_end": _closed_bar_end_for_test(),
                    }
                ],
                "missing_symbols": [],
            },
        )(),
    )

    created = client.post(
        "/v1/realtime/monitors",
        headers=headers,
        json={
            "name": "不可卖卖出测试",
            "strategy_id": strategy_id,
            "account_key": "paper_sim",
            "execution_mode": "auto",
            "monitor_pool": {"symbols": ["300520.SZ"]},
            "config": {"poll_interval_seconds": 20, "max_signals_per_cycle": 1, "signal_mode": "first_day_band", "signal_timeframe": "5m"},
        },
    )
    assert created.status_code == 200
    monitor_id = created.json()["id"]

    run_once = client.post(f"/v1/realtime/monitors/{monitor_id}/run-once", headers=headers)
    assert run_once.status_code == 200
    event_types = [item["event_type"] for item in run_once.json()["events"]]
    assert "signal_generated" in event_types
    assert "signal_blocked" in event_types
    assert "order_rejected" not in event_types
    assert "order_submitted" not in event_types


def test_realtime_first_day_band_uses_only_symbols_captured_by_qmt(monkeypatch):
    today = datetime.now().date().isoformat()
    latest_bar = realtime_monitor_service._latest_closed_bar_end(datetime.now(), "5m").isoformat()
    monitor = SimpleNamespace(
        account_key="paper_sim",
        config_json={"signal_mode": "first_day_band", "signal_timeframe": "5m"},
    )
    captured_eval_symbols = {}

    def fake_evaluate(symbols, trade_date, timeframe="5m", **kwargs):
        captured_eval_symbols["symbols"] = list(symbols)
        return type(
            "MinuteResult",
            (),
            {
                "timeframe": timeframe,
                "source": "postgresql:market_stock_minute_kline",
                "items": [
                    {
                        "symbol": "300520.SZ",
                        "signal": "sell",
                        "close": 10.2,
                        "cross_below": True,
                        "bar_end": latest_bar,
                    },
                    {
                        "symbol": "603118.SH",
                        "signal": "sell",
                        "close": 15.2,
                        "cross_below": True,
                        "bar_end": latest_bar,
                    },
                ],
                "missing_symbols": [],
            },
        )()

    monkeypatch.setattr("api.services.realtime_monitor_service.evaluate_first_day_band_signals", fake_evaluate)
    monkeypatch.setattr("api.services.realtime_monitor_service._supplement_first_day_band_result", lambda **kwargs: None)

    result = realtime_monitor_service._build_minute_features(
        monitor,
        ["300520.SZ", "603118.SH"],
        minute_capture={
            "success": True,
            "rows": 10,
            "trade_date": today,
            "captured_symbols": ["300520.SZ"],
            "missing_symbols": ["603118.SH"],
            "symbol_rows": {"300520.SZ": 10, "603118.SH": 0},
            "symbol_latest_trade_times": {"300520.SZ": latest_bar.replace("T", " ")},
        },
    )

    assert captured_eval_symbols["symbols"] == ["300520.SZ"]
    assert [item["symbol"] for item in result["items"]] == ["300520.SZ"]
    assert result["missing_symbols"] == ["603118.SH"]
    assert result["qmt_required"] is True


def test_realtime_first_day_band_filters_stale_minute_rows(monkeypatch):
    today = datetime.now().date().isoformat()
    latest_bar = realtime_monitor_service._latest_closed_bar_end(datetime.now(), "5m").isoformat()
    monitor = SimpleNamespace(
        account_key="paper_sim",
        config_json={"signal_mode": "first_day_band", "signal_timeframe": "5m"},
    )

    def fake_evaluate(symbols, trade_date, timeframe="5m", **kwargs):
        return type(
            "MinuteResult",
            (),
            {
                "timeframe": timeframe,
                "source": "postgresql:market_stock_minute_kline",
                "items": [
                    {
                        "symbol": "300520.SZ",
                        "signal": "hold",
                        "close": 37.1,
                        "cross_below": False,
                        "bar_end": latest_bar,
                    },
                    {
                        "symbol": "603118.SH",
                        "signal": "sell",
                        "close": 13.94,
                        "cross_below": True,
                        "bar_end": "2026-04-28T14:45:00",
                    },
                ],
                "missing_symbols": [],
            },
        )()

    monkeypatch.setattr("api.services.realtime_monitor_service.evaluate_first_day_band_signals", fake_evaluate)
    monkeypatch.setattr("api.services.realtime_monitor_service._supplement_first_day_band_result", lambda **kwargs: None)

    result = realtime_monitor_service._build_minute_features(
        monitor,
        ["300520.SZ", "603118.SH"],
        minute_capture={
            "success": True,
            "rows": 20,
            "trade_date": today,
            "captured_symbols": ["300520.SZ", "603118.SH"],
            "missing_symbols": [],
            "symbol_rows": {"300520.SZ": 10, "603118.SH": 10},
            "symbol_latest_trade_times": {
                "300520.SZ": latest_bar.replace("T", " "),
                "603118.SH": latest_bar.replace("T", " "),
            },
        },
    )

    assert [item["symbol"] for item in result["items"]] == ["300520.SZ"]
    assert result["missing_symbols"] == ["603118.SH"]
    assert result["stale_symbols"] == ["603118.SH"]


def test_realtime_first_day_band_rejects_stale_qmt_capture_latest_time(monkeypatch):
    trade_date = "2026-05-07"
    current_time = datetime(2026, 5, 7, 10, 40, 30)
    monitor = SimpleNamespace(
        account_key="paper_sim",
        config_json={"signal_mode": "first_day_band", "signal_timeframe": "5m"},
    )
    captured_eval_symbols = {}

    def fake_evaluate(symbols, trade_date, timeframe="5m", **kwargs):
        captured_eval_symbols["symbols"] = list(symbols)
        return type(
            "MinuteResult",
            (),
            {
                "timeframe": timeframe,
                "source": "postgresql:market_stock_minute_kline",
                "items": [
                    {
                        "symbol": "300520.SZ",
                        "signal": "hold",
                        "close": 37.1,
                        "cross_above": False,
                        "cross_below": False,
                        "bar_end": "2026-05-07T10:40:00",
                    },
                    {
                        "symbol": "601136.SH",
                        "signal": "buy",
                        "close": 16.7,
                        "cross_above": True,
                        "cross_below": False,
                        "bar_end": "2026-05-07T10:25:00",
                    },
                ],
                "missing_symbols": [],
            },
        )()

    monkeypatch.setattr("api.services.realtime_monitor_service.evaluate_first_day_band_signals", fake_evaluate)
    monkeypatch.setattr("api.services.realtime_monitor_service._supplement_first_day_band_result", lambda **kwargs: None)

    result = realtime_monitor_service._build_minute_features(
        monitor,
        ["300520.SZ", "601136.SH"],
        current_time=current_time,
        minute_capture={
            "success": True,
            "rows": 60,
            "trade_date": trade_date,
            "captured_symbols": ["300520.SZ", "601136.SH"],
            "missing_symbols": [],
            "symbol_rows": {"300520.SZ": 40, "601136.SH": 20},
            "symbol_latest_trade_times": {
                "300520.SZ": "2026-05-07 10:40:00",
                "601136.SH": "2026-05-07 10:26:00",
            },
        },
    )

    assert captured_eval_symbols["symbols"] == ["300520.SZ"]
    assert [item["symbol"] for item in result["items"]] == ["300520.SZ"]
    assert result["capture_stale_symbols"] == ["601136.SH"]
    assert result["missing_symbols"] == ["601136.SH"]


def test_realtime_first_day_band_filters_unclosed_future_bar():
    items, missing, stale, incomplete = realtime_monitor_service._fresh_minute_items_for_trade_date(
        [
            {
                "symbol": "300520.SZ",
                "signal": "sell",
                "cross_below": True,
                "bar_end": "2026-05-07T10:10:00",
            }
        ],
        required_symbols=["300520.SZ"],
        trade_date="2026-05-07",
        timeframe="5m",
        current_time=datetime(2026, 5, 7, 10, 9, 23),
    )

    assert items == []
    assert missing == ["300520.SZ"]
    assert stale == []
    assert incomplete == ["300520.SZ"]


def test_realtime_first_day_band_filters_old_closed_bar_when_latest_required():
    items, missing, stale, incomplete = realtime_monitor_service._fresh_minute_items_for_trade_date(
        [
            {
                "symbol": "601136.SH",
                "signal": "buy",
                "cross_above": True,
                "bar_end": "2026-05-07T10:25:00",
            }
        ],
        required_symbols=["601136.SH"],
        trade_date="2026-05-07",
        timeframe="5m",
        current_time=datetime(2026, 5, 7, 10, 40, 23),
        require_latest_bar=True,
    )

    assert items == []
    assert missing == ["601136.SH"]
    assert stale == ["601136.SH"]
    assert incomplete == []


def test_qmt_minute_subscription_resolve_capture_symbols_has_no_200_cap(monkeypatch):
    configured_symbols = [f"6{idx:05d}" for idx in range(250)]
    monkeypatch.setattr(qmt_minute_subscription_service.watchlist_service, "list_watchlist", lambda db, user_id: [])
    monkeypatch.setattr(
        qmt_minute_subscription_service.qmt_virtual_account_service,
        "get_qmt_virtual_account_overview",
        lambda *args, **kwargs: {"positions": []},
    )

    resolved = qmt_minute_subscription_service._resolve_capture_symbols(
        object(),
        "tester",
        configured_symbols,
        account_key="paper_sim",
    )
    scope_key = qmt_minute_subscription_service._capture_scope_key(resolved)

    assert len(resolved) == 250
    assert scope_key.startswith("intraday_capture:count=250:")


def test_qmt_minute_subscription_refreshes_realtime_selection_after_capture(monkeypatch):
    qmt_minute_subscription_service._LAST_SELECTION_REFRESH_AT = None
    monkeypatch.setenv("AI_QUANT_MINUTE_CAPTURE_REFRESH_SELECTION", "1")
    monkeypatch.setenv("AI_QUANT_MINUTE_SELECTION_REFRESH_INTERVAL_SECONDS", "55")
    calls: list[dict[str, object]] = []

    class FakeDb:
        def rollback(self):
            calls.append({"rollback": True})

    fake_db = FakeDb()

    def fake_refresh(*args, **kwargs):
        raise AssertionError("minute capture should schedule selection refresh asynchronously")

    def fake_schedule(*, trigger, windows, limit, user_id=None, trade_date=None, reason=None, context=None):
        calls.append(
            {
                "trigger": trigger,
                "windows": tuple(windows),
                "limit": limit,
                "user_id": user_id,
                "trade_date": trade_date,
                "reason": reason,
                "context": context,
            }
        )
        return {
            "trigger": trigger,
            "windows": list(windows),
            "generated": [],
            "errors": [],
            "skipped": False,
            "status": "scheduled",
        }

    monkeypatch.setattr(catalyst_selection_service, "refresh_event_driven_selection", fake_refresh)
    monkeypatch.setattr(catalyst_selection_service, "schedule_event_driven_selection_refresh", fake_schedule)
    now = datetime(2026, 6, 1, 10, 0, tzinfo=timezone.utc)

    refreshed = qmt_minute_subscription_service._refresh_realtime_selection_after_capture(
        fake_db,
        user_id="user-1",
        now=now,
        capture_result={"success": True, "rows": 128},
    )
    debounced = qmt_minute_subscription_service._refresh_realtime_selection_after_capture(
        fake_db,
        user_id="user-1",
        now=now + timedelta(seconds=10),
        capture_result={"success": True, "rows": 88},
    )

    assert refreshed["status"] == "scheduled"
    assert refreshed["scheduled"] is True
    assert refreshed["generated_count"] == 0
    assert refreshed["capture_success"] is True
    assert refreshed["capture_rows"] == 128
    assert debounced == {"status": "skipped", "reason": "debounced"}
    assert calls == [
        {
            "trigger": "qmt-minute-subscription:intraday",
            "windows": ("24h",),
            "limit": 10,
            "user_id": "user-1",
            "trade_date": None,
            "reason": "minute_capture",
            "context": {
                "capture_success": True,
                "capture_rows": 128,
                "source": "qmt_minute_subscription",
            },
        }
    ]


def test_qmt_minute_subscription_selection_refresh_can_be_disabled(monkeypatch):
    qmt_minute_subscription_service._LAST_SELECTION_REFRESH_AT = None
    monkeypatch.setenv("AI_QUANT_MINUTE_CAPTURE_REFRESH_SELECTION", "0")

    result = qmt_minute_subscription_service._refresh_realtime_selection_after_capture(
        object(),
        user_id="user-1",
        now=datetime(2026, 6, 1, 10, 0, tzinfo=timezone.utc),
        capture_result={"success": True, "rows": 128},
    )

    assert result == {"status": "skipped", "reason": "disabled"}


def test_first_day_band_signals_ignore_position_side_limits():
    monitor = SimpleNamespace(
        config_json={"signal_mode": "first_day_band", "signal_timeframe": "5m"},
        risk_config_json={},
    )
    strategy = {
        "id": "strategy-1",
        "current_version": {"dsl": {"position": {"initial_position_pct": 0.2}}},
    }
    overview = {
        "positions": [
            {"symbol": "601136.SH", "current_position": 8000, "available_position": 0},
        ],
    }
    quotes = {
        "601136.SH": {"price": 16.72},
        "603118.SH": {"price": 15.3},
    }

    signals = realtime_monitor_service._generate_signals(
        monitor,
        strategy,
        overview,
        quotes,
        {
            "timeframe": "5m",
            "items": [
                {"symbol": "601136.SH", "signal": "buy", "close": 16.72, "cross_above": True},
                {"symbol": "603118.SH", "signal": "sell", "close": 15.3, "cross_below": True},
            ],
        },
    )

    assert [(item["symbol"], item["side"]) for item in signals] == [
        ("601136.SH", "buy"),
        ("603118.SH", "sell"),
    ]


def test_intraday_confirmation_signal_ignores_existing_position():
    monitor = SimpleNamespace(
        config_json={"signal_mode": "intraday_confirmation", "signal_timeframe": "30m"},
        risk_config_json={},
    )
    strategy = {
        "id": "strategy-1",
        "current_version": {"dsl": {"position": {"initial_position_pct": 0.2}}},
    }

    signals = realtime_monitor_service._generate_signals(
        monitor,
        strategy,
        {"positions": [{"symbol": "300520.SZ", "current_position": 100}]},
        {"300520.SZ": {"price": 37.4}},
        {"items": [{"symbol": "300520.SZ", "confirmed": True}]},
    )

    assert [(item["symbol"], item["side"]) for item in signals] == [("300520.SZ", "buy")]


def test_risk_check_keeps_only_execution_hard_limits_after_signal():
    class _NoQueryDb:
        def query(self, *args, **kwargs):
            raise AssertionError("risk_check should not query same-day order limits")

    monitor = SimpleNamespace(
        account_role="paper",
        live_trading_enabled=False,
        status="running",
        config_json={"allow_outside_session": True, "lot_size": 100},
        risk_config_json={"max_daily_orders": 0},
    )

    result = realtime_monitor_service._risk_check(
        _NoQueryDb(),
        monitor,
        {"symbol": "300520.SZ", "quantity": 100, "side": "buy"},
        {"source": "first_day_band_realtime"},
    )

    assert result["passed"] is True


def test_realtime_route_notify_only_does_not_submit_order(monkeypatch):
    client = _client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}
    strategy_id = _create_strategy(client, f"只提醒路线策略-{uuid4().hex[:6]}")
    _mock_common(monkeypatch, account_key="paper_sim", role="paper")
    monkeypatch.setattr("api.services.realtime_monitor_service._is_monitor_trading_window", lambda value: True)

    def fail_submit_order(*args, **kwargs):
        raise AssertionError("notify_only route should not submit an order")

    monkeypatch.setattr(
        "api.services.realtime_monitor_service.qmt_virtual_account_service.submit_qmt_order",
        fail_submit_order,
    )

    created = client.post(
        "/v1/realtime/monitors",
        headers=headers,
        json={
            "name": "新版路线只提醒",
            "strategy_id": strategy_id,
            "account_key": "paper_sim",
            "execution_mode": "auto",
            "monitor_pool": {"symbols": ["000001.SZ"]},
            "config": {
                "poll_interval_seconds": 20,
                "max_signals_per_cycle": 1,
                "signal_routes": [
                    {
                        "id": "buy-5m-notify",
                        "side": "buy",
                        "timeframe": "5m",
                        "strategy_id": strategy_id,
                        "signal_id": "dsl-3",
                        "action": "notify_only",
                        "priority": 20,
                    }
                ],
            },
        },
    )
    assert created.status_code == 200
    monitor_id = created.json()["id"]

    run_once = client.post(f"/v1/realtime/monitors/{monitor_id}/run-once", headers=headers)
    assert run_once.status_code == 200
    event_types = [item["event_type"] for item in run_once.json()["events"]]
    assert "signal_generated" in event_types
    assert "signal_notified" in event_types
    assert "order_intent" not in event_types
    assert "order_submitted" not in event_types

    with get_strategy_db_ctx() as db:
        row = db.query(RealtimeSignalExecutionDB).filter(RealtimeSignalExecutionDB.monitor_id == monitor_id).one()
        assert row.status == "notified"
        assert row.signal_identity_json["route_id"] == "buy-5m-notify"


def test_realtime_route_reduce_position_uses_configured_pct():
    monitor = SimpleNamespace(
        id="monitor-reduce-test",
        account_key="paper_sim",
        config_json={"lot_size": 100, "price_type": "opponent"},
        strategy_id="strategy-1",
        strategy_version_id="version-1",
    )
    overview = {
        "account": {"total_asset": 1_000_000.0, "available_cash": 900_000.0},
        "positions": [
            {"symbol": "300520.SZ", "current_position": 1000, "available_position": 1000},
        ],
    }

    reduce_intent = realtime_monitor_service._build_order_intent(
        monitor,
        overview,
        {
            "symbol": "300520.SZ",
            "side": "sell",
            "price": 35.0,
            "execution_action": "reduce_position",
            "sell_position_pct": 0.35,
            "route_id": "sell-30m",
        },
    )
    clear_intent = realtime_monitor_service._build_order_intent(
        monitor,
        overview,
        {
            "symbol": "300520.SZ",
            "side": "sell",
            "price": 35.0,
            "execution_action": "clear_position",
            "sell_position_pct": 1.0,
            "route_id": "sell-1d",
        },
    )

    assert reduce_intent["quantity"] == 300
    assert reduce_intent["execution_action"] == "reduce_position"
    assert clear_intent["quantity"] == 1000


def test_realtime_buy_route_pct_uses_available_cash_not_total_asset():
    monitor = SimpleNamespace(
        id="monitor-buy-sizing-test",
        account_key="paper_sim",
        config_json={"lot_size": 100, "price_type": "opponent", "buy_cash_buffer_pct": 0, "buy_price_buffer_pct": 0},
        risk_config_json={"max_single_position_pct": 0.2},
        strategy_id="strategy-1",
        strategy_version_id="version-1",
    )
    overview = {
        "account": {"total_asset": 7_806_027.8, "available_cash": 1_131_480.8},
        "positions": [],
    }

    intent = realtime_monitor_service._build_order_intent(
        monitor,
        overview,
        {
            "symbol": "601136.SH",
            "side": "buy",
            "price": 13.59,
            "execution_action": "buy_or_add",
            "target_position_pct": 0.2,
            "buy_cash_pct": 0.2,
            "route_id": "buy-5m",
        },
    )

    assert intent["quantity"] == 16600
    assert intent["sizing"]["mode"] == "available_cash_pct"
    assert intent["sizing"]["cash_budget"] == 226296.16


def test_realtime_buy_route_respects_single_position_cap():
    monitor = SimpleNamespace(
        id="monitor-buy-cap-test",
        account_key="paper_sim",
        config_json={"lot_size": 100, "price_type": "opponent", "buy_cash_buffer_pct": 0, "buy_price_buffer_pct": 0},
        risk_config_json={"max_single_position_pct": 0.2},
        strategy_id="strategy-1",
        strategy_version_id="version-1",
    )
    overview = {
        "account": {"total_asset": 1_000_000.0, "available_cash": 500_000.0},
        "positions": [
            {"symbol": "300520.SZ", "current_position": 18000, "available_position": 18000, "market_value": 180_000.0},
        ],
    }

    intent = realtime_monitor_service._build_order_intent(
        monitor,
        overview,
        {
            "symbol": "300520.SZ",
            "side": "buy",
            "price": 10.0,
            "execution_action": "buy_or_add",
            "target_position_pct": 0.5,
            "buy_cash_pct": 0.5,
            "route_id": "buy-30m",
        },
    )

    assert intent["quantity"] == 2000
    assert intent["sizing"]["max_position_cash"] == 20000.0


def test_realtime_signal_identity_includes_route_and_action():
    monitor = SimpleNamespace(
        id="monitor-identity-test",
        account_key="paper_sim",
        strategy_id="default-strategy",
        strategy_version_id="default-version",
    )
    base_signal = {
        "symbol": "300520.SZ",
        "side": "buy",
        "price": 35.0,
        "source": "intraday_confirmation_route",
        "timeframe": "5m",
        "bar_end": "2026-06-03T14:40:00",
        "strategy_id": "route-strategy",
        "strategy_version_id": "route-version",
        "signal_id": "dsl-1",
    }

    buy_key = realtime_monitor_service._signal_request_id(
        monitor,
        {**base_signal, "route_id": "buy-5m", "execution_action": "buy_or_add"},
    )
    notify_key = realtime_monitor_service._signal_request_id(
        monitor,
        {**base_signal, "route_id": "buy-5m-notify", "execution_action": "notify_only"},
    )

    assert buy_key != notify_key
    identity = realtime_monitor_service._signal_identity_payload(
        monitor,
        {**base_signal, "route_id": "buy-5m", "execution_action": "buy_or_add"},
    )
    assert identity["strategy_id"] == "route-strategy"
    assert identity["route_id"] == "buy-5m"
    assert identity["execution_action"] == "buy_or_add"


def test_realtime_route_conflict_prefers_daily_clear_position():
    resolved = realtime_monitor_service._resolve_signal_route_conflicts(
        [
            {
                "symbol": "300520.SZ",
                "side": "buy",
                "timeframe": "5m",
                "execution_action": "buy_or_add",
                "priority": 20,
            },
            {
                "symbol": "300520.SZ",
                "side": "sell",
                "timeframe": "1d",
                "execution_action": "clear_position",
                "priority": 100,
            },
        ]
    )

    assert len(resolved) == 1
    assert resolved[0]["side"] == "sell"
    assert resolved[0]["execution_action"] == "clear_position"


def test_run_monitor_once_skips_outside_trading_session(monkeypatch):
    client = _client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}
    strategy_id = _create_strategy(client, f"非交易时段策略-{uuid4().hex[:6]}")
    _mock_common(monkeypatch, account_key="paper_sim", role="paper")
    monkeypatch.setattr("api.services.realtime_monitor_service._is_monitor_trading_window", lambda value: False)
    monkeypatch.setattr(
        "api.services.realtime_monitor_service.qmt_virtual_account_service.get_qmt_virtual_account_overview",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("should not fetch overview")),
    )

    created = client.post(
        "/v1/realtime/monitors",
        headers=headers,
        json={
            "name": "非交易时段跳过测试",
            "strategy_id": strategy_id,
            "account_key": "paper_sim",
            "execution_mode": "auto",
            "monitor_pool": {"symbols": ["300520.SZ"]},
        },
    )
    assert created.status_code == 200
    monitor_id = created.json()["id"]

    run_once = client.post(f"/v1/realtime/monitors/{monitor_id}/run-once", headers=headers)
    assert run_once.status_code == 200
    event_types = [item["event_type"] for item in run_once.json()["events"]]
    assert "manual_cycle_requested" in event_types
    assert "cycle_skipped" in event_types
    assert "market_snapshot" not in event_types
    assert "minute_capture" not in event_types
    assert "minute_features" not in event_types
    assert "signal_generated" not in event_types
    assert "order_submitted" not in event_types


def test_run_monitor_once_keeps_running_when_qmt_overview_is_stale(monkeypatch):
    client = _client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}
    strategy_id = _create_strategy(client, f"禁用缓存快照策略-{uuid4().hex[:6]}")
    _mock_common(monkeypatch, account_key="paper_sim", role="paper")
    monkeypatch.setattr("api.services.realtime_monitor_service._is_monitor_trading_window", lambda value: True)
    monkeypatch.setattr(
        "api.services.realtime_monitor_service.qmt_virtual_account_service.get_qmt_virtual_account_overview",
        lambda *args, **kwargs: {
            "account": {"total_asset": 1_000_000.0, "available_cash": 900_000.0},
            "positions": [{"symbol": "300520.SZ", "available_position": 100}],
            "orders": [],
            "trades": [],
            "connection": {"account_key": "paper_sim", "connected": True, "message": "已回退到最近快照"},
            "fetched_at": datetime.now().isoformat(),
            "data_source": "cache",
            "is_stale": True,
        },
    )

    created = client.post(
        "/v1/realtime/monitors",
        headers=headers,
        json={
            "name": "缓存快照熔断测试",
            "strategy_id": strategy_id,
            "account_key": "paper_sim",
            "execution_mode": "auto",
            "monitor_pool": {"symbols": ["300520.SZ"]},
        },
    )
    assert created.status_code == 200
    monitor_id = created.json()["id"]

    run_once = client.post(f"/v1/realtime/monitors/{monitor_id}/run-once", headers=headers)
    assert run_once.status_code == 200
    payload = run_once.json()
    event_types = [item["event_type"] for item in payload["events"]]
    assert payload["monitor"]["status"] == "ready"
    assert "monitor_interrupted" in event_types
    assert "monitor_fused" not in event_types
    assert "market_snapshot" not in event_types
    assert "order_submitted" not in event_types


def test_qmt_recovered_auto_resumes_legacy_fuse_reset_pause(monkeypatch):
    _mock_common(monkeypatch, account_key="paper_sim", role="paper")
    monitor_id = _create_paused_qmt_recovery_monitor()

    with get_strategy_db_ctx() as strategy_db, get_db_ctx() as main_db:
        monitor = strategy_db.query(RealtimeMonitorDB).filter(RealtimeMonitorDB.id == monitor_id).one()
        assert realtime_monitor_service._try_auto_resume_qmt_monitor(strategy_db, main_db, monitor) is True
        strategy_db.commit()

    with get_strategy_db_ctx() as db:
        monitor = db.query(RealtimeMonitorDB).filter(RealtimeMonitorDB.id == monitor_id).one()
        assert monitor.status == "running"
        event_types = [
            row.event_type
            for row in db.query(RealtimeEventDB)
            .filter(RealtimeEventDB.monitor_id == monitor_id)
            .order_by(RealtimeEventDB.created_at)
            .all()
        ]
        assert "monitor_auto_resumed" in event_types


def test_qmt_auto_resume_does_not_override_manual_pause(monkeypatch):
    _mock_common(monkeypatch, account_key="paper_sim", role="paper")
    monitor_id = _create_paused_qmt_recovery_monitor(append_manual_pause=True)

    with get_strategy_db_ctx() as strategy_db, get_db_ctx() as main_db:
        monitor = strategy_db.query(RealtimeMonitorDB).filter(RealtimeMonitorDB.id == monitor_id).one()
        assert realtime_monitor_service._try_auto_resume_qmt_monitor(strategy_db, main_db, monitor) is False
        strategy_db.commit()

    with get_strategy_db_ctx() as db:
        monitor = db.query(RealtimeMonitorDB).filter(RealtimeMonitorDB.id == monitor_id).one()
        assert monitor.status == "paused"
        state = monitor.state_json or {}
        assert "qmt_auto_resume" not in state


def test_qmt_order_submit_exception_records_order_error_without_interrupt(monkeypatch):
    client = _client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}
    strategy_id = _create_strategy(client, f"下单异常不中断策略-{uuid4().hex[:6]}")
    account_key = f"paper_order_error_{uuid4().hex[:8]}"
    _mock_common(monkeypatch, account_key=account_key, role="paper")
    monkeypatch.setattr("api.services.realtime_monitor_service._is_monitor_trading_window", lambda value: True)

    def raise_submit_error(*args, **kwargs):
        raise RuntimeError("bridge timeout")

    monkeypatch.setattr("api.services.qmt_virtual_account_service.submit_qmt_order", raise_submit_error)

    created = client.post(
        "/v1/realtime/monitors",
        headers=headers,
        json={
            "name": "下单异常不中断",
            "strategy_id": strategy_id,
            "account_key": account_key,
            "execution_mode": "auto",
            "monitor_pool": {"symbols": ["000001.SZ"]},
            "config": {"poll_interval_seconds": 10},
        },
    )
    assert created.status_code == 200
    monitor_id = created.json()["id"]

    run_once = client.post(f"/v1/realtime/monitors/{monitor_id}/run-once", headers=headers)
    assert run_once.status_code == 200
    payload = run_once.json()
    event_types = [item["event_type"] for item in payload["events"]]
    assert "order_error" in event_types
    assert "monitor_interrupted" not in event_types
    assert "monitor_fused" not in event_types
    assert payload["monitor"]["status"] == "ready"

    order_error = next(item for item in payload["events"] if item["event_type"] == "order_error")
    assert order_error["error_payload"]["recoverable"] is True
    assert order_error["error_payload"]["interrupted"] is False
    assert "QMT 交易接口异常" in order_error["error_payload"]["error"]


def test_run_monitor_once_replays_positions_and_auto_replaces_stale_order(monkeypatch):
    client = _client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}
    strategy_id = _create_strategy(client, f"撤单补单策略-{uuid4().hex[:6]}")
    _mock_common(monkeypatch, account_key="paper_sim", role="paper")
    monkeypatch.setattr("api.services.realtime_monitor_service._is_monitor_trading_window", lambda value: True)

    orders_state: dict[str, dict] = {}
    positions_state: list[dict] = []
    submit_count = {"value": 0}

    def overview(db, user_id, account_key=None, **kwargs):
        return {
            "account": {
                "account_id": "39027628",
                "total_asset": 1_000_000.0,
                "available_cash": 900_000.0,
                "cash": 900_000.0,
            },
            "positions": list(positions_state),
            "orders": list(orders_state.values()),
            "trades": [],
            "connection": {"account_key": account_key or "paper_sim", "connected": True},
            "fetched_at": "2026-04-23T10:00:00+08:00",
            "data_source": "live",
            "is_stale": False,
        }

    def submit_order(db, user_id, **kwargs):
        submit_count["value"] += 1
        order_id = f"mock-{submit_count['value']}-{kwargs['symbol']}"
        orders_state[order_id] = {
            "order_id": order_id,
            "symbol": kwargs["symbol"],
            "side": kwargs["side"],
            "status": "submitted",
            "can_cancel": True,
            "quantity": kwargs["quantity"],
            "filled_quantity": 0,
            "price_type": kwargs.get("price_type"),
        }
        return {
            "message": "QMT 委托已提交",
            "account_key": kwargs.get("account_key"),
            "request_id": f"request-{submit_count['value']}",
            "order_result": {"success": True, "order_id": order_id},
            "overview": overview(db, user_id, account_key=kwargs.get("account_key")),
        }

    def cancel_order(db, user_id, *, account_key=None, order_id):
        orders_state[order_id] = {
            **orders_state[order_id],
            "status": "cancelled",
            "can_cancel": False,
        }
        return {
            "message": "QMT 撤单请求已提交",
            "account_key": account_key,
            "request_id": "cancel-request",
            "cancel_result": {"success": True, "order_id": order_id},
            "overview": overview(db, user_id, account_key=account_key),
        }

    monkeypatch.setattr(
        "api.services.realtime_monitor_service.qmt_virtual_account_service.get_qmt_virtual_account_overview",
        overview,
    )
    monkeypatch.setattr(
        "api.services.realtime_monitor_service.qmt_virtual_account_service.submit_qmt_order",
        submit_order,
    )
    monkeypatch.setattr(
        "api.services.realtime_monitor_service.qmt_virtual_account_service.cancel_qmt_order",
        cancel_order,
    )

    created = client.post(
        "/v1/realtime/monitors",
        headers=headers,
        json={
            "name": "撤单补单测试",
            "strategy_id": strategy_id,
            "account_key": "paper_sim",
            "execution_mode": "auto",
            "monitor_pool": {"symbols": ["000001.SZ"]},
            "config": {"poll_interval_seconds": 20, "max_signals_per_cycle": 1, "cancel_after_seconds": 1, "max_replace_attempts": 1},
        },
    )
    assert created.status_code == 200
    monitor_id = created.json()["id"]

    first_run = client.post(f"/v1/realtime/monitors/{monitor_id}/run-once", headers=headers)
    assert first_run.status_code == 200
    assert submit_count["value"] == 1

    with get_strategy_db_ctx() as db:
        monitor = db.query(RealtimeMonitorDB).filter(RealtimeMonitorDB.id == monitor_id).first()
        state = deepcopy(monitor.state_json or {})
        tracker = deepcopy(state.get("execution_tracker") or {})
        pending = deepcopy(tracker.get("pending_orders") or {})
        assert pending
        for item in pending.values():
            item["submitted_at"] = "2000-01-01T00:00:00+00:00"
        tracker["pending_orders"] = pending
        state["execution_tracker"] = tracker
        monitor.state_json = state
        flag_modified(monitor, "state_json")
        db.add(monitor)
        db.commit()

    positions_state.append(
        {
            "symbol": "000001.SZ",
            "name": "平安银行",
            "current_position": 100,
            "available_position": 100,
            "market_value": 1050.0,
            "average_cost": 10.0,
        }
    )
    second_run = client.post(f"/v1/realtime/monitors/{monitor_id}/run-once", headers=headers)
    assert second_run.status_code == 200
    assert submit_count["value"] == 2

    event_types = [item["event_type"] for item in second_run.json()["events"]]
    assert "order_cancel_requested" in event_types
    assert "order_cancelled" in event_types
    assert "order_replace_requested" in event_types
    assert "signal_deduplicated" not in event_types
    assert "position_changed" in event_types
    summary = second_run.json()["monitor"]["state"]["execution_tracker_summary"]
    assert summary["pending_orders"] == 1


def test_execution_refresh_attaches_signal_context_to_trade_and_position():
    user_id = f"execution-context-{uuid4().hex}"
    strategy_id = uuid4().hex
    monitor_id = uuid4().hex
    order_id = "1082170437"
    signal_payload = {
        "symbol": "603118.SH",
        "side": "buy",
        "timeframe": "5m",
        "signal_name": "first_day_band_5m_买点规则 1",
        "signal_key": "sig-5m",
        "price": 11.89,
    }
    order_intent = {
        "symbol": "603118.SH",
        "side": "buy",
        "quantity": 18400,
        "reference_price": 11.89,
        "signal_key": "sig-5m",
        "signal_timeframe": "5m",
        "signal_bar_end": "2026-06-11T14:50:00",
        "signal_name": "first_day_band_5m_买点规则 1",
        "signal_id": "buy-rule",
        "signal_reason": "first_day_band_5m_golden_cross",
    }
    with get_strategy_db_ctx() as db:
        db.add(
            StrategyDB(
                id=strategy_id,
                name="事件链上下文测试策略",
                strategy_type=StrategyType.TRADING,
                status=StrategyStatus.ACTIVE,
                is_active=True,
            )
        )
        monitor = RealtimeMonitorDB(
            id=monitor_id,
            user_id=user_id,
            name="事件链上下文测试",
            account_key="paper_sim",
            account_role="paper",
            strategy_id=strategy_id,
            status="running",
            execution_mode="auto",
            auto_trade_enabled=True,
            quote_source="qmt",
            monitor_pool_json={"symbols": ["603118.SH"], "resolved_symbols": ["603118.SH"]},
            config_json={"poll_interval_seconds": 20},
            risk_config_json={},
            state_json={
                "execution_tracker": {
                    "initialized": True,
                    "pending_orders": {
                        order_id: {
                            "order_id": order_id,
                            "symbol": "603118.SH",
                            "side": "buy",
                            "quantity": 18400,
                            "submitted_at": "2026-06-11T14:50:13+08:00",
                            "replace_attempts": 0,
                            "order_intent": order_intent,
                            "signal_payload": signal_payload,
                            "risk_payload": {"passed": True},
                        }
                    },
                    "last_orders": {},
                    "seen_trade_ids": [],
                    "last_positions": {},
                }
            },
            updated_at=realtime_monitor_service._now_dt(),
        )
        db.add(monitor)
        db.flush()

        realtime_monitor_service._refresh_execution_state(
            db,
            db,
            monitor,
            {
                "orders": [],
                "trades": [
                    {
                        "trade_id": "trade-1082170437",
                        "order_id": order_id,
                        "symbol": "603118.SH",
                        "side": "buy",
                        "quantity": 18400,
                        "price": 11.89,
                        "trade_time": "2026-06-11 14:50:34",
                    }
                ],
                "positions": [
                    {
                        "symbol": "603118.SH",
                        "name": "共进股份",
                        "current_position": 18400,
                        "available_position": 18400,
                        "market_value": 218776.0,
                        "average_cost": 11.89,
                    }
                ],
            },
            correlation_id="cycle-context",
        )
        db.commit()

        rows = (
            db.query(RealtimeEventDB)
            .filter(RealtimeEventDB.monitor_id == monitor_id)
            .order_by(RealtimeEventDB.created_at.asc())
            .all()
        )
        trade_event = next(item for item in rows if item.event_type == "trade_confirmed")
        position_event = next(item for item in rows if item.event_type == "position_changed")
        assert trade_event.signal_payload["timeframe"] == "5m"
        assert trade_event.signal_payload["signal_name"] == "first_day_band_5m_买点规则 1"
        assert trade_event.order_payload["signal_timeframe"] == "5m"
        assert position_event.signal_payload["timeframe"] == "5m"
        assert position_event.signal_payload["signal_name"] == "first_day_band_5m_买点规则 1"
        assert position_event.order_payload["signal_timeframe"] == "5m"


def test_activity_events_hide_signal_deduplicated_rows():
    user_id = f"dedupe-activity-{uuid4().hex}"
    strategy_id = uuid4().hex
    monitor_id = uuid4().hex
    with get_strategy_db_ctx() as db:
        db.add(
            StrategyDB(
                id=strategy_id,
                name="重复信号活动测试策略",
                strategy_type=StrategyType.TRADING,
                status=StrategyStatus.ACTIVE,
                is_active=True,
            )
        )
        db.add(
            RealtimeMonitorDB(
                id=monitor_id,
                user_id=user_id,
                name="重复信号活动测试",
                account_key="paper_sim",
                account_role="paper",
                strategy_id=strategy_id,
                status="running",
                execution_mode="auto",
                auto_trade_enabled=True,
                quote_source="qmt",
                monitor_pool_json={},
                config_json={},
                risk_config_json={},
                state_json={},
            )
        )
        db.flush()
        realtime_monitor_service._append_event(
            db,
            db.query(RealtimeMonitorDB).filter(RealtimeMonitorDB.id == monitor_id).one(),
            "signal_deduplicated",
            payload={
                "cycle_id": "cycle-1",
                "trigger_source": "worker",
                "suppressed_count": 2,
                "reason": "same_signal_already_processed_in_bar",
            },
        )
        realtime_monitor_service._append_event(
            db,
            db.query(RealtimeMonitorDB).filter(RealtimeMonitorDB.id == monitor_id).one(),
            "signal_generated",
            symbol="000001.SZ",
            signal_payload={
                "symbol": "000001.SZ",
                "side": "buy",
                "timeframe": "5m",
                "signal_key": "sig-test",
            },
        )
        db.commit()

        rows = realtime_monitor_service.list_events(db, user_id, monitor_id, limit=20, activity_only=True)
        event_types = [row["event_type"] for row in rows]
        assert "signal_deduplicated" not in event_types
        assert "signal_generated" in event_types


def test_realtime_monitor_performance_compares_strategy_to_hold_baseline(monkeypatch):
    user_id = f"performance-{uuid4().hex}"
    strategy_id = uuid4().hex
    monitor_id = uuid4().hex
    account_key = f"paper_perf_{uuid4().hex[:8]}"
    with get_strategy_db_ctx() as db:
        db.add(
            StrategyDB(
                id=strategy_id,
                name="收益统计测试策略",
                strategy_type=StrategyType.TRADING,
                status=StrategyStatus.ACTIVE,
                is_active=True,
            )
        )
        monitor = RealtimeMonitorDB(
            id=monitor_id,
            user_id=user_id,
            name="收益统计测试",
            account_key=account_key,
            account_role="paper",
            strategy_id=strategy_id,
            status="running",
            execution_mode="auto",
            auto_trade_enabled=True,
            quote_source="qmt",
            monitor_pool_json={"resolved_symbols": ["000001.SZ", "000002.SZ"]},
            config_json={},
            risk_config_json={},
            state_json={
                "performance_baseline": {
                    "captured_at": "2026-06-15T09:30:00+08:00",
                    "account": {"total_asset": 100000.0, "available_cash": 40000.0},
                    "positions": [
                        {
                            "symbol": "000001.SZ",
                            "name": "平安银行",
                            "current_position": 1000,
                            "current_price": 10.0,
                            "market_value": 10000.0,
                        },
                        {
                            "symbol": "000002.SZ",
                            "name": "万科A",
                            "current_position": 2000,
                            "current_price": 25.0,
                            "market_value": 50000.0,
                        },
                    ],
                    "start_total_asset": 100000.0,
                    "start_cash": 40000.0,
                    "start_market_value": 60000.0,
                }
            },
            updated_at=realtime_monitor_service._now_dt(),
        )
        db.add(monitor)
        realtime_monitor_service._append_event(
            db,
            monitor,
            "trade_confirmed",
            symbol="000001.SZ",
            broker_result={
                "trade_id": "performance-sell-000001",
                "symbol": "000001.SZ",
                "side": "sell",
                "quantity": 500,
                "price": 10.0,
            },
        )
        realtime_monitor_service._append_event(
            db,
            monitor,
            "trade_confirmed",
            symbol="000002.SZ",
            broker_result={
                "trade_id": "performance-buy-000002",
                "symbol": "000002.SZ",
                "side": "buy",
                "quantity": 1000,
                "price": 25.0,
            },
        )
        realtime_monitor_service._append_event(
            db,
            monitor,
            "trade_confirmed",
            symbol="000003.SZ",
            broker_result={
                "trade_id": "performance-buy-000003",
                "symbol": "000003.SZ",
                "side": "buy",
                "quantity": 100,
                "price": 20.0,
            },
        )
        db.commit()

    monkeypatch.setattr(
        "api.services.realtime_monitor_service.qmt_virtual_account_service.get_qmt_virtual_account_overview",
        lambda db, user_id, account_key=None, **kwargs: {
            "account": {
                "account_key": account_key,
                "total_asset": 103500.0,
                "available_cash": 23500.0,
                "market_value": 80000.0,
            },
            "positions": [
                {
                    "symbol": "000001.SZ",
                    "name": "平安银行",
                    "current_position": 500,
                    "current_price": 12.0,
                    "market_value": 6000.0,
                    "total_pnl": 1000.0,
                },
                {
                    "symbol": "000002.SZ",
                    "name": "万科A",
                    "current_position": 3000,
                    "current_price": 24.0,
                    "market_value": 72000.0,
                    "total_pnl": -3000.0,
                },
                {
                    "symbol": "000003.SZ",
                    "name": "新买股票",
                    "current_position": 100,
                    "current_price": 20.0,
                    "market_value": 2000.0,
                    "total_pnl": 0.0,
                },
            ],
            "connection": {"connected": True},
            "data_source": "live",
            "is_stale": False,
            "fetched_at": "2026-06-15T10:30:00+08:00",
        },
    )

    with get_strategy_db_ctx() as strategy_db, get_db_ctx() as main_db:
        result = realtime_monitor_service.get_performance(strategy_db, main_db, user_id, monitor_id)

    assert result["start_total_asset"] == 60000.0
    assert result["strategy"]["total_asset"] == 103500.0
    assert result["strategy"]["pnl"] == -2000.0
    assert result["hold_baseline"]["total_asset"] == 60000.0
    assert result["hold_baseline"]["pnl"] == 0.0
    assert result["excess"]["pnl"] == -2000.0
    by_symbol = {item["symbol"]: item for item in result["symbols"]}
    assert by_symbol["000001.SZ"]["hold_pnl"] == 2000.0
    assert by_symbol["000001.SZ"]["strategy_pnl"] == 1000.0
    assert by_symbol["000001.SZ"]["excess_pnl"] == -1000.0
    assert by_symbol["000001.SZ"]["strategy_market_value"] == 6000.0
    assert by_symbol["000002.SZ"]["hold_pnl"] == -2000.0
    assert by_symbol["000002.SZ"]["strategy_pnl"] == -3000.0
    assert by_symbol["000002.SZ"]["excess_pnl"] == -1000.0
    assert by_symbol["000003.SZ"]["baseline_quantity"] == 0.0
    assert by_symbol["000003.SZ"]["strategy_pnl"] == 0.0
    assert by_symbol["000003.SZ"]["hold_pnl"] == 0.0
    assert by_symbol["000003.SZ"]["excess_pnl"] == 0.0


def test_realtime_monitor_performance_symbol_strategy_pnl_uses_trade_cashflow(monkeypatch):
    user_id = f"performance-cashflow-{uuid4().hex}"
    strategy_id = uuid4().hex
    monitor_id = uuid4().hex
    account_key = f"paper_perf_{uuid4().hex[:8]}"
    with get_strategy_db_ctx() as db:
        db.add(
            StrategyDB(
                id=strategy_id,
                name="收益现金流测试策略",
                strategy_type=StrategyType.TRADING,
                status=StrategyStatus.ACTIVE,
                is_active=True,
            )
        )
        monitor = RealtimeMonitorDB(
            id=monitor_id,
            user_id=user_id,
            name="收益现金流测试",
            account_key=account_key,
            account_role="paper",
            strategy_id=strategy_id,
            status="running",
            execution_mode="auto",
            auto_trade_enabled=True,
            quote_source="qmt",
            monitor_pool_json={"resolved_symbols": ["000001.SZ"]},
            config_json={},
            risk_config_json={},
            state_json={
                "performance_baseline": {
                    "captured_at": "2026-06-15T09:30:00+08:00",
                    "positions": [
                        {
                            "symbol": "000001.SZ",
                            "name": "平安银行",
                            "current_position": 1000,
                            "current_price": 10.0,
                            "market_value": 10000.0,
                        },
                    ],
                    "start_total_asset": 100000.0,
                    "start_cash": 90000.0,
                    "start_market_value": 10000.0,
                }
            },
            updated_at=realtime_monitor_service._now_dt(),
        )
        db.add(monitor)
        realtime_monitor_service._append_event(
            db,
            monitor,
            "trade_confirmed",
            symbol="000001.SZ",
            broker_result={
                "trade_id": "trade-sell-1",
                "symbol": "000001.SZ",
                "side": "sell",
                "quantity": 500,
                "price": 12.0,
            },
        )
        db.commit()

    monkeypatch.setattr(
        "api.services.realtime_monitor_service.qmt_virtual_account_service.get_qmt_virtual_account_overview",
        lambda db, user_id, account_key=None, **kwargs: {
            "account": {
                "account_key": account_key,
                "total_asset": 102000.0,
                "available_cash": 96000.0,
                "market_value": 6000.0,
            },
            "positions": [
                {
                    "symbol": "000001.SZ",
                    "name": "平安银行",
                    "current_position": 500,
                    "current_price": 12.0,
                    "market_value": 6000.0,
                    "total_pnl": 1000.0,
                },
            ],
            "connection": {"connected": True},
            "data_source": "live",
            "is_stale": False,
            "fetched_at": "2026-06-15T10:30:00+08:00",
        },
    )

    with get_strategy_db_ctx() as strategy_db, get_db_ctx() as main_db:
        result = realtime_monitor_service.get_performance(strategy_db, main_db, user_id, monitor_id)

    row = {item["symbol"]: item for item in result["symbols"]}["000001.SZ"]
    assert row["strategy_pnl"] == 2000.0
    assert row["hold_pnl"] == 2000.0
    assert row["excess_pnl"] == 0.0
    assert row["trade_buy_amount"] == 0.0
    assert row["trade_sell_amount"] == 6000.0


def test_realtime_monitor_performance_today_new_buy_not_double_counted(monkeypatch):
    user_id = f"performance-today-buy-{uuid4().hex}"
    strategy_id = uuid4().hex
    monitor_id = uuid4().hex
    account_key = f"paper_perf_{uuid4().hex[:8]}"
    with get_strategy_db_ctx() as db:
        db.add(
            StrategyDB(
                id=strategy_id,
                name="今日买入收益测试策略",
                strategy_type=StrategyType.TRADING,
                status=StrategyStatus.ACTIVE,
                is_active=True,
            )
        )
        monitor = RealtimeMonitorDB(
            id=monitor_id,
            user_id=user_id,
            name="今日买入收益测试",
            account_key=account_key,
            account_role="paper",
            strategy_id=strategy_id,
            status="running",
            execution_mode="auto",
            auto_trade_enabled=True,
            quote_source="qmt",
            monitor_pool_json={"resolved_symbols": ["300245.SZ"]},
            config_json={},
            risk_config_json={},
            state_json={
                "performance_baseline": {
                    "captured_at": "2026-06-15T11:30:00+08:00",
                    "positions": [
                        {
                            "symbol": "300245.SZ",
                            "name": "ST天玑",
                            "current_position": 15900,
                            "current_price": 5.97,
                            "market_value": 94923.0,
                        },
                    ],
                    "start_total_asset": 100000.0,
                    "start_cash": 5077.0,
                    "start_market_value": 94923.0,
                }
            },
            updated_at=realtime_monitor_service._now_dt(),
        )
        db.add(monitor)
        realtime_monitor_service._append_event(
            db,
            monitor,
            "trade_confirmed",
            symbol="300245.SZ",
            broker_result={
                "trade_id": "today-buy-300245",
                "symbol": "300245.SZ",
                "side": "buy",
                "quantity": 15900,
                "price": 5.86,
                "amount": 93174.0,
                "trade_time": "2026-06-15T09:29:59+08:00",
            },
        )
        db.commit()

    monkeypatch.setattr(
        "api.services.realtime_monitor_service.qmt_virtual_account_service.get_qmt_virtual_account_overview",
        lambda db, user_id, account_key=None, **kwargs: {
            "account": {
                "account_key": account_key,
                "total_asset": 101272.0,
                "available_cash": 6826.0,
                "market_value": 94446.0,
            },
            "positions": [
                {
                    "symbol": "300245.SZ",
                    "name": "ST天玑",
                    "current_position": 15900,
                    "current_price": 5.94,
                    "market_value": 94446.0,
                    "average_cost": 5.86,
                    "previous_close": 5.76,
                    "yesterday_position": 0,
                    "total_pnl": 1272.0,
                    "today_pnl": 0.0,
                },
            ],
            "connection": {"connected": True},
            "data_source": "live",
            "is_stale": False,
            "fetched_at": "2026-06-15T14:30:00+08:00",
        },
    )

    with get_strategy_db_ctx() as strategy_db, get_db_ctx() as main_db:
        result = realtime_monitor_service.get_performance(strategy_db, main_db, user_id, monitor_id)

    row = {item["symbol"]: item for item in result["symbols"]}["300245.SZ"]
    assert row["baseline_quantity"] == 0.0
    assert row["hold_pnl"] == 0.0
    assert row["strategy_pnl"] == 1272.0
    assert row["excess_pnl"] == 1272.0
    assert row["trade_buy_amount"] == 93174.0


def test_realtime_monitor_performance_today_sell_reports_realized_and_excess(monkeypatch):
    user_id = f"performance-today-sell-{uuid4().hex}"
    strategy_id = uuid4().hex
    monitor_id = uuid4().hex
    account_key = f"paper_perf_{uuid4().hex[:8]}"
    with get_strategy_db_ctx() as db:
        db.add(
            StrategyDB(
                id=strategy_id,
                name="今日卖出收益测试策略",
                strategy_type=StrategyType.TRADING,
                status=StrategyStatus.ACTIVE,
                is_active=True,
            )
        )
        monitor = RealtimeMonitorDB(
            id=monitor_id,
            user_id=user_id,
            name="今日卖出收益测试",
            account_key=account_key,
            account_role="paper",
            strategy_id=strategy_id,
            status="running",
            execution_mode="auto",
            auto_trade_enabled=True,
            quote_source="qmt",
            monitor_pool_json={"resolved_symbols": ["601136.SH"]},
            config_json={},
            risk_config_json={},
            state_json={
                "performance_baseline": {
                    "captured_at": "2026-06-15T11:30:00+08:00",
                    "positions": [
                        {
                            "symbol": "601136.SH",
                            "name": "首创证券",
                            "current_position": 7500,
                            "current_price": 14.29,
                            "market_value": 107175.0,
                        },
                    ],
                    "start_total_asset": 200000.0,
                    "start_cash": 92825.0,
                    "start_market_value": 107175.0,
                }
            },
            updated_at=realtime_monitor_service._now_dt(),
        )
        db.add(monitor)
        realtime_monitor_service._append_event(
            db,
            monitor,
            "trade_confirmed",
            symbol="601136.SH",
            broker_result={
                "trade_id": "today-sell-601136",
                "symbol": "601136.SH",
                "side": "sell",
                "quantity": 300,
                "price": 14.36,
                "amount": 4308.0,
                "trade_time": "2026-06-15T14:29:49+08:00",
            },
        )
        db.commit()

    monkeypatch.setattr(
        "api.services.realtime_monitor_service.qmt_virtual_account_service.get_qmt_virtual_account_overview",
        lambda db, user_id, account_key=None, **kwargs: {
            "account": {
                "account_key": account_key,
                "total_asset": 201035.0,
                "available_cash": 97133.0,
                "market_value": 103902.0,
            },
            "positions": [
                {
                    "symbol": "601136.SH",
                    "name": "首创证券",
                    "current_position": 7200,
                    "current_price": 14.43,
                    "market_value": 103896.0,
                    "average_cost": 13.31,
                    "previous_close": 14.08,
                    "yesterday_position": 7500,
                },
            ],
            "connection": {"connected": True},
            "data_source": "live",
            "is_stale": False,
            "fetched_at": "2026-06-15T14:35:00+08:00",
        },
    )

    with get_strategy_db_ctx() as strategy_db, get_db_ctx() as main_db:
        result = realtime_monitor_service.get_performance(strategy_db, main_db, user_id, monitor_id)

    row = {item["symbol"]: item for item in result["symbols"]}["601136.SH"]
    assert row["baseline_quantity"] == 7500.0
    assert row["strategy_pnl"] == 2604.0
    assert row["hold_pnl"] == 2625.0
    assert row["excess_pnl"] == -21.0
    assert row["realized_pnl"] == 315.0
    trade = row["trades"][0]
    assert trade["realized_pnl"] == 315.0
    assert trade["excess_pnl"] == -21.0


def test_realtime_monitor_performance_endpoint(monkeypatch):
    client = _client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}
    strategy_id = _create_strategy(client, f"收益接口策略-{uuid4().hex[:6]}")
    account_key = f"paper_perf_api_{uuid4().hex[:8]}"
    _mock_common(monkeypatch, account_key=account_key, role="paper")
    created = client.post(
        "/v1/realtime/monitors",
        headers=headers,
        json={
            "name": "收益接口测试",
            "strategy_id": strategy_id,
            "account_key": account_key,
            "execution_mode": "auto",
            "monitor_pool": {"symbols": ["000001.SZ"]},
        },
    )
    assert created.status_code == 200
    monitor_id = created.json()["id"]

    response = client.get(f"/v1/realtime/monitors/{monitor_id}/performance", headers=headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["monitor_id"] == monitor_id
    assert payload["strategy"]["total_asset"] == 1000000.0
    assert payload["hold_baseline"]["total_asset"] == 1000000.0
    assert payload["excess"]["pnl"] == 0.0


def test_first_day_band_single_symbol_reentry_buy_uses_last_exit_quantity(monkeypatch):
    client = _client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}
    strategy_id = _create_strategy(client, f"首日波段回补策略-{uuid4().hex[:6]}")
    _mock_common(monkeypatch, account_key="paper_sim", role="paper")

    created = client.post(
        "/v1/realtime/monitors",
        headers=headers,
        json={
            "name": "首日波段单票回补",
            "strategy_id": strategy_id,
            "account_key": "paper_sim",
            "execution_mode": "auto",
            "monitor_pool": {"mode": "manual_only", "manual_symbols": ["300520.SZ"]},
            "config": {"signal_mode": "first_day_band", "signal_timeframe": "5m", "lot_size": 100},
        },
    )
    assert created.status_code == 200
    monitor_id = created.json()["id"]

    with get_strategy_db_ctx() as db:
        monitor = db.query(RealtimeMonitorDB).filter(RealtimeMonitorDB.id == monitor_id).first()
        realtime_monitor_service._sync_reentry_anchor_with_position_change(
            monitor,
            "300520.SZ",
            {"symbol": "300520.SZ", "current_position": 1000},
            None,
        )
        intent = realtime_monitor_service._build_order_intent(
            monitor,
            {
                "account": {
                    "total_asset": 1_000_000.0,
                    "available_cash": 900_000.0,
                    "cash": 900_000.0,
                },
                "positions": [],
            },
            {"symbol": "300520.SZ", "side": "buy", "price": 34.57, "target_position_pct": 0.2},
        )

    assert intent["quantity"] == 1000
    assert intent["reentry_anchor_quantity"] == 1000


def test_ensure_utc_interprets_naive_datetimes_as_local_time():
    naive_value = datetime(2026, 4, 27, 12, 57, 37)
    local_tz = datetime.now().astimezone().tzinfo or timezone.utc

    converted = realtime_monitor_service._ensure_utc(naive_value)
    expected = naive_value.replace(tzinfo=local_tz).astimezone(timezone.utc)

    assert converted == expected


def test_monitor_due_handles_naive_local_heartbeat():
    local_now = datetime.now().astimezone().replace(tzinfo=None)
    monitor = RealtimeMonitorDB(
        id=uuid4().hex,
        user_id="tester",
        name="心跳时区测试",
        account_key="paper_sim",
        strategy_id=uuid4().hex,
        status="running",
        config_json={"poll_interval_seconds": 20},
        last_heartbeat_at=local_now - timedelta(seconds=45),
    )

    assert realtime_monitor_service._monitor_due(monitor) is True


def test_repetitive_status_event_guard_suppresses_until_cooldown():
    monitor = SimpleNamespace(state_json={})
    now = datetime(2026, 5, 8, 10, 0, tzinfo=timezone.utc)

    assert realtime_monitor_service._mark_repetitive_status_event_allowed(
        monitor,
        "cycle_skipped",
        "outside_trading_session",
        now,
    ) is True
    assert realtime_monitor_service._mark_repetitive_status_event_allowed(
        monitor,
        "cycle_skipped",
        "outside_trading_session",
        now + timedelta(seconds=20),
    ) is False
    assert monitor.state_json["event_guard"]["cycle_skipped:outside_trading_session"]["suppressed_count"] == 1
    assert realtime_monitor_service._mark_repetitive_status_event_allowed(
        monitor,
        "cycle_skipped",
        "outside_trading_session",
        now + timedelta(seconds=301),
    ) is True


def test_catalyst_feedback_hook_only_runs_for_catalyst_monitor_pool(monkeypatch):
    calls: list[dict] = []
    monkeypatch.setattr(
        catalyst_selection_service,
        "capture_realtime_monitor_feedback",
        lambda strategy_db, main_db, **kwargs: calls.append(kwargs),
    )

    catalyst_monitor = SimpleNamespace(id="monitor-1", monitor_pool_json={"source": "catalyst-selection"})
    ordinary_monitor = SimpleNamespace(id="monitor-2", monitor_pool_json={"source": "watchlist"})

    realtime_monitor_service._capture_catalyst_feedback_after_cycle(object(), object(), catalyst_monitor)
    realtime_monitor_service._capture_catalyst_feedback_after_cycle(object(), object(), ordinary_monitor)

    assert calls == [{"monitor_id": "monitor-1", "limit": 100, "refresh_profiles": True}]


def test_build_position_items_skips_broken_row(monkeypatch):
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._resolve_security_name",
        lambda payload, symbol, security_name_map=None: (_ for _ in ()).throw(RuntimeError("bad row")) if symbol == "300520.SZ" else "平安银行",
    )

    config = QmtRuntimeConfig(
        key="paper_sim",
        enabled=True,
        host="127.0.0.1",
        port=58610,
        account_id="39027628",
        account_type="STOCK",
        account_name="测试账户",
        userdata_path="D:/qmt/userdata_mini",
        role="paper",
        bridge_base_url="http://127.0.0.1:8710",
        bridge_token="bridge-token",
        refresh_interval_seconds=10,
    )

    class _EmptyQuery:
        def filter(self, *args, **kwargs):
            return self

        def all(self):
            return []

    class _FakeDb:
        def query(self, *args, **kwargs):
            return _EmptyQuery()

    items = qmt_virtual_account_service._build_position_items(
        _FakeDb(),
        "test-user",
        config,
        [
            {
                "stockCode": "300520",
                "totalAmt": 100,
                "enableAmount": 0,
                "lastPrice": 10.2,
                "costPrice": 10.0,
            },
            {
                "stockCode": "000001",
                "totalAmt": 200,
                "enableAmount": 200,
                "lastPrice": 12.5,
                "costPrice": 12.0,
            },
        ],
    )

    assert len(items) == 1
    assert items[0]["symbol"] == "000001.SZ"
