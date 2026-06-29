from contextlib import contextmanager
from datetime import date, datetime, timezone

from fastapi.testclient import TestClient
from types import SimpleNamespace
from uuid import uuid4

from api.database import ImportedPortfolioPositionDB, QmtAccountEquitySnapshotDB, QmtAccountSnapshotDB, QmtAccountTradeHistoryDB, QmtSyncProfileDB, get_db_ctx
from api.data_downloader import DataDownloader
from api.services import auth_service
from api.services import qmt_virtual_account_service
from api.services.qmt_virtual_account_service import QmtRuntimeConfig


def _get_client():
    from api.main import app

    return TestClient(app, raise_server_exceptions=False)


def _auth(client: TestClient) -> str:
    response = client.post("/v1/auth/request-code", json={"email": "virtual-warehouse@test.com"})
    code = response.json()["dev_code"]
    verified = client.post("/v1/auth/verify-code", json={"email": "virtual-warehouse@test.com", "code": code})
    return verified.json()["access_token"]


def test_default_qmt_account_configs_do_not_import_env_accounts(monkeypatch):
    fake_settings = SimpleNamespace(
        qmt_host="192.168.10.1",
        qmt_port=58610,
        qmt_account_id="39027628",
        qmt_account_type="STOCK",
        qmt_account_name="Env QMT Account",
        qmt_userdata_path="D:/env/userdata_mini",
        qmt_bridge_base_url="http://192.168.10.1:8710",
        qmt_accounts=lambda: [
            {
                "key": "paper_sim",
                "enabled": True,
                "role": "paper",
                "account_id": "39027628",
                "bridge_base_url": "http://192.168.10.1:8710",
            }
        ],
    )
    monkeypatch.setattr(auth_service, "settings", fake_settings)

    configs = auth_service.default_qmt_account_configs()

    assert configs["paper"]["key"] == "paper_sim"
    assert configs["paper"]["enabled"] is False
    assert configs["paper"]["account_id"] == ""
    assert configs["paper"]["bridge_base_url"] == ""


def test_qmt_bridge_error_calls_out_local_backend_address():
    config = QmtRuntimeConfig(
        key="paper_sim",
        enabled=True,
        host="127.0.0.1",
        port=58610,
        account_id="39027628",
        account_type="STOCK",
        account_name="QMT 模拟账户",
        userdata_path="",
        role="paper",
        bridge_base_url="http://127.0.0.1:8710",
        bridge_token="",
        refresh_interval_seconds=10,
    )

    message = qmt_virtual_account_service._compact_qmt_snapshot_error(
        RuntimeError("HTTPConnectionPool: Failed to establish a new connection"),
        config,
    )

    assert "当前后端本机地址" in message
    assert "Windows bridge" in message


def test_qmt_order_bridge_accepts_legacy_string_metadata(monkeypatch):
    config = QmtRuntimeConfig(
        key="paper_bridge",
        enabled=True,
        host="127.0.0.1",
        port=58610,
        account_id="39027628",
        account_type="STOCK",
        account_name="QMT 模拟账户",
        userdata_path="",
        role="paper",
        bridge_base_url="http://127.0.0.1:8710",
        bridge_token="bridge-token",
        refresh_interval_seconds=10,
    )
    captured = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "success": True,
                "order_id": "O9001",
                "role": "paper",
                "account_key": "paper_bridge",
                "bridge": "legacy-bridge",
            }

    def fake_post(url, json, headers, timeout):
        captured.update({"url": url, "json": json, "headers": headers, "timeout": timeout})
        return FakeResponse()

    monkeypatch.setattr(qmt_virtual_account_service.requests, "post", fake_post)

    result = qmt_virtual_account_service._submit_qmt_order_via_bridge(
        config,
        symbol="000001.SZ",
        side="buy",
        quantity=1000,
        price=12.4,
        price_type="limit",
        strategy_name="test",
        order_remark="legacy bridge metadata",
    )

    assert result["order_id"] == "O9001"
    assert result["bridge"]["role"] == "paper"
    assert result["bridge"]["account_key"] == "paper_bridge"
    assert result["bridge"]["raw_bridge"] == "legacy-bridge"
    assert captured["url"] == "http://127.0.0.1:8710/orders"
    assert captured["headers"]["Authorization"] == "Bearer bridge-token"


def test_qmt_overview_route_forwards_cache_fallback_flag(monkeypatch):
    client = _get_client()
    monkeypatch.setenv("TA_DEV_ACCESS_TOKEN", "dev-test-token-001")
    headers = {"Authorization": "Bearer dev-test-token-001"}
    captured: dict[str, object] = {}

    def fake_overview(db, user_id, **kwargs):
        captured.update(kwargs)
        return {
            "connection": {
                "account_key": kwargs.get("account_key"),
                "enabled": True,
                "provider": "xtquant",
                "host": "192.168.31.220",
                "port": 58610,
                "account_id": "8886186680",
                "account_type": "STOCK",
                "account_name": "QMT 实盘仓",
                "connected": True,
                "message": "已连接 QMT 实盘账户",
                "effective_connected": True,
                "health_status": "live",
                "health_label": "实时直连",
            },
            "account": None,
            "positions": [],
            "orders": [],
            "trades": [],
            "summary": {
                "total_asset": 0,
                "total_pnl": 0,
                "today_pnl": 0,
                "market_value": 0,
                "available_cash": 0,
                "position_count": 0,
            },
            "refresh_interval_seconds": 10,
            "fetched_at": "2026-06-22T12:00:00+00:00",
            "active_account_key": kwargs.get("account_key"),
            "accounts": [],
            "data_source": "live",
            "is_stale": False,
        }

    monkeypatch.setattr(
        "api.routes.virtual_warehouse.qmt_virtual_account_service.get_qmt_virtual_account_overview",
        fake_overview,
    )

    response = client.get(
        "/v1/virtual-warehouse/qmt/overview?account_key=live_real&allow_cache_fallback=false",
        headers=headers,
    )

    assert response.status_code == 200
    assert captured["account_key"] == "live_real"
    assert captured["allow_cache_fallback"] is False


def test_qmt_virtual_warehouse_overview(monkeypatch):
    client = _get_client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}

    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="paper_demo",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="demo123",
                account_type="STOCK",
                account_name="QMT 模拟测试账户",
                userdata_path="C:/miniqmt/userdata_mini",
                role="paper",
                bridge_base_url="",
                bridge_token="",
                refresh_interval_seconds=10,
            )
        ],
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._query_qmt_snapshot",
        lambda config: {
            "fund": {"assetBalance": 1250000.0, "marketValue": 850000.0, "enableBalance": 400000.0},
            "positions": [
                {
                    "stockCode": "600519",
                    "stockName": "贵州茅台",
                    "totalAmt": 100,
                    "enableAmount": 100,
                    "costPrice": 1680.0,
                    "lastPrice": 1715.0,
                    "marketValue": 171500.0,
                    "income": 3500.0,
                }
            ],
            "asset": {"cash": 400000.0},
        },
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._fetch_live_quotes",
        lambda symbols, **kwargs: {
            "600519.SH": {
                "price": 1715.0,
                "previous_close": 1702.0,
                "change": 13.0,
                "change_pct": 0.7638,
                "quote_time": "2026-04-22 14:58:00",
                "source": "mock",
            }
        },
    )

    response = client.get("/v1/virtual-warehouse/qmt/overview", headers=headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["connection"]["connected"] is True
    assert payload["active_account_key"] == "paper_demo"
    assert payload["accounts"][0]["account_key"] == "paper_demo"
    assert payload["account"]["account_id"] == "demo123"
    assert payload["summary"]["position_count"] == 1
    assert payload["positions"][0]["symbol"] == "600519.SH"
    assert payload["positions"][0]["today_pnl"] == 1300.0
    assert payload["last_synced_at"] is not None
    assert payload["data_source"] == "live"

    with get_db_ctx() as db:
        row = (
            db.query(QmtAccountSnapshotDB)
            .filter(
                QmtAccountSnapshotDB.account_key == "paper_demo",
            )
            .first()
        )
        assert row is not None
        assert row.positions_json


def test_qmt_return_stats_calculates_full_periods(monkeypatch):
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="paper_returns_full",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="demo123",
                account_type="STOCK",
                account_name="QMT 模拟收益仓",
                userdata_path="",
                role="paper",
                bridge_base_url="http://127.0.0.1:8710",
                bridge_token="",
                refresh_interval_seconds=10,
            )
        ],
    )

    user_id = "return-stats-full-user"
    with get_db_ctx() as db:
        qmt_virtual_account_service._ensure_qmt_equity_snapshot_schema(db)
        db.query(QmtAccountEquitySnapshotDB).filter(
            QmtAccountEquitySnapshotDB.user_id == user_id,
            QmtAccountEquitySnapshotDB.account_key == "paper_returns_full",
        ).delete(synchronize_session=False)
        for snapshot_date, total_asset in [
            (date(2025, 12, 31), 950000.0),
            (date(2026, 3, 31), 1000000.0),
            (date(2026, 4, 21), 1020000.0),
            (date(2026, 4, 22), 1050000.0),
        ]:
            db.add(
                QmtAccountEquitySnapshotDB(
                    id=uuid4().hex,
                    user_id=user_id,
                    account_key="paper_returns_full",
                    role="paper",
                    account_id="demo123",
                    snapshot_date=snapshot_date,
                    total_asset=total_asset,
                    market_value=500000.0,
                    available_cash=total_asset - 500000.0,
                    total_pnl=50000.0,
                    today_pnl=12000.0,
                    fetched_at=datetime(snapshot_date.year, snapshot_date.month, snapshot_date.day, 7, 0, tzinfo=timezone.utc),
                )
            )
        db.commit()

        payload = qmt_virtual_account_service.get_qmt_return_stats(db, user_id, account_key="paper_returns_full")

    assert payload["account_key"] == "paper_returns_full"
    assert payload["periods"]["day"]["amount"] == 30000.0
    assert payload["periods"]["day"]["rate"] == 2.94
    assert payload["periods"]["month"]["amount"] == 50000.0
    assert payload["periods"]["month"]["rate"] == 5.0
    assert payload["periods"]["year"]["amount"] == 100000.0
    assert payload["periods"]["year"]["rate"] == 10.53
    assert payload["periods"]["day"]["coverage"] == "full"


def test_qmt_return_stats_falls_back_when_history_is_missing(monkeypatch):
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="paper_returns_partial",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="demo123",
                account_type="STOCK",
                account_name="QMT 模拟收益仓",
                userdata_path="",
                role="paper",
                bridge_base_url="http://127.0.0.1:8710",
                bridge_token="",
                refresh_interval_seconds=10,
            )
        ],
    )

    user_id = "return-stats-partial-user"
    with get_db_ctx() as db:
        qmt_virtual_account_service._ensure_qmt_equity_snapshot_schema(db)
        db.query(QmtAccountEquitySnapshotDB).filter(
            QmtAccountEquitySnapshotDB.user_id == user_id,
            QmtAccountEquitySnapshotDB.account_key == "paper_returns_partial",
        ).delete(synchronize_session=False)
        db.add(
            QmtAccountEquitySnapshotDB(
                id=uuid4().hex,
                user_id=user_id,
                account_key="paper_returns_partial",
                role="paper",
                account_id="demo123",
                snapshot_date=date(2026, 4, 22),
                total_asset=100000.0,
                market_value=30000.0,
                available_cash=70000.0,
                total_pnl=5000.0,
                today_pnl=1500.0,
                fetched_at=datetime(2026, 4, 22, 7, 0, tzinfo=timezone.utc),
            )
        )
        db.commit()

        payload = qmt_virtual_account_service.get_qmt_return_stats(db, user_id, account_key="paper_returns_partial")

    assert payload["periods"]["day"]["amount"] == 1500.0
    assert payload["periods"]["day"]["coverage"] == "fallback"
    assert payload["periods"]["month"]["amount"] == 0.0
    assert payload["periods"]["month"]["coverage"] == "partial"
    assert payload["periods"]["year"]["coverage"] == "partial"


def test_qmt_return_stats_includes_calendar_heatmap(monkeypatch):
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="paper_returns_calendar",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="demo123",
                account_type="STOCK",
                account_name="QMT 模拟收益仓",
                userdata_path="",
                role="paper",
                bridge_base_url="http://127.0.0.1:8710",
                bridge_token="",
                refresh_interval_seconds=10,
            )
        ],
    )

    user_id = "return-stats-calendar-user"
    with get_db_ctx() as db:
        qmt_virtual_account_service._ensure_qmt_equity_snapshot_schema(db)
        db.query(QmtAccountEquitySnapshotDB).filter(
            QmtAccountEquitySnapshotDB.user_id == user_id,
            QmtAccountEquitySnapshotDB.account_key == "paper_returns_calendar",
        ).delete(synchronize_session=False)
        for snapshot_date, total_asset, today_pnl in [
            (date(2026, 3, 31), 100000.0, 0.0),
            (date(2026, 4, 1), 101000.0, 1000.0),
            (date(2026, 4, 2), 100500.0, -500.0),
        ]:
            db.add(
                QmtAccountEquitySnapshotDB(
                    id=uuid4().hex,
                    user_id=user_id,
                    account_key="paper_returns_calendar",
                    role="paper",
                    account_id="demo123",
                    snapshot_date=snapshot_date,
                    total_asset=total_asset,
                    market_value=0.0,
                    available_cash=total_asset,
                    total_pnl=0.0,
                    today_pnl=today_pnl,
                    fetched_at=datetime(snapshot_date.year, snapshot_date.month, snapshot_date.day, 7, 0, tzinfo=timezone.utc),
                )
            )
        db.commit()

        payload = qmt_virtual_account_service.get_qmt_return_stats(db, user_id, account_key="paper_returns_calendar")

    calendar_payload = payload["calendar"]
    by_date = {item["date"]: item for item in calendar_payload["days"]}
    assert calendar_payload["month_label"] == "2026年04月"
    assert len(calendar_payload["days"]) == 30
    assert calendar_payload["max_abs_amount"] == 1000.0
    assert by_date["2026-04-01"]["amount"] == 1000.0
    assert by_date["2026-04-01"]["tone"] == "gain"
    assert by_date["2026-04-02"]["amount"] == -500.0
    assert by_date["2026-04-02"]["tone"] == "loss"
    assert by_date["2026-04-03"]["has_snapshot"] is False


def test_qmt_return_stats_summarizes_traded_securities(monkeypatch):
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="paper_trade_history",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="demo123",
                account_type="STOCK",
                account_name="QMT 模拟收益仓",
                userdata_path="",
                role="paper",
                bridge_base_url="http://127.0.0.1:8710",
                bridge_token="",
                refresh_interval_seconds=10,
            )
        ],
    )

    user_id = "return-stats-trades-user"
    with get_db_ctx() as db:
        qmt_virtual_account_service._ensure_qmt_equity_snapshot_schema(db)
        db.query(QmtAccountEquitySnapshotDB).filter(
            QmtAccountEquitySnapshotDB.user_id == user_id,
            QmtAccountEquitySnapshotDB.account_key == "paper_trade_history",
        ).delete(synchronize_session=False)
        db.query(QmtAccountTradeHistoryDB).filter(
            QmtAccountTradeHistoryDB.user_id == user_id,
            QmtAccountTradeHistoryDB.account_key == "paper_trade_history",
        ).delete(synchronize_session=False)
        db.add(
            QmtAccountEquitySnapshotDB(
                id=uuid4().hex,
                user_id=user_id,
                account_key="paper_trade_history",
                role="paper",
                account_id="demo123",
                snapshot_date=date(2026, 4, 22),
                total_asset=100000.0,
                market_value=0.0,
                available_cash=100000.0,
                total_pnl=0.0,
                today_pnl=0.0,
                fetched_at=datetime(2026, 4, 22, 7, 0, tzinfo=timezone.utc),
            )
        )
        for trade_id, side, quantity, amount, trade_time in [
            ("T1", "buy", 1000.0, 12000.0, datetime(2026, 4, 20, 2, 0, tzinfo=timezone.utc)),
            ("T2", "sell", 500.0, 6500.0, datetime(2026, 4, 22, 2, 0, tzinfo=timezone.utc)),
        ]:
            db.add(
                QmtAccountTradeHistoryDB(
                    id=uuid4().hex,
                    user_id=user_id,
                    account_key="paper_trade_history",
                    role="paper",
                    account_id="demo123",
                    trade_uid=f"id:{trade_id}",
                    trade_id=trade_id,
                    symbol="000001.SZ",
                    name="平安银行",
                    side=side,
                    price=12.0,
                    quantity=quantity,
                    amount=amount,
                    cost_price=12.0 if side == "sell" else None,
                    cost_basis=6000.0 if side == "sell" else None,
                    realized_pnl=500.0 if side == "sell" else 0.0,
                    realized_pnl_pct=8.33 if side == "sell" else 0.0,
                    pnl_status="estimated" if side == "sell" else "buy_open",
                    trade_time=trade_time,
                    trade_date=trade_time.date(),
                    fetched_at=trade_time,
                )
            )
        db.commit()

        payload = qmt_virtual_account_service.get_qmt_return_stats(db, user_id, account_key="paper_trade_history")

    item = payload["traded_securities"][0]
    assert item["symbol"] == "000001.SZ"
    assert item["trade_count"] == 2
    assert item["buy_quantity"] == 1000.0
    assert item["sell_quantity"] == 500.0
    assert item["net_quantity"] == 500.0
    assert item["net_cashflow"] == -5500.0
    assert item["realized_pnl"] == 500.0
    assert item["realized_pnl_pct"] == 8.33


def test_qmt_trade_history_calculates_sell_realized_pnl(monkeypatch):
    config = QmtRuntimeConfig(
        key="paper_sell_pnl",
        enabled=True,
        host="192.168.10.1",
        port=58610,
        account_id="demo123",
        account_type="STOCK",
        account_name="QMT 模拟收益仓",
        userdata_path="",
        role="paper",
        bridge_base_url="http://127.0.0.1:8710",
        bridge_token="",
        refresh_interval_seconds=10,
    )
    user_id = "return-stats-sell-pnl-user"
    with get_db_ctx() as db:
        qmt_virtual_account_service._ensure_qmt_equity_snapshot_schema(db)
        db.query(QmtAccountTradeHistoryDB).filter(
            QmtAccountTradeHistoryDB.user_id == user_id,
            QmtAccountTradeHistoryDB.account_key == "paper_sell_pnl",
        ).delete(synchronize_session=False)
        payload = {
            "positions": [
                {
                    "symbol": "000001.SZ",
                    "name": "平安银行",
                    "current_position": 500.0,
                    "average_cost": 10.0,
                }
            ],
            "trades": [
                {
                    "trade_id": "SELL001",
                    "symbol": "000001.SZ",
                    "name": "平安银行",
                    "side": "sell",
                    "price": 12.0,
                    "quantity": 100.0,
                    "amount": 1200.0,
                    "trade_time": "2026-04-22T10:00:00+08:00",
                }
            ],
        }

        qmt_virtual_account_service._persist_qmt_trade_history(
            db,
            user_id,
            config,
            payload,
            fetched_at=datetime(2026, 4, 22, 2, 0, tzinfo=timezone.utc),
        )
        db.commit()

        row = db.query(QmtAccountTradeHistoryDB).filter(
            QmtAccountTradeHistoryDB.user_id == user_id,
            QmtAccountTradeHistoryDB.account_key == "paper_sell_pnl",
            QmtAccountTradeHistoryDB.trade_id == "SELL001",
        ).first()

    assert row is not None
    assert row.cost_price == 10.0
    assert row.cost_basis == 1000.0
    assert row.realized_pnl == 200.0
    assert row.realized_pnl_pct == 20.0
    assert row.pnl_status == "estimated"


def test_qmt_return_stats_separates_paper_and_live(monkeypatch):
    user_id = "return-stats-role-user"
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="paper_returns_route",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="paper123",
                account_type="STOCK",
                account_name="QMT 模拟收益仓",
                userdata_path="",
                role="paper",
                bridge_base_url="http://127.0.0.1:8710",
                bridge_token="",
                refresh_interval_seconds=10,
            ),
            QmtRuntimeConfig(
                key="live_returns_route",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="live123",
                account_type="STOCK",
                account_name="QMT 实盘收益仓",
                userdata_path="",
                role="live",
                bridge_base_url="http://127.0.0.1:8711",
                bridge_token="",
                refresh_interval_seconds=10,
            ),
        ],
    )

    with get_db_ctx() as db:
        qmt_virtual_account_service._ensure_qmt_equity_snapshot_schema(db)
        db.query(QmtAccountEquitySnapshotDB).filter(
            QmtAccountEquitySnapshotDB.user_id == user_id,
            QmtAccountEquitySnapshotDB.account_key.in_(["paper_returns_route", "live_returns_route"]),
        ).delete(synchronize_session=False)
        for account_key, role, account_id, previous_asset, current_asset in [
            ("paper_returns_route", "paper", "paper123", 10000.0, 10100.0),
            ("live_returns_route", "live", "live123", 20000.0, 20300.0),
        ]:
            db.add(
                QmtAccountEquitySnapshotDB(
                    id=uuid4().hex,
                    user_id=user_id,
                    account_key=account_key,
                    role=role,
                    account_id=account_id,
                    snapshot_date=date(2026, 4, 21),
                    total_asset=previous_asset,
                    market_value=0.0,
                    available_cash=previous_asset,
                    total_pnl=0.0,
                    today_pnl=0.0,
                    fetched_at=datetime(2026, 4, 21, 7, 0, tzinfo=timezone.utc),
                )
            )
            db.add(
                QmtAccountEquitySnapshotDB(
                    id=uuid4().hex,
                    user_id=user_id,
                    account_key=account_key,
                    role=role,
                    account_id=account_id,
                    snapshot_date=date(2026, 4, 22),
                    total_asset=current_asset,
                    market_value=0.0,
                    available_cash=current_asset,
                    total_pnl=0.0,
                    today_pnl=current_asset - previous_asset,
                    fetched_at=datetime(2026, 4, 22, 7, 0, tzinfo=timezone.utc),
                )
            )
        db.commit()

        live_payload = qmt_virtual_account_service.get_qmt_return_stats(db, user_id, preferred_role="live")
        paper_payload = qmt_virtual_account_service.get_qmt_return_stats(db, user_id, preferred_role="paper")

    assert live_payload["account_key"] == "live_returns_route"
    assert live_payload["periods"]["day"]["amount"] == 300.0
    assert paper_payload["account_key"] == "paper_returns_route"
    assert paper_payload["periods"]["day"]["amount"] == 100.0


def test_qmt_virtual_warehouse_sync_does_not_write_tracking_board(monkeypatch):
    client = _get_client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}
    me = client.get("/v1/auth/me", headers=headers).json()

    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="paper_demo",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="demo123",
                account_type="STOCK",
                account_name="QMT 模拟测试账户",
                userdata_path="C:/miniqmt/userdata_mini",
                role="paper",
                bridge_base_url="",
                bridge_token="",
                refresh_interval_seconds=10,
            )
        ],
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._query_qmt_snapshot",
        lambda config: {
            "fund": {"assetBalance": 1000000.0, "marketValue": 100000.0, "enableBalance": 900000.0},
            "positions": [
                {
                    "stockCode": "300750",
                    "stockName": "宁德时代",
                    "totalAmt": 200,
                    "enableAmount": 100,
                    "costPrice": 200.0,
                    "lastPrice": 210.0,
                    "marketValue": 42000.0,
                    "income": 2000.0,
                }
            ],
            "asset": {"cash": 900000.0},
        },
    )
    monkeypatch.setattr("api.services.qmt_virtual_account_service._fetch_live_quotes", lambda symbols, **kwargs: {})

    response = client.post("/v1/virtual-warehouse/qmt/sync", headers=headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] is None
    assert "隔离" in payload["message"]

    with get_db_ctx() as db:
        row = (
            db.query(ImportedPortfolioPositionDB)
            .filter(
                ImportedPortfolioPositionDB.user_id == me["id"],
                ImportedPortfolioPositionDB.symbol == "300750.SZ",
            )
            .first()
        )
        assert row is None


def test_qmt_virtual_warehouse_diagnostics(monkeypatch):
    client = _get_client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}

    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="paper_sim",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="39027628",
                account_type="STOCK",
                account_name="国金QMT模拟仓",
                userdata_path="D:/国金QMT交易端模拟/userdata_mini",
                role="paper",
                bridge_base_url="",
                bridge_token="",
                refresh_interval_seconds=10,
            )
        ],
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._check_xtquant_available",
        lambda: (True, "xtquant 已安装"),
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service.os.path.exists",
        lambda path: True,
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._run_connect_diagnostic",
        lambda config: {"attempted": True, "connected": True, "message": "连接成功，可读取账户资产与持仓"},
    )

    response = client.get("/v1/virtual-warehouse/qmt/diagnostics?run_connect_test=true", headers=headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["total"] == 1
    assert payload["summary"]["connected"] == 1
    assert payload["items"][0]["account_key"] == "paper_sim"
    assert payload["items"][0]["ready"] is True


def test_qmt_virtual_warehouse_overview_via_bridge(monkeypatch):
    client = _get_client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}

    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="paper_bridge",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="39027628",
                account_type="STOCK",
                account_name="QMT Bridge 模拟仓",
                userdata_path="",
                role="paper",
                bridge_base_url="http://127.0.0.1:8710",
                bridge_token="bridge-token",
                refresh_interval_seconds=10,
            )
        ],
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._query_qmt_snapshot_via_bridge",
        lambda config: {
            "fund": {"assetBalance": 800000.0, "marketValue": 300000.0, "enableBalance": 500000.0},
            "positions": [
                {
                    "stockCode": "000001",
                    "stockName": "平安银行",
                    "totalAmt": 1000,
                    "enableAmount": 1000,
                    "costPrice": 12.0,
                    "lastPrice": 12.5,
                    "marketValue": 12500.0,
                    "income": 500.0,
                }
            ],
            "orders": [
                {
                    "orderId": "O001",
                    "stockCode": "000001",
                    "stockName": "平安银行",
                    "orderType": "buy",
                    "orderStatus": "filled",
                    "orderPrice": 12.4,
                    "orderVolume": 1000,
                    "tradedVolume": 1000,
                    "orderTime": "2026-04-22 10:00:00",
                }
            ],
            "trades": [
                {
                    "tradedId": "T001",
                    "orderId": "O001",
                    "stockCode": "000001",
                    "stockName": "平安银行",
                    "orderType": "buy",
                    "tradedPrice": 12.4,
                    "tradedVolume": 1000,
                    "tradedTime": "2026-04-22 10:00:03",
                }
            ],
            "asset": {"cash": 500000.0},
            "bridge": {"mode": "http_bridge"},
        },
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._fetch_live_quotes",
        lambda symbols, **kwargs: {"000001.SZ": {"price": 12.5, "previous_close": 12.3, "change": 0.2, "change_pct": 1.626}},
    )

    response = client.get("/v1/virtual-warehouse/qmt/overview?account_key=paper_bridge", headers=headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["connection"]["connected"] is True
    assert payload["positions"][0]["symbol"] == "000001.SZ"
    assert payload["orders"][0]["order_id"] == "O001"
    assert payload["trades"][0]["trade_id"] == "T001"
    assert payload["account"]["total_asset"] == 800000.0


def test_qmt_virtual_warehouse_normalizes_xtquant_order_trade_fields(monkeypatch):
    client = _get_client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}

    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="paper_bridge",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="39027628",
                account_type="STOCK",
                account_name="QMT Bridge 模拟仓",
                userdata_path="",
                role="paper",
                bridge_base_url="http://127.0.0.1:8710",
                bridge_token="bridge-token",
                refresh_interval_seconds=10,
            )
        ],
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._query_qmt_snapshot_via_bridge",
        lambda config: {
            "fund": {"assetBalance": 800000.0, "marketValue": 0.0, "enableBalance": 800000.0},
            "positions": [],
            "orders": [
                {
                    "m_nOrderID": 1098929264,
                    "order_id": 1098929264,
                    "m_strStockCode": "300520.SZ",
                    "m_nOrderTime": 1778119702,
                    "order_time": 1778119702,
                    "m_nOrderType": 24,
                    "order_type": 24,
                    "m_nOrderVolume": 5000,
                    "order_volume": 5000,
                    "m_nTradedVolume": 5000,
                    "traded_volume": 5000,
                    "m_dPrice": 37.35,
                    "price": 37.35,
                    "m_nOrderStatus": 56,
                    "order_status": 56,
                }
            ],
            "trades": [
                {
                    "m_strTradedID": "T1098929264",
                    "traded_id": "T1098929264",
                    "m_nOrderID": 1098929264,
                    "order_id": 1098929264,
                    "m_strStockCode": "300520.SZ",
                    "m_nTradedTime": 1778119742,
                    "traded_time": 1778119742,
                    "m_nOrderType": 24,
                    "order_type": 24,
                    "m_nTradedVolume": 5000,
                    "traded_volume": 5000,
                    "m_dTradedPrice": 37.35,
                    "traded_price": 37.35,
                    "m_dTradedAmount": 186750.0,
                    "traded_amount": 186750.0,
                }
            ],
            "asset": {"cash": 800000.0},
        },
    )
    monkeypatch.setattr("api.services.qmt_virtual_account_service._fetch_live_quotes", lambda symbols, **kwargs: {})
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service.get_reverse_stock_map_cached_only",
        lambda: {"300520.SZ": "科大国创"},
    )

    response = client.get("/v1/virtual-warehouse/qmt/overview?account_key=paper_bridge", headers=headers)
    assert response.status_code == 200
    payload = response.json()
    order = payload["orders"][0]
    trade = payload["trades"][0]

    assert order["order_id"] == "1098929264"
    assert order["symbol"] == "300520.SZ"
    assert order["name"] == "科大国创"
    assert order["side"] == "sell"
    assert order["status"] == "filled"
    assert order["quantity"] == 5000
    assert order["filled_quantity"] == 5000
    assert order["price"] == 37.35
    assert datetime.fromisoformat(order["order_time"]).date().isoformat() == "2026-05-07"
    assert order["can_cancel"] is False

    assert trade["trade_id"] == "T1098929264"
    assert trade["order_id"] == "1098929264"
    assert trade["symbol"] == "300520.SZ"
    assert trade["name"] == "科大国创"
    assert trade["side"] == "sell"
    assert trade["quantity"] == 5000
    assert trade["price"] == 37.35
    assert trade["amount"] == 186750.0
    assert datetime.fromisoformat(trade["trade_time"]).date().isoformat() == "2026-05-07"


def test_qmt_live_position_uses_quote_change_for_today_metrics(monkeypatch):
    client = _get_client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}

    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="live_real",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="8886186680",
                account_type="STOCK",
                account_name="QMT 实盘仓",
                userdata_path="",
                role="live",
                bridge_base_url="http://127.0.0.1:8711",
                bridge_token="bridge-token",
                refresh_interval_seconds=10,
            )
        ],
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._query_qmt_snapshot_via_bridge",
        lambda config: {
            "fund": {"assetBalance": 161357.27, "marketValue": 18720.0, "enableBalance": 513.27},
            "positions": [
                {
                    "stock_code": "601136.SH",
                    "stockName": "首创证券",
                    "volume": 1300,
                    "can_use_volume": 1300,
                    "avg_price": 22.885961538461537,
                    "market_value": 18720.0,
                    "yesterday_volume": 1300,
                }
            ],
            "orders": [],
            "trades": [],
            "asset": {"cash": 513.27},
            "bridge": {"mode": "http_bridge", "role": "live", "account_key": "live_real", "account_id": "8886186680"},
        },
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._fetch_live_quotes",
        lambda symbols, **kwargs: {
            "601136.SH": {
                "price": 14.5,
                "previous_close": 14.4,
                "change": 0.1,
                "change_pct": 0.6944,
                "quote_time": "2026-06-16 10:41:47",
                "source": "qmt_bridge",
            }
        },
    )

    response = client.get("/v1/virtual-warehouse/qmt/overview?account_key=live_real", headers=headers)
    assert response.status_code == 200
    payload = response.json()
    position = payload["positions"][0]

    assert position["current_price"] == 14.4
    assert position["price_source"] == "qmt_position_market_value"
    assert position["previous_close"] == 14.4
    assert position["today_pnl"] == 130.0
    assert position["today_pnl_pct"] == 0.6944
    assert payload["summary"]["today_pnl"] == 130.0


def test_qmt_virtual_warehouse_name_fallback_from_cached_map(monkeypatch):
    client = _get_client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}

    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="paper_bridge",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="39027628",
                account_type="STOCK",
                account_name="QMT Bridge 模拟仓",
                userdata_path="",
                role="paper",
                bridge_base_url="http://127.0.0.1:8710",
                bridge_token="bridge-token",
                refresh_interval_seconds=10,
            )
        ],
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._query_qmt_snapshot_via_bridge",
        lambda config: {
            "fund": {"assetBalance": 800000.0, "marketValue": 300000.0, "enableBalance": 500000.0},
            "positions": [
                {
                    "stockCode": "600006",
                    "totalAmt": 100,
                    "enableAmount": 100,
                    "costPrice": 6.2,
                    "lastPrice": 6.42,
                    "marketValue": 642.0,
                    "income": 22.0,
                }
            ],
            "orders": [],
            "trades": [],
            "asset": {"cash": 500000.0},
            "bridge": {"mode": "http_bridge"},
        },
    )
    monkeypatch.setattr("api.services.qmt_virtual_account_service._fetch_live_quotes", lambda symbols, **kwargs: {})
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service.get_reverse_stock_map_cached_only",
        lambda: {"600006.SH": "东风股份"},
    )

    response = client.get("/v1/virtual-warehouse/qmt/overview?account_key=paper_bridge", headers=headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["positions"][0]["symbol"] == "600006.SH"
    assert payload["positions"][0]["name"] == "东风股份"


def test_qmt_overview_only_fetches_active_account(monkeypatch):
    client = _get_client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}

    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="paper_sim",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="39027628",
                account_type="STOCK",
                account_name="QMT 模拟仓",
                userdata_path="",
                role="paper",
                bridge_base_url="http://127.0.0.1:8710",
                bridge_token="bridge-token",
                refresh_interval_seconds=10,
            ),
            QmtRuntimeConfig(
                key="live_real",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="8886186680",
                account_type="STOCK",
                account_name="QMT 实盘仓",
                userdata_path="",
                role="live",
                bridge_base_url="http://127.0.0.1:8711",
                bridge_token="bridge-token",
                refresh_interval_seconds=10,
            ),
        ],
    )

    fetched: list[str] = []

    def fake_query(config):
        fetched.append(config.key)
        return {
            "fund": {"assetBalance": 1000.0, "marketValue": 0.0, "enableBalance": 1000.0},
            "positions": [],
            "orders": [],
            "trades": [],
            "asset": {"cash": 1000.0},
        }

    monkeypatch.setattr("api.services.qmt_virtual_account_service._query_qmt_snapshot", fake_query)
    monkeypatch.setattr("api.services.qmt_virtual_account_service._fetch_live_quotes", lambda symbols, **kwargs: {})

    response = client.get("/v1/virtual-warehouse/qmt/overview?account_key=live_real", headers=headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["active_account_key"] == "live_real"
    assert fetched == ["live_real"]
    assert payload["accounts"][0]["summary"]["total_asset"] == 0.0
    assert payload["accounts"][1]["summary"]["total_asset"] == 1000.0


def test_qmt_overview_falls_back_to_cached_snapshot(monkeypatch):
    client = _get_client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}

    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="paper_sim",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="39027628",
                account_type="STOCK",
                account_name="QMT 模拟仓",
                userdata_path="",
                role="paper",
                bridge_base_url="http://127.0.0.1:8710",
                bridge_token="bridge-token",
                refresh_interval_seconds=10,
            ),
        ],
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._query_qmt_snapshot_via_bridge",
        lambda config: {
            "fund": {"assetBalance": 100000.0, "marketValue": 20000.0, "enableBalance": 80000.0},
            "positions": [
                {
                    "stockCode": "000001",
                    "stockName": "平安银行",
                    "totalAmt": 1000,
                    "enableAmount": 1000,
                    "costPrice": 12.0,
                    "lastPrice": 12.5,
                    "marketValue": 12500.0,
                    "income": 500.0,
                }
            ],
            "orders": [],
            "trades": [],
            "asset": {"cash": 80000.0},
        },
    )
    monkeypatch.setattr("api.services.qmt_virtual_account_service._fetch_live_quotes", lambda symbols, **kwargs: {})

    first_response = client.get("/v1/virtual-warehouse/qmt/overview?account_key=paper_sim", headers=headers)
    assert first_response.status_code == 200
    first_payload = first_response.json()
    assert first_payload["data_source"] == "live"
    assert first_payload["positions"][0]["name"] == "平安银行"

    def fail_query(config):
        raise RuntimeError("bridge disconnected")

    monkeypatch.setattr("api.services.qmt_virtual_account_service._query_qmt_snapshot_via_bridge", fail_query)

    second_response = client.get("/v1/virtual-warehouse/qmt/overview?account_key=paper_sim", headers=headers)
    assert second_response.status_code == 200
    second_payload = second_response.json()
    assert second_payload["data_source"] == "cache"
    assert second_payload["is_stale"] is True
    assert second_payload["connection"]["connected"] is True
    assert second_payload["connection"]["health_status"] == "snapshot_available"
    assert second_payload["connection"]["health_label"] == "快照可用"
    assert second_payload["positions"][0]["name"] == "平安银行"
    assert "最近快照" in second_payload["connection"]["message"]


def test_qmt_overview_uses_sync_profile_for_background_health(monkeypatch):
    client = _get_client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}
    user_id = auth_service.decode_access_token(token)["sub"]

    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="paper_health",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="39027628",
                account_type="STOCK",
                account_name="QMT 模拟仓",
                userdata_path="",
                role="paper",
                bridge_base_url="http://127.0.0.1:8710",
                bridge_token="bridge-token",
                refresh_interval_seconds=10,
            ),
        ],
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._query_qmt_snapshot_via_bridge",
        lambda config: {
            "fund": {"assetBalance": 100000.0, "marketValue": 20000.0, "enableBalance": 80000.0},
            "positions": [],
            "orders": [],
            "trades": [],
            "asset": {"cash": 80000.0},
        },
    )
    monkeypatch.setattr("api.services.qmt_virtual_account_service._fetch_live_quotes", lambda symbols, **kwargs: {})

    first_response = client.get("/v1/virtual-warehouse/qmt/overview?account_key=paper_health", headers=headers)
    assert first_response.status_code == 200
    with get_db_ctx() as db:
        db.add(
            QmtSyncProfileDB(
                id=uuid4().hex,
                user_id=user_id,
                account_key="paper_health",
                is_active=True,
                sync_interval_seconds=30,
                sync_tracking_board=False,
                alert_on_disconnect=True,
                last_synced_at=datetime.now(timezone.utc),
                last_status="success",
                consecutive_failures=0,
            )
        )
        db.commit()

    def fail_query(config):
        raise RuntimeError("bridge disconnected")

    monkeypatch.setattr("api.services.qmt_virtual_account_service._query_qmt_snapshot_via_bridge", fail_query)

    second_response = client.get("/v1/virtual-warehouse/qmt/overview?account_key=paper_health", headers=headers)
    assert second_response.status_code == 200
    payload = second_response.json()
    assert payload["data_source"] == "cache"
    assert payload["is_stale"] is True
    assert payload["sync_profile"]["last_status"] == "success"
    assert payload["connection"]["health_status"] == "background_live"
    assert payload["connection"]["health_label"] == "后台在线"
    assert payload["connection"]["effective_connected"] is True


def test_qmt_background_refresh_skips_recent_bridge_failure(monkeypatch):
    monkeypatch.setattr(
        qmt_virtual_account_service,
        "_resolve_runtime_config",
        lambda account_key, db=None, user_id=None: QmtRuntimeConfig(
            key="paper_bridge",
            enabled=True,
            host="192.168.10.1",
            port=58610,
            account_id="39027628",
            account_type="STOCK",
            account_name="QMT 模拟仓",
            userdata_path="",
            role="paper",
            bridge_base_url="http://127.0.0.1:8710",
            bridge_token="bridge-token",
            refresh_interval_seconds=10,
        ),
    )
    qmt_virtual_account_service._QMT_RECENT_FAILURES.clear()
    qmt_virtual_account_service._QMT_BACKGROUND_REFRESH_STATE.clear()
    qmt_virtual_account_service._remember_fetch_failure("user-1:paper_bridge", "QMT 连接失败：bridge timeout")

    calls = {"count": 0}

    def fail_query(config):
        calls["count"] += 1
        raise RuntimeError("bridge should not be called")

    monkeypatch.setattr(qmt_virtual_account_service, "_query_qmt_snapshot_via_bridge_async", fail_query)

    @contextmanager
    def fake_db_ctx():
        yield object()

    monkeypatch.setattr("api.database.get_db_ctx", fake_db_ctx)

    qmt_virtual_account_service._run_qmt_background_refresh("user-1", "paper_bridge")

    assert calls["count"] == 0
    state = qmt_virtual_account_service._get_background_refresh_status("user-1:paper_bridge")
    assert state is not None
    assert "bridge timeout" in str(state.get("last_error") or "")


def test_qmt_overview_uses_cached_summary_for_inactive_account(monkeypatch):
    client = _get_client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}

    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="paper_sim",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="39027628",
                account_type="STOCK",
                account_name="QMT 模拟仓",
                userdata_path="",
                role="paper",
                bridge_base_url="http://127.0.0.1:8710",
                bridge_token="bridge-token",
                refresh_interval_seconds=10,
            ),
            QmtRuntimeConfig(
                key="live_real",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="8886186680",
                account_type="STOCK",
                account_name="QMT 实盘仓",
                userdata_path="",
                role="live",
                bridge_base_url="http://127.0.0.1:8711",
                bridge_token="bridge-token",
                refresh_interval_seconds=10,
            ),
        ],
    )

    def fake_query(config):
        total_asset = 1000.0 if config.key == "paper_sim" else 2000.0
        return {
            "fund": {"assetBalance": total_asset, "marketValue": 0.0, "enableBalance": total_asset},
            "positions": [],
            "orders": [],
            "trades": [],
            "asset": {"cash": total_asset},
        }

    monkeypatch.setattr("api.services.qmt_virtual_account_service._query_qmt_snapshot", fake_query)
    monkeypatch.setattr("api.services.qmt_virtual_account_service._fetch_live_quotes", lambda symbols, **kwargs: {})

    seed_response = client.get("/v1/virtual-warehouse/qmt/overview?account_key=paper_sim", headers=headers)
    assert seed_response.status_code == 200

    response = client.get("/v1/virtual-warehouse/qmt/overview?account_key=live_real", headers=headers)
    assert response.status_code == 200
    payload = response.json()
    paper_summary = next(item for item in payload["accounts"] if item["account_key"] == "paper_sim")
    live_summary = next(item for item in payload["accounts"] if item["account_key"] == "live_real")

    assert paper_summary["connection"]["connected"] is True
    assert paper_summary["summary"]["total_asset"] == 1000.0
    assert live_summary["summary"]["total_asset"] == 2000.0


def test_qmt_sync_profile_endpoint():
    client = _get_client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}

    create_response = client.post(
        "/v1/virtual-warehouse/qmt/sync-profiles/paper_sim",
        headers=headers,
        json={
            "is_active": True,
            "sync_interval_seconds": 45,
            "sync_tracking_board": True,
            "alert_on_disconnect": True,
        },
    )
    assert create_response.status_code == 200
    payload = create_response.json()
    assert payload["account_key"] == "paper_sim"
    assert payload["is_active"] is True
    assert payload["sync_interval_seconds"] == 45
    assert payload["sync_tracking_board"] is False

    list_response = client.get("/v1/virtual-warehouse/qmt/sync-profiles", headers=headers)
    assert list_response.status_code == 200
    items = list_response.json()["items"]
    assert any(item["account_key"] == "paper_sim" for item in items)


def test_qmt_submit_order_route(monkeypatch):
    client = _get_client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}

    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="paper_bridge",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="39027628",
                account_type="STOCK",
                account_name="QMT Bridge 模拟仓",
                userdata_path="",
                role="paper",
                bridge_base_url="http://127.0.0.1:8710",
                bridge_token="bridge-token",
                refresh_interval_seconds=10,
            )
        ],
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._submit_qmt_order",
        lambda config, **kwargs: {
            "success": True,
            "order_id": "O9001",
            "result": "O9001",
            "request": kwargs,
        },
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._fetch_qmt_bridge_health",
        lambda config, timeout=2.0: {"role": "paper", "account_key": "paper_bridge", "trading_allowed": True},
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._query_qmt_snapshot",
        lambda config: {
            "fund": {"assetBalance": 800000.0, "marketValue": 300000.0, "enableBalance": 500000.0},
            "positions": [],
            "orders": [
                {
                    "orderId": "O9001",
                    "stockCode": "000001",
                    "stockName": "平安银行",
                    "orderType": "buy",
                    "orderStatus": "submitted",
                    "orderPrice": 12.4,
                    "orderVolume": 1000,
                    "tradedVolume": 0,
                    "orderTime": "2026-04-22 10:00:00",
                }
            ],
            "trades": [],
            "asset": {"cash": 500000.0},
        },
    )
    monkeypatch.setattr("api.services.qmt_virtual_account_service._fetch_live_quotes", lambda symbols, **kwargs: {})

    response = client.post(
        "/v1/virtual-warehouse/qmt/orders",
        headers=headers,
        json={
            "account_key": "paper_bridge",
            "symbol": "000001.SZ",
            "side": "buy",
            "quantity": 1000,
            "price": 12.4,
            "price_type": "limit",
            "strategy_name": "test",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["order_result"]["order_id"] == "O9001"
    assert payload["overview"]["orders"][0]["order_id"] == "O9001"


def test_qmt_submit_order_allows_live_account(monkeypatch):
    client = _get_client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}
    called = {"submit": False}

    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="live_real",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="8886186680",
                account_type="STOCK",
                account_name="QMT 实盘仓",
                userdata_path="",
                role="live",
                bridge_base_url="http://127.0.0.1:8711",
                bridge_token="bridge-token",
                refresh_interval_seconds=10,
            )
        ],
    )

    def fake_submit(*args, **kwargs):
        called["submit"] = True
        return {"success": True, "order_id": "L9001"}

    monkeypatch.setattr("api.services.qmt_virtual_account_service._submit_qmt_order", fake_submit)
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._fetch_qmt_bridge_health",
        lambda config, timeout=2.0: {"role": "live", "account_key": "live_real", "trading_allowed": True},
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._query_qmt_snapshot",
        lambda config: {
            "fund": {"assetBalance": 800000.0, "marketValue": 300000.0, "enableBalance": 500000.0},
            "positions": [],
            "orders": [
                {
                    "orderId": "L9001",
                    "stockCode": "000001",
                    "stockName": "平安银行",
                    "orderType": "buy",
                    "orderStatus": "submitted",
                    "orderPrice": 12.4,
                    "orderVolume": 100,
                    "tradedVolume": 0,
                    "orderTime": "2026-04-22 10:00:00",
                }
            ],
            "trades": [],
            "asset": {"cash": 500000.0},
        },
    )
    monkeypatch.setattr("api.services.qmt_virtual_account_service._fetch_live_quotes", lambda symbols, **kwargs: {})
    response = client.post(
        "/v1/virtual-warehouse/qmt/orders",
        headers=headers,
        json={
            "account_key": "live_real",
            "symbol": "000001.SZ",
            "side": "buy",
            "quantity": 100,
            "price": 12.4,
            "price_type": "limit",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["order_result"]["order_id"] == "L9001"
    assert payload["overview"]["orders"][0]["order_id"] == "L9001"
    assert called["submit"] is True


def test_qmt_bulk_sell_rejects_live_account_even_when_trading_allowed(monkeypatch):
    called = {"overview": False, "thread": False}
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="live_real",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="8886186680",
                account_type="STOCK",
                account_name="QMT 实盘仓",
                userdata_path="",
                role="live",
                bridge_base_url="http://127.0.0.1:8711",
                bridge_token="bridge-token",
                refresh_interval_seconds=10,
            )
        ],
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._fetch_qmt_bridge_health",
        lambda config, timeout=2.0: {"role": "live", "account_key": "live_real", "trading_allowed": True},
    )

    def fake_overview(*args, **kwargs):
        called["overview"] = True
        return {
            "positions": [{"symbol": "000001.SZ", "name": "平安银行", "available_position": 100}],
        }

    class FakeThread:
        def __init__(self, *args, **kwargs):
            called["thread"] = True

        def start(self):
            return None

    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service.get_qmt_virtual_account_overview",
        fake_overview,
    )
    monkeypatch.setattr("api.services.qmt_virtual_account_service.threading.Thread", FakeThread)

    with get_db_ctx() as db:
        try:
            qmt_virtual_account_service.create_qmt_bulk_sell_task(
                db,
                "user-live-bulk-sell",
                account_key="live_real",
                strategy_name="test",
            )
        except RuntimeError as exc:
            assert "实盘账户不支持一键卖出全部持仓" in str(exc)
        else:
            raise AssertionError("live account bulk sell should be rejected")

    assert called["overview"] is False
    assert called["thread"] is False


def test_qmt_submit_order_rejects_readonly_bridge(monkeypatch):
    client = _get_client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}
    called = {"submit": False}

    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="live_real",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="8886186680",
                account_type="STOCK",
                account_name="QMT 实盘仓",
                userdata_path="",
                role="live",
                bridge_base_url="http://127.0.0.1:8711",
                bridge_token="bridge-token",
                refresh_interval_seconds=10,
            )
        ],
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._fetch_qmt_bridge_health",
        lambda config, timeout=2.0: {"role": "live", "account_key": "live_real", "trading_allowed": False},
    )

    def fake_submit(*args, **kwargs):
        called["submit"] = True
        return {"success": True, "order_id": "L9001"}

    monkeypatch.setattr("api.services.qmt_virtual_account_service._submit_qmt_order", fake_submit)
    response = client.post(
        "/v1/virtual-warehouse/qmt/orders",
        headers=headers,
        json={
            "account_key": "live_real",
            "symbol": "000001.SZ",
            "side": "buy",
            "quantity": 100,
            "price": 12.4,
            "price_type": "limit",
        },
    )
    assert response.status_code == 400
    assert "bridge 当前为只读状态" in response.json()["detail"]
    assert called["submit"] is False


def test_qmt_cancel_order_route(monkeypatch):
    client = _get_client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}

    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="paper_bridge",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="39027628",
                account_type="STOCK",
                account_name="QMT Bridge 模拟仓",
                userdata_path="",
                role="paper",
                bridge_base_url="http://127.0.0.1:8710",
                bridge_token="bridge-token",
                refresh_interval_seconds=10,
            )
        ],
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._cancel_qmt_order",
        lambda config, **kwargs: {
            "success": True,
            "order_id": kwargs["order_id"],
            "result": 0,
        },
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._fetch_qmt_bridge_health",
        lambda config, timeout=2.0: {"role": "paper", "account_key": "paper_bridge", "trading_allowed": True},
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._query_qmt_snapshot",
        lambda config: {
            "fund": {"assetBalance": 800000.0, "marketValue": 300000.0, "enableBalance": 500000.0},
            "positions": [],
            "orders": [
                {
                    "orderId": "O9001",
                    "stockCode": "000001",
                    "stockName": "平安银行",
                    "orderType": "buy",
                    "orderStatus": "cancelled",
                    "orderPrice": 12.4,
                    "orderVolume": 1000,
                    "tradedVolume": 0,
                    "orderTime": "2026-04-22 10:00:00",
                }
            ],
            "trades": [],
            "asset": {"cash": 500000.0},
        },
    )
    monkeypatch.setattr("api.services.qmt_virtual_account_service._fetch_live_quotes", lambda symbols, **kwargs: {})

    response = client.post(
        "/v1/virtual-warehouse/qmt/orders/O9001/cancel?account_key=paper_bridge",
        headers=headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["cancel_result"]["order_id"] == "O9001"
    assert payload["overview"]["orders"][0]["status"] == "cancelled"


def test_qmt_cancel_order_allows_live_account(monkeypatch):
    client = _get_client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}
    called = {"cancel": False}

    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._runtime_configs",
        lambda: [
            QmtRuntimeConfig(
                key="live_real",
                enabled=True,
                host="192.168.10.1",
                port=58610,
                account_id="8886186680",
                account_type="STOCK",
                account_name="QMT 实盘仓",
                userdata_path="",
                role="live",
                bridge_base_url="http://127.0.0.1:8711",
                bridge_token="bridge-token",
                refresh_interval_seconds=10,
            )
        ],
    )

    def fake_cancel(*args, **kwargs):
        called["cancel"] = True
        return {"success": True, "order_id": kwargs["order_id"]}

    monkeypatch.setattr("api.services.qmt_virtual_account_service._cancel_qmt_order", fake_cancel)
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._fetch_qmt_bridge_health",
        lambda config, timeout=2.0: {"role": "live", "account_key": "live_real", "trading_allowed": True},
    )
    monkeypatch.setattr(
        "api.services.qmt_virtual_account_service._query_qmt_snapshot",
        lambda config: {
            "fund": {"assetBalance": 800000.0, "marketValue": 300000.0, "enableBalance": 500000.0},
            "positions": [],
            "orders": [
                {
                    "orderId": "O9001",
                    "stockCode": "000001",
                    "stockName": "平安银行",
                    "orderType": "buy",
                    "orderStatus": "cancelled",
                    "orderPrice": 12.4,
                    "orderVolume": 1000,
                    "tradedVolume": 0,
                    "orderTime": "2026-04-22 10:00:00",
                }
            ],
            "trades": [],
            "asset": {"cash": 500000.0},
        },
    )
    monkeypatch.setattr("api.services.qmt_virtual_account_service._fetch_live_quotes", lambda symbols, **kwargs: {})
    response = client.post(
        "/v1/virtual-warehouse/qmt/orders/O9001/cancel?account_key=live_real",
        headers=headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["cancel_result"]["order_id"] == "O9001"
    assert payload["overview"]["orders"][0]["status"] == "cancelled"
    assert called["cancel"] is True


def test_qmt_history_bridge_uses_paper_account_key(monkeypatch):
    monkeypatch.delenv("QMT_MINUTE_HISTORY_ACCOUNT_KEY", raising=False)
    monkeypatch.delenv("QMT_HISTORY_BRIDGE_BASE_URL", raising=False)
    monkeypatch.setenv("QMT_HISTORY_ACCOUNT_KEY", "paper_sim")
    fake_settings = SimpleNamespace(
        qmt_history_account_key="paper_sim",
        qmt_accounts=lambda: [
            {
                "key": "live_real",
                "enabled": True,
                "role": "live",
                "account_id": "8886186680",
                "bridge_base_url": "http://192.168.10.1:8711",
                "bridge_token": "live-token",
            },
            {
                "key": "paper_sim",
                "enabled": True,
                "role": "paper",
                "account_id": "39027628",
                "bridge_base_url": "http://192.168.10.1:8710",
                "bridge_token": "paper-token",
            },
        ],
        qmt_accounts_json="[]",
        qmt_default_account_key="paper_sim",
        qmt_bridge_base_url="",
        qmt_bridge_token="",
        qmt_account_id="",
    )
    monkeypatch.setattr("api.data_downloader.settings", fake_settings)

    bridge = DataDownloader._resolve_qmt_history_bridge()

    assert bridge is not None
    assert bridge["account_key"] == "paper_sim"
    assert bridge["role"] == "paper"
    assert bridge["bridge_base_url"].endswith(":8710")


def test_qmt_history_bridge_rejects_live_history_key(monkeypatch):
    monkeypatch.delenv("QMT_MINUTE_HISTORY_ACCOUNT_KEY", raising=False)
    monkeypatch.delenv("QMT_HISTORY_BRIDGE_BASE_URL", raising=False)
    monkeypatch.setenv("QMT_HISTORY_ACCOUNT_KEY", "live_real")
    fake_settings = SimpleNamespace(
        qmt_history_account_key="paper_sim",
        qmt_accounts=lambda: [
            {
                "key": "live_real",
                "enabled": True,
                "role": "live",
                "account_id": "8886186680",
                "bridge_base_url": "http://192.168.10.1:8711",
                "bridge_token": "live-token",
            }
        ],
        qmt_accounts_json="[]",
        qmt_default_account_key="paper_sim",
        qmt_bridge_base_url="",
        qmt_bridge_token="",
        qmt_account_id="",
    )
    monkeypatch.setattr("api.data_downloader.settings", fake_settings)

    assert DataDownloader._resolve_qmt_history_bridge() is None


def test_qmt_history_bridge_rejects_explicit_live_port(monkeypatch):
    monkeypatch.delenv("QMT_MINUTE_HISTORY_ACCOUNT_KEY", raising=False)
    monkeypatch.setenv("QMT_HISTORY_BRIDGE_BASE_URL", "http://192.168.10.1:8711")
    monkeypatch.setenv("QMT_HISTORY_ACCOUNT_KEY", "paper_sim")

    assert DataDownloader._resolve_qmt_history_bridge() is None


def test_list_paper_accounts_endpoint():
    client = _get_client()
    token = _auth(client)
    headers = {"Authorization": f"Bearer {token}"}

    account_id = f"paper-list-{uuid4().hex[:8]}"
    create_response = client.post(
        "/v1/paper/accounts",
        headers=headers,
        json={"id": account_id, "name": "纸交易列表测试账户", "initial_capital": 500000},
    )
    assert create_response.status_code == 200

    list_response = client.get("/v1/paper/accounts", headers=headers)
    assert list_response.status_code == 200
    items = list_response.json()["items"]
    assert any(item["id"] == account_id for item in items)
