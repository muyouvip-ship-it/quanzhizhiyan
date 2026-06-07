from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from api.services import qmt_sync_scheduler_service
from api.services.qmt_virtual_account_service import QmtRuntimeConfig


def test_should_run_accepts_naive_last_synced_at():
    now = datetime(2026, 5, 8, 6, 0, 40, tzinfo=timezone.utc)
    local_last_synced_at = datetime(2026, 5, 8, 14, 0, 0)
    row = SimpleNamespace(last_synced_at=local_last_synced_at, sync_interval_seconds=30)
    assert qmt_sync_scheduler_service._should_run(row, now) is True


def test_should_run_respects_interval_for_aware_last_synced_at():
    now = datetime.now(timezone.utc)
    row = SimpleNamespace(last_synced_at=now - timedelta(seconds=5), sync_interval_seconds=30)
    assert qmt_sync_scheduler_service._should_run(row, now) is False


def test_run_single_profile_skips_disabled_account(monkeypatch):
    committed = {"value": False}

    class FakeDB:
        def add(self, row):
            pass

        def commit(self):
            committed["value"] = True

    row = SimpleNamespace(
        user_id="user-1",
        account_key="paper_sim",
        last_synced_at=None,
        last_status=None,
        last_error=None,
    )
    disabled_config = QmtRuntimeConfig(
        key="paper_sim",
        enabled=False,
        host="127.0.0.1",
        port=58610,
        account_id="",
        account_type="STOCK",
        account_name="paper",
        userdata_path="",
        role="paper",
        bridge_base_url="",
        bridge_token="",
        refresh_interval_seconds=10,
    )
    monkeypatch.setattr(
        qmt_sync_scheduler_service.qmt_virtual_account_service,
        "_resolve_runtime_config",
        lambda account_key, db=None, user_id=None: disabled_config,
    )
    monkeypatch.setattr(
        qmt_sync_scheduler_service.qmt_virtual_account_service,
        "get_qmt_virtual_account_overview",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("overview should not be queried")),
    )

    now = datetime(2026, 5, 8, 6, 0, tzinfo=timezone.utc)
    qmt_sync_scheduler_service._run_single_profile(FakeDB(), row, now)

    assert committed["value"] is True
    assert row.last_synced_at == now
    assert row.last_status == "skipped_disabled"
    assert "未启用" in row.last_error


def test_run_single_profile_treats_cached_overview_as_failed_sync(monkeypatch):
    committed = {"value": False}

    class FakeDB:
        def add(self, row):
            pass

        def commit(self):
            committed["value"] = True

    row = SimpleNamespace(
        user_id="user-1",
        account_key="live_real",
        last_synced_at=None,
        last_status=None,
        last_error=None,
        consecutive_failures=0,
        alert_on_disconnect=False,
    )
    config = QmtRuntimeConfig(
        key="live_real",
        enabled=True,
        host="127.0.0.1",
        port=58610,
        account_id="8886186680",
        account_type="STOCK",
        account_name="live",
        userdata_path="",
        role="live",
        bridge_base_url="http://127.0.0.1:8711",
        bridge_token="",
        refresh_interval_seconds=10,
    )
    monkeypatch.setattr(
        qmt_sync_scheduler_service.qmt_virtual_account_service,
        "_resolve_runtime_config",
        lambda account_key, db=None, user_id=None: config,
    )
    monkeypatch.setattr(
        qmt_sync_scheduler_service.qmt_virtual_account_service,
        "get_qmt_virtual_account_overview",
        lambda *args, **kwargs: {
            "data_source": "cache",
            "is_stale": True,
            "connection": {
                "connected": True,
                "message": "QMT bridge连接超时：http://127.0.0.1:8711，已回退到最近快照",
            },
        },
    )

    now = datetime(2026, 5, 8, 6, 0, tzinfo=timezone.utc)
    qmt_sync_scheduler_service._run_single_profile(FakeDB(), row, now)

    assert committed["value"] is True
    assert row.last_synced_at == now
    assert row.last_status == "failed"
    assert row.consecutive_failures == 1
    assert "bridge连接超时" in row.last_error


def test_run_single_profile_does_not_repeat_disconnect_alert(monkeypatch):
    class FakeDB:
        def add(self, row):
            pass

        def commit(self):
            pass

    row = SimpleNamespace(
        user_id="user-1",
        account_key="live_real",
        last_synced_at=None,
        last_status="failed",
        last_error="previous failure",
        consecutive_failures=3,
        alert_on_disconnect=True,
    )
    config = QmtRuntimeConfig(
        key="live_real",
        enabled=True,
        host="127.0.0.1",
        port=58610,
        account_id="8886186680",
        account_type="STOCK",
        account_name="live",
        userdata_path="",
        role="live",
        bridge_base_url="http://127.0.0.1:8711",
        bridge_token="",
        refresh_interval_seconds=10,
    )
    alerts: list[str] = []
    monkeypatch.setattr(
        qmt_sync_scheduler_service.qmt_virtual_account_service,
        "_resolve_runtime_config",
        lambda account_key, db=None, user_id=None: config,
    )
    monkeypatch.setattr(
        qmt_sync_scheduler_service.qmt_virtual_account_service,
        "get_qmt_virtual_account_overview",
        lambda *args, **kwargs: {"data_source": "cache", "connection": {"connected": False, "message": "QMT bridge不可达"}},
    )
    monkeypatch.setattr(
        qmt_sync_scheduler_service,
        "_maybe_send_disconnect_alert",
        lambda *args, **kwargs: alerts.append("disconnect"),
    )

    now = datetime(2026, 5, 8, 6, 0, tzinfo=timezone.utc)
    qmt_sync_scheduler_service._run_single_profile(FakeDB(), row, now)

    assert row.last_status == "failed"
    assert row.consecutive_failures == 4
    assert alerts == []


def test_run_single_profile_sends_reconnect_alert_after_failure(monkeypatch):
    class FakeDB:
        def add(self, row):
            pass

        def commit(self):
            pass

    row = SimpleNamespace(
        user_id="user-1",
        account_key="live_real",
        last_synced_at=None,
        last_status="failed",
        last_error="QMT bridge不可达",
        consecutive_failures=2,
        alert_on_disconnect=True,
    )
    config = QmtRuntimeConfig(
        key="live_real",
        enabled=True,
        host="127.0.0.1",
        port=58610,
        account_id="8886186680",
        account_type="STOCK",
        account_name="live",
        userdata_path="",
        role="live",
        bridge_base_url="http://127.0.0.1:8711",
        bridge_token="",
        refresh_interval_seconds=10,
    )
    alerts: list[str] = []
    monkeypatch.setattr(
        qmt_sync_scheduler_service.qmt_virtual_account_service,
        "_resolve_runtime_config",
        lambda account_key, db=None, user_id=None: config,
    )
    monkeypatch.setattr(
        qmt_sync_scheduler_service.qmt_virtual_account_service,
        "get_qmt_virtual_account_overview",
        lambda *args, **kwargs: {"data_source": "live", "connection": {"connected": True, "message": "ok"}},
    )
    monkeypatch.setattr(
        qmt_sync_scheduler_service,
        "_maybe_send_reconnect_alert",
        lambda *args, **kwargs: alerts.append("reconnect"),
    )

    now = datetime(2026, 5, 8, 6, 0, tzinfo=timezone.utc)
    qmt_sync_scheduler_service._run_single_profile(FakeDB(), row, now)

    assert row.last_status == "success"
    assert row.consecutive_failures == 0
    assert alerts == ["reconnect"]
