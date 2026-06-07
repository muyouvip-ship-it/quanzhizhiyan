from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy.orm import Session

from api.database import QmtSyncProfileDB, SessionLocal, UserDB
from api.services import auth_service, qmt_virtual_account_service
from api.services.wecom_notification_service import send_message
from tradingagents.dataflows.trade_calendar import CN_TZ


logger = logging.getLogger(__name__)
_SYNC_TASK: asyncio.Task | None = None
_STOP_EVENT: asyncio.Event | None = None
_POLL_SECONDS = 5


def list_sync_profiles(db: Session, user_id: str) -> list[dict]:
    rows = (
        db.query(QmtSyncProfileDB)
        .filter(QmtSyncProfileDB.user_id == user_id)
        .order_by(QmtSyncProfileDB.created_at.asc())
        .all()
    )
    return [_to_dict(row) for row in rows]


def upsert_sync_profile(
    db: Session,
    user_id: str,
    account_key: str,
    *,
    is_active: bool,
    sync_interval_seconds: int = 30,
    sync_tracking_board: bool = True,
    alert_on_disconnect: bool = True,
) -> dict:
    profile = (
        db.query(QmtSyncProfileDB)
        .filter(QmtSyncProfileDB.user_id == user_id, QmtSyncProfileDB.account_key == account_key)
        .first()
    )
    if profile is None:
        profile = QmtSyncProfileDB(
            id=uuid4().hex,
            user_id=user_id,
            account_key=account_key,
            created_at=datetime.now(timezone.utc),
        )
        db.add(profile)
    profile.is_active = bool(is_active)
    profile.sync_interval_seconds = max(int(sync_interval_seconds or 30), 10)
    profile.sync_tracking_board = False
    profile.alert_on_disconnect = bool(alert_on_disconnect)
    profile.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(profile)
    return _to_dict(profile)


def ensure_default_profile(
    db: Session,
    user_id: str,
    account_key: str,
) -> dict:
    existing = (
        db.query(QmtSyncProfileDB)
        .filter(QmtSyncProfileDB.user_id == user_id, QmtSyncProfileDB.account_key == account_key)
        .first()
    )
    if existing is not None:
        return _to_dict(existing)
    return upsert_sync_profile(
        db,
        user_id,
        account_key,
        is_active=False,
        sync_interval_seconds=30,
        sync_tracking_board=False,
        alert_on_disconnect=True,
    )


async def start_background_worker() -> None:
    global _SYNC_TASK, _STOP_EVENT
    if _SYNC_TASK and not _SYNC_TASK.done():
        return
    _STOP_EVENT = asyncio.Event()
    _SYNC_TASK = asyncio.create_task(_run_loop(), name="qmt-auto-sync")


async def stop_background_worker() -> None:
    global _SYNC_TASK, _STOP_EVENT
    if _STOP_EVENT is not None:
        _STOP_EVENT.set()
    if _SYNC_TASK is not None:
        try:
            await _SYNC_TASK
        except Exception:
            logger.exception("[qmt-sync] stop background worker failed")
    _SYNC_TASK = None
    _STOP_EVENT = None


async def _run_loop() -> None:
    logger.info("[qmt-sync] background worker started")
    while _STOP_EVENT is not None and not _STOP_EVENT.is_set():
        try:
            await asyncio.to_thread(_scan_and_run_once)
        except Exception:
            logger.exception("[qmt-sync] loop iteration failed")
        try:
            await asyncio.wait_for(_STOP_EVENT.wait(), timeout=_POLL_SECONDS)
        except asyncio.TimeoutError:
            pass
    logger.info("[qmt-sync] background worker stopped")


def _scan_and_run_once() -> None:
    with SessionLocal() as db:
        rows = (
            db.query(QmtSyncProfileDB)
            .filter(QmtSyncProfileDB.is_active == True)
            .all()
        )
        now = datetime.now(timezone.utc)
        for row in rows:
            if not _should_run(row, now):
                continue
            _run_single_profile(db, row, now)


def _should_run(row: QmtSyncProfileDB, now: datetime) -> bool:
    if row.last_synced_at is None:
        return True
    interval_seconds = max(int(row.sync_interval_seconds or 30), 10)
    return (now - _ensure_utc(row.last_synced_at)) >= timedelta(seconds=interval_seconds)


def _run_single_profile(db: Session, row: QmtSyncProfileDB, now: datetime) -> None:
    previous_status = str(getattr(row, "last_status", None) or "").strip().lower()
    previous_failures = int(getattr(row, "consecutive_failures", 0) or 0)
    try:
        config = qmt_virtual_account_service._resolve_runtime_config(
            row.account_key,
            db=db,
            user_id=row.user_id,
        )
        if not config.enabled:
            row.last_synced_at = now
            row.last_status = "skipped_disabled"
            row.last_error = "当前账户未启用，后台同步已跳过。"
            db.add(row)
            db.commit()
            logger.info("[qmt-sync] skipped disabled profile user=%s account=%s", row.user_id, row.account_key)
            return
        overview = qmt_virtual_account_service.get_qmt_virtual_account_overview(
            db,
            row.user_id,
            account_key=row.account_key,
            sync_to_imports=False,
        )
        connected = bool((overview.get("connection") or {}).get("connected"))
        if not connected:
            raise RuntimeError((overview.get("connection") or {}).get("message") or "QMT 未连接")
        data_source = str(overview.get("data_source") or "")
        if data_source != "live":
            message = str((overview.get("connection") or {}).get("message") or "").strip()
            raise RuntimeError(message or f"QMT 未完成实时同步，当前数据源为 {data_source or 'unknown'}")
        row.last_synced_at = now
        row.last_status = "success"
        row.last_error = None
        row.consecutive_failures = 0
        db.add(row)
        db.commit()
        logger.info("[qmt-sync] synced user=%s account=%s", row.user_id, row.account_key)
        if row.alert_on_disconnect and (previous_status == "failed" or previous_failures > 0):
            _maybe_send_reconnect_alert(db, row, now)
    except Exception as exc:
        row.last_synced_at = now
        row.last_status = "failed"
        row.last_error = str(exc)
        row.consecutive_failures = int(row.consecutive_failures or 0) + 1
        db.add(row)
        db.commit()
        logger.warning("[qmt-sync] sync failed user=%s account=%s error=%s", row.user_id, row.account_key, exc)
        if row.alert_on_disconnect and _should_send_disconnect_alert(previous_status, previous_failures):
            _maybe_send_disconnect_alert(db, row, now, str(exc))


def _should_send_disconnect_alert(previous_status: str, previous_failures: int) -> bool:
    if previous_status == "failed":
        return False
    return int(previous_failures or 0) <= 0


def _maybe_send_disconnect_alert(db: Session, row: QmtSyncProfileDB, now: datetime, error_text: str) -> None:
    session_note = _qmt_session_note(now)
    lines = [
        "量化之神 QMT 自动同步失联",
        f"账户 Key：{row.account_key}",
        f"错误：{error_text[:300]}",
    ]
    if session_note:
        lines.append(session_note)
    _send_qmt_sync_alert(db, row, now, "\n".join(lines))


def _maybe_send_reconnect_alert(db: Session, row: QmtSyncProfileDB, now: datetime) -> None:
    _send_qmt_sync_alert(
        db,
        row,
        now,
        "\n".join(
            [
                "量化之神 QMT 自动同步已恢复",
                f"账户 Key：{row.account_key}",
                "状态：QMT 已重新连接，并完成实时同步。",
            ]
        ),
    )


def _send_qmt_sync_alert(db: Session, row: QmtSyncProfileDB, now: datetime, message: str) -> None:
    user = db.query(UserDB).filter(UserDB.id == row.user_id).first()
    if not user or not bool(user.wecom_report_enabled):
        return
    user_cfg = auth_service.get_user_llm_config(db, row.user_id)
    webhook_url = auth_service.decrypt_secret(getattr(user_cfg, "wecom_webhook_encrypted", None)) if user_cfg else None
    if not webhook_url:
        return
    payload = f"{message}\n用户：{getattr(user, 'email', row.user_id)}"
    try:
        if send_message(payload, webhook_url):
            row.last_alerted_at = now
            db.add(row)
            db.commit()
    except Exception:
        logger.exception("[qmt-sync] send alert failed")


def _qmt_session_note(now: datetime) -> str | None:
    local_now = _ensure_utc(now).astimezone(CN_TZ)
    weekday = local_now.weekday()
    current_time = local_now.time()
    in_trading_window = weekday < 5 and (
        (current_time.hour == 9 and current_time.minute >= 30)
        or (10 <= current_time.hour < 11)
        or (current_time.hour == 11 and current_time.minute <= 30)
        or (13 <= current_time.hour < 15)
    )
    if in_trading_window:
        return None
    return "提示：当前不在 A 股连续竞价时段，QMT/bridge 离线可能是正常状态；本次失联只按状态变化提醒一次。"


def _to_dict(row: QmtSyncProfileDB) -> dict:
    return {
        "id": row.id,
        "user_id": row.user_id,
        "account_key": row.account_key,
        "is_active": bool(row.is_active),
        "sync_interval_seconds": int(row.sync_interval_seconds or 30),
        "sync_tracking_board": bool(row.sync_tracking_board),
        "alert_on_disconnect": bool(row.alert_on_disconnect),
        "last_synced_at": row.last_synced_at.isoformat() if row.last_synced_at else None,
        "last_status": row.last_status,
        "last_error": row.last_error,
        "consecutive_failures": int(row.consecutive_failures or 0),
        "last_alerted_at": row.last_alerted_at.isoformat() if row.last_alerted_at else None,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=CN_TZ).astimezone(timezone.utc)
    return value.astimezone(timezone.utc)
