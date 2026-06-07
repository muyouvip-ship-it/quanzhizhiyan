"""收盘后/盘前自动补齐 1 分钟 K 线缺口。

执行逻辑：
- 在交易日收盘后（15:30 之后）扫描股票分钟线缺口，
  通过 pytdx 连接通达信拉取缺失数据并 upsert 入库。
- 适用于 QMT 分钟同步通道不可用时的兜底方案。
- 通过 ``ENABLE_MINUTE_KLINE_GAP_FILLER_WORKER=1`` 环境变量启用。
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from api.database import SessionLocal
from sqlalchemy import text

from api.core.utils import env_flag as _env_flag
from tradingagents.dataflows.trade_calendar import CN_TZ, is_cn_trading_day

logger = logging.getLogger(__name__)

_TASK: asyncio.Task | None = None
_STOP_EVENT: asyncio.Event | None = None
_POLL_INTERVAL = 300  # 5 分钟检测一次
_MIN_RUN_INTERVAL = timedelta(minutes=90)  # 同一天内至少间隔 90 分钟
_EXECUTOR: "ThreadPoolExecutor | None" = None

ROOT = Path(__file__).resolve().parents[2]
SYNC_SCRIPT = ROOT / "scripts" / "fill_minute_kline_gaps.py"


def is_worker_enabled() -> bool:
    return _env_flag("ENABLE_MINUTE_KLINE_GAP_FILLER_WORKER", "0")


def is_worker_running() -> bool:
    return bool(_TASK is not None and not _TASK.done())


async def start_background_worker() -> None:
    global _TASK, _STOP_EVENT
    if _TASK and not _TASK.done():
        logger.warning("[minute-gap-filler] worker already running, skipping")
        return
    _STOP_EVENT = asyncio.Event()
    _TASK = asyncio.create_task(_run_loop(), name="minute-kline-gap-filler")
    logger.info("[minute-gap-filler] background worker started")


async def stop_background_worker() -> None:
    global _TASK, _STOP_EVENT
    if _STOP_EVENT is not None:
        _STOP_EVENT.set()
    if _TASK is not None:
        try:
            await _TASK
        except Exception:
            logger.exception("[minute-gap-filler] stop worker failed")
    _TASK = None
    _STOP_EVENT = None


async def _run_loop() -> None:
    """Main loop: run the gap fill once on startup, then poll periodically."""
    logger.info("[minute-gap-filler] loop starting")
    # 启动后先尝试跑一次（仅当在合适时间段内）
    try:
        await asyncio.to_thread(_fill_minute_gaps_once)
    except Exception:
        logger.exception("[minute-gap-filler] initial run failed")

    while _STOP_EVENT is not None and not _STOP_EVENT.is_set():
        try:
            await asyncio.wait_for(_STOP_EVENT.wait(), timeout=_POLL_INTERVAL)
        except asyncio.TimeoutError:
            pass
        try:
            if _can_run_now():
                await asyncio.to_thread(_fill_minute_gaps_once)
        except Exception:
            logger.exception("[minute-gap-filler] loop iteration failed")

    logger.info("[minute-gap-filler] loop stopped")


def _can_run_now() -> bool:
    """判断当前是否适合执行补齐，条件：
    1. 时间在收盘后（15:30 ~ 23:59）或盘前（06:00 ~ 09:00）
    2. 当前水印允许（同窗口内至少间隔 _MIN_RUN_INTERVAL）
    备注：周末/节假日时即便不是交易日，也允许跑一次补齐前一交易日缺口；
    若库里无缺失，子脚本会自然返回 0 行。
    """
    now = datetime.now(tz=CN_TZ)
    today = now.strftime("%Y-%m-%d")
    minute_of_day = now.hour * 60 + now.minute
    # 收盘窗口：15:30 ~ 23:59
    if 930 <= minute_of_day <= 1439:
        return _should_run_by_watermark(today)
    # 盘前窗口：06:00 ~ 09:00
    if 360 <= minute_of_day < 540:
        return _should_run_by_watermark(today)
    return False


def _should_run_by_watermark(today: str) -> bool:
    """检查本天内是否已经跑过，防止重复执行。"""
    try:
        with SessionLocal() as db:
            row = db.execute(
                text(
                    "SELECT last_run_started_at FROM minute_kline_gap_filler_watermarks "
                    "WHERE trade_date = :trade_date ORDER BY id DESC LIMIT 1"
                ),
                {"trade_date": today},
            ).fetchone()
            if row is None:
                return True
            last_run = row[0]
            if last_run.tzinfo is None:
                last_run = last_run.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            return (now - last_run) >= _MIN_RUN_INTERVAL
    except Exception:
        logger.warning("[minute-gap-filler] watermark check failed, will run anyway", exc_info=True)
        return True


def _touch_watermark(today: str) -> None:
    """记录本次执行时间水印。"""
    try:
        with SessionLocal() as db:
            db.execute(
                text(
                    "INSERT INTO minute_kline_gap_filler_watermarks "
                    "(trade_date, last_run_started_at) VALUES (:trade_date, :now)"
                ),
                {"trade_date": today, "now": datetime.now(timezone.utc)},
            )
            db.commit()
    except Exception:
        logger.warning("[minute-gap-filler] touch watermark failed", exc_info=True)


def _ensure_watermark_table(db) -> None:
    """确保水印表存在。"""
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS minute_kline_gap_filler_watermarks (
                id SERIAL PRIMARY KEY,
                trade_date DATE NOT NULL,
                last_run_started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    )
    db.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_minute_gap_filler_trade_date "
            "ON minute_kline_gap_filler_watermarks (trade_date)"
        )
    )
    db.commit()


def _fill_minute_gaps_once() -> None:
    """执行一次补齐。

    补齐最近 14 个交易日（约~3周）的分钟线缺口。
    """
    import subprocess

    logger.info("[minute-gap-filler] starting gap fill run")
    started = time.time()

    with SessionLocal() as db:
        _ensure_watermark_table(db)

    # 计算需要补齐的时间范围
    end_d = date.today()
    start_d = end_d - timedelta(days=21)  # 补最近 3 周的缺口

    workers = os.getenv("MINUTE_GAP_FILLER_WORKERS", "8")
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT) + ":" + env.get("PYTHONPATH", "")

    cmd = [
        sys.executable,
        str(SYNC_SCRIPT),
        "--start-date", start_d.isoformat(),
        "--end-date", end_d.isoformat(),
        "--workers", workers,
    ]

    logger.info("[minute-gap-filler] running: %s", " ".join(cmd))

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 小时超时
            env=env,
        )
        elapsed = time.time() - started

        # 子脚本日志同时写到 stdout/stderr 与本地 .log 文件，
        # 我们只挑关键行（缺口数 / 需补齐 / 完成）记到 service 日志。
        combined_lines = (result.stdout or "").splitlines() + (result.stderr or "").splitlines()
        summary_keywords = ("缺口 (symbol", "需要补齐的股票数", "完成!")
        printed = 0
        for line in combined_lines:
            stripped = line.strip()
            if any(k in stripped for k in summary_keywords):
                logger.info("[minute-gap-filler] %s", stripped)
                printed += 1
        if printed == 0:
            tail = "\n".join(combined_lines[-5:]) if combined_lines else "(empty)"
            logger.info(
                "[minute-gap-filler] finished (rc=%s, elapsed=%.1fmin, tail=%s)",
                result.returncode, elapsed / 60, tail,
            )
        else:
            logger.info("[minute-gap-filler] finished (rc=%s, elapsed=%.1fmin)", result.returncode, elapsed / 60)

        if result.returncode != 0:
            err_tail = (result.stderr or "")[-500:]
            logger.error("[minute-gap-filler] non-zero exit (rc=%s) stderr_tail=%s", result.returncode, err_tail)

        today_str = end_d.isoformat()
        _touch_watermark(today_str)

    except subprocess.TimeoutExpired:
        logger.error("[minute-gap-filler] timeout after 3600s")
    except Exception:
        logger.exception("[minute-gap-filler] run failed")
