from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager

from api.core.settings import settings
from api.core.strategy_db import strategy_engine
from api.database import init_db
from api.job_store import get_job_store
from api.models.strategy_models import Base
from api.core.utils import env_flag as _env_flag
from api.services import (
    backtest_data_auto_update_service,
    daily_review_service,
    minute_kline_gap_filler_service,
    news_eye_service,
    qmt_market_sync_service,
    qmt_minute_subscription_service,
    qmt_sync_scheduler_service,
    realtime_monitor_service,
)


def _log(msg: str):
    from api.core.logging import logger

    logger.info(msg)



@asynccontextmanager
async def lifespan(app):
    """Initialize resources on startup and cleanup on shutdown."""
    init_db()
    _log("Database initialized.")

    # 初始化策略管理数据库（优先复用 PostgreSQL）
    Base.metadata.create_all(strategy_engine)
    _log("Strategy management database initialized.")

    store = get_job_store()
    if _env_flag("CLEAR_JOB_STORE_ON_STARTUP", "0"):
        store.clear()
        _log("Job store cleared on startup.")
    else:
        _log("Job store preserved on startup.")

    if not os.getenv("TA_APP_SECRET_KEY"):
        _log("=" * 70)
        _log("WARNING: TA_APP_SECRET_KEY is not set!")
        _log("Using hardcoded default key. ALL encryption and JWT signing")
        _log("is INSECURE. Set TA_APP_SECRET_KEY env var before production use.")
        _log("=" * 70)

    from tradingagents.dataflows.trade_calendar import _load_cn_trade_dates
    from api.core.stock_map import load_cn_stock_map, refresh_cn_stock_map_if_stale

    _load_cn_trade_dates()
    _log("Trade calendar pre-loaded.")
    load_cn_stock_map()
    _log("Stock map pre-loaded from local cache/fallback.")
    asyncio.create_task(asyncio.to_thread(refresh_cn_stock_map_if_stale), name="stock-map-refresh")
    qmt_sync_enabled = _env_flag("ENABLE_QMT_SYNC_WORKER", "0")
    qmt_market_enabled = _env_flag("ENABLE_QMT_MARKET_SYNC_WORKER", "0")
    qmt_minute_subscription_enabled = _env_flag(
        "ENABLE_QMT_MINUTE_SUBSCRIPTION_WORKER",
        "1" if qmt_market_enabled else "0",
    )
    backtest_auto_enabled = _env_flag("ENABLE_BACKTEST_AUTO_UPDATE_WORKER", "0")
    news_eye_enabled = _env_flag("ENABLE_NEWS_EYE_WORKER", "1")
    realtime_monitor_enabled = _env_flag("ENABLE_REALTIME_MONITOR_WORKER", "0")
    daily_review_enabled = _env_flag("ENABLE_DAILY_REVIEW_WORKER", "1")
    minute_gap_filler_enabled = _env_flag("ENABLE_MINUTE_KLINE_GAP_FILLER_WORKER", "0")

    if qmt_sync_enabled:
        await qmt_sync_scheduler_service.start_background_worker()
        _log("QMT sync background worker started.")
    else:
        _log("QMT sync background worker skipped.")
    if qmt_market_enabled:
        await qmt_market_sync_service.start_background_worker()
        _log("QMT market sync worker started.")
    else:
        _log("QMT market sync worker skipped.")
    if qmt_minute_subscription_enabled:
        await qmt_minute_subscription_service.start_background_worker()
        _log("QMT minute subscription worker started.")
    else:
        _log("QMT minute subscription worker skipped.")
    if backtest_auto_enabled:
        await backtest_data_auto_update_service.start_background_worker()
        _log("Backtest data auto update worker started.")
    else:
        _log("Backtest data auto update worker skipped.")
    if news_eye_enabled:
        await news_eye_service.start_background_worker()
        _log("News eye background worker started.")
    else:
        _log("News eye background worker skipped.")
    if realtime_monitor_enabled:
        await realtime_monitor_service.start_background_worker()
        _log("Realtime monitor background worker started.")
    else:
        _log("Realtime monitor background worker skipped.")
    if daily_review_enabled:
        await daily_review_service.start_background_worker()
        _log("Daily review background worker started.")
    else:
        _log("Daily review background worker skipped.")
    if minute_gap_filler_enabled:
        await minute_kline_gap_filler_service.start_background_worker()
        _log("Minute kline gap filler worker started.")
    else:
        _log("Minute kline gap filler worker skipped.")
    yield
    if minute_gap_filler_enabled:
        await minute_kline_gap_filler_service.stop_background_worker()
        _log("Minute kline gap filler worker stopped.")
    if daily_review_enabled:
        await daily_review_service.stop_background_worker()
        _log("Daily review background worker stopped.")
    if realtime_monitor_enabled:
        await realtime_monitor_service.stop_background_worker()
        _log("Realtime monitor background worker stopped.")
    if news_eye_enabled:
        await news_eye_service.stop_background_worker()
        _log("News eye background worker stopped.")
    if qmt_minute_subscription_enabled:
        await qmt_minute_subscription_service.stop_background_worker()
        _log("QMT minute subscription worker stopped.")
    if qmt_market_enabled:
        await qmt_market_sync_service.stop_background_worker()
        _log("QMT market sync worker stopped.")
    if backtest_auto_enabled:
        await backtest_data_auto_update_service.stop_background_worker()
        _log("Backtest data auto update worker stopped.")
    if qmt_sync_enabled:
        await qmt_sync_scheduler_service.stop_background_worker()
    _log("Shutting down: Cleaning up resources...")
