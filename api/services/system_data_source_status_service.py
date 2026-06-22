from __future__ import annotations

import json
import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from api.services.data_source_governance import list_news_source_links


DAILY_RAW_TABLES = {
    "quantclass": "raw_stock_daily_kline_quantclass",
    "tdx": "raw_stock_daily_kline_tdx",
    "akshare": "raw_stock_daily_kline_akshare",
    "baostock": "raw_stock_daily_kline_baostock",
    "efinance": "raw_stock_daily_kline_efinance",
    "postgresql": "raw_stock_daily_kline_postgresql",
}

MINUTE_RAW_TABLES = {
    "qmt": "raw_stock_minute_kline_qmt",
    "tdx": "raw_stock_minute_kline_tdx",
    "akshare": "raw_stock_minute_kline_akshare",
    "postgresql": "raw_stock_minute_kline_postgresql",
}


def build_system_data_update_overview(db: Session, *, user_id: str | None = None) -> dict[str, Any]:
    """Build read-only cards for Settings data-source observability."""
    daily_config = _latest_backtest_config(db, data_type="daily_kline")
    minute_config = _latest_backtest_config(db, data_type="minute_kline")
    cards = [
        _build_news_card(db),
        _build_stock_daily_card(db, daily_config),
        _build_stock_minute_card(db, minute_config),
        _build_index_market_card(db),
        _build_qmt_runtime_card(db),
        _build_daily_review_card(db, user_id=user_id),
    ]
    return {
        "cards": cards,
        "workers": _build_worker_status(),
    }


def _build_news_card(db: Session) -> dict[str, Any]:
    state = _query_one(
        db,
        """
        SELECT status, last_run_at, last_success_at, last_error,
               active_sources_json, saved_count, new_count, updated_count,
               unchanged_count, fresh_event_count, updated_at
        FROM market_news_sync_state
        WHERE worker_name = 'news_eye'
        """,
        table_name="market_news_sync_state",
    )
    active_sources = _loads_list(state.get("active_sources_json") if state else None)
    links = list_news_source_links()
    link_sources = [
        {"key": item.get("key"), "label": item.get("name"), "url": item.get("url")}
        for item in links
    ]
    worker_enabled = _env_flag("ENABLE_NEWS_EYE_WORKER", "1")
    last_success = state.get("last_success_at") if state else None
    return _card(
        card_id="news-eye",
        title="新闻资讯",
        category="news",
        source_label="外部资讯链接 + 本地资讯缓存",
        status_label="已更新" if last_success else ("等待首次更新" if worker_enabled else "后台未开启"),
        status_tone="good" if last_success else ("warn" if worker_enabled else "bad"),
        schedule=f"后台轮询每 {_env_int('NEWS_EYE_POLL_SECONDS', 45)} 秒检查一次，缓存保留 {_env_int('NEWS_EYE_RETENTION_DAYS', 7)} 天",
        mechanism="News Eye worker 并发抓取外部资讯源，去重后写入 market_news_items；资讯之眼、题材刷新和每日复盘读取本地缓存。",
        tables=["market_news_items", "market_news_sync_state"],
        last_run_at=state.get("last_run_at") if state else None,
        last_success_at=last_success,
        last_updated_at=state.get("updated_at") if state else None,
        metrics=[
            _metric("外部链接", f"{len(links)} 个", "含 6 个全局资讯源和 1 个个股新闻补充源"),
            _metric("活跃源", f"{len(active_sources)} 个", "最近一次成功抓取返回的来源"),
            _metric("保存新闻", state.get("saved_count") if state else None),
            _metric("新入库", state.get("new_count") if state else None),
            _metric("鲜活事件", state.get("fresh_event_count") if state else None),
        ],
        sources=link_sources,
        notes=[
            "页面展示的是缓存层，新闻原始发布时间和入库时间不是同一个概念。",
            "外部站点可用性、限流和内容结构变化会影响当次抓取质量。",
        ],
    )


def _build_stock_daily_card(db: Session, config: dict[str, Any] | None) -> dict[str, Any]:
    stats = _latest_daily_stats(db, "stock_daily_kline")
    pub_sources = _source_counts_for_date(db, "pub_stock_daily_kline", stats.get("latest_date"))
    raw_sources = _raw_counts_for_date(db, DAILY_RAW_TABLES, stats.get("latest_date"), date_column="trade_date")
    chosen_source = _top_source(pub_sources) or (config or {}).get("data_source_preference") or "--"
    schedule_time = (config or {}).get("schedule_time") or "15:05"
    timezone_name = (config or {}).get("timezone") or "Asia/Shanghai"
    return _card(
        card_id="stock-daily-kline",
        title="股票日 K",
        category="market",
        source_label=f"TDX 主同步 + 量化小课堂富字段补充 -> stock_daily_kline",
        status_label="最终表有数据" if stats.get("row_count") else "最终表暂无数据",
        status_tone="good" if stats.get("row_count") else "warn",
        schedule=f"回测数据自动订阅按配置 {schedule_time}（{timezone_name}）触发；量化小课堂富字段补充默认 20:05 触发；失败后每 30 分钟重试；当前 worker：{_on_off(_env_flag('ENABLE_BACKTEST_AUTO_UPDATE_WORKER', '0'))}",
        mechanism="15:05 后优先用 TDX 快速同步当日基础日 K；20:05 后使用量化小课堂 stock-trading-data-pro 补充 pre_close、流通市值、总市值、申万行业等富字段；TDX/量化课堂/AkShare/Baostock/EFinance 统一进入 raw -> norm -> pub -> stock_daily_kline。",
        tables=[
            "raw_stock_daily_kline_*",
            "norm_stock_daily_kline",
            "pub_stock_daily_kline",
            "stock_daily_kline",
        ],
        last_run_at=(config or {}).get("last_run_at"),
        last_success_at=(config or {}).get("last_success_at"),
        last_updated_at=stats.get("last_updated_at") or (config or {}).get("updated_at"),
        watermark=stats.get("latest_date"),
        metrics=[
            _metric("最新交易日", stats.get("latest_date")),
            _metric("最终表行数", _format_int(stats.get("row_count"))),
            _metric("发布源", _source_display(chosen_source), _format_source_counts(pub_sources)),
            _metric("配置源", _source_display((config or {}).get("data_source_preference"))),
            _metric("补充源", "量化小课堂 20:05"),
            _metric("订阅开关", "已开启" if (config or {}).get("auto_download") else "未开启"),
        ],
        sources=_source_count_sources(raw_sources),
        notes=[
            "日线主行情发布优先级：tdx > quantclass > akshare > baostock > postgresql > efinance；量化小课堂会补齐主行情为空的富字段。",
            "每日复盘和市场总览读取最终业务表；raw/norm/pub 用于采集、审计和质量追踪。",
        ],
    )


def _build_stock_minute_card(db: Session, config: dict[str, Any] | None) -> dict[str, Any]:
    stats = _latest_minute_stats(db, "stock_minute_kline")
    raw_sources = _raw_counts_for_date(db, MINUTE_RAW_TABLES, stats.get("latest_date"), date_column="trade_time")
    watermark = _latest_watermark(db, "minute_kline")
    intraday_watermark = _latest_watermark(db, "minute_kline_intraday")
    return _card(
        card_id="stock-minute-kline",
        title="股票 1 分钟 K",
        category="market",
        source_label="TDX / QMT / AkShare -> stock_minute_kline",
        status_label="最终表有数据" if stats.get("row_count") else "最终表暂无数据",
        status_tone="good" if stats.get("row_count") else "warn",
        schedule=(
            "盘中 QMT 行情 worker 每 60 秒扫描；订阅采集约每 "
            f"{_env_int('AI_QUANT_MINUTE_SELECTION_REFRESH_INTERVAL_SECONDS', 55)} 秒刷新股票池；"
            f"缺口补齐 worker：{_on_off(_env_flag('ENABLE_MINUTE_KLINE_GAP_FILLER_WORKER', '0'))}；回测订阅默认 15:05 收盘后同步，失败后 30 分钟重试"
        ),
        mechanism="盘中 QMT 采集会写 stock_minute_kline 并同步 raw/pub；回测订阅可用 TDX 拉取近期全市场分钟线进入 raw/pub，QMT 历史 bridge 与 pytdx 缺口补齐脚本也可补缺。",
        tables=[
            "raw_stock_minute_kline_*",
            "norm_stock_minute_kline",
            "pub_stock_minute_kline",
            "stock_minute_kline",
        ],
        last_run_at=(intraday_watermark or watermark or {}).get("last_run_started_at"),
        last_success_at=(intraday_watermark or watermark or {}).get("last_success_at"),
        last_updated_at=stats.get("last_updated_at") or (intraday_watermark or watermark or {}).get("updated_at"),
        watermark=stats.get("latest_trade_time") or stats.get("latest_date"),
        metrics=[
            _metric("最新分钟", stats.get("latest_trade_time")),
            _metric("当日最终表", _format_int(stats.get("row_count"))),
            _metric("QMT raw", _format_int(raw_sources.get("qmt", {}).get("row_count"))),
            _metric("AkShare raw", _format_int(raw_sources.get("akshare", {}).get("row_count"))),
            _metric("订阅源", _source_display((config or {}).get("data_source_preference")) if config else "按 QMT 运行配置"),
        ],
        sources=_source_count_sources(raw_sources),
        notes=[
            "分钟线最终表可能大于 raw/pub，因为历史同步和缺口补齐会直接写 stock_minute_kline。",
            "QMT 历史 bridge 需要 QMT_MINUTE_DATABASE_URL 指向 Windows 可访问的 PostgreSQL 地址。",
        ],
    )


def _build_index_market_card(db: Session) -> dict[str, Any]:
    daily_stats = _latest_daily_stats(db, "index_daily_kline")
    minute_stats = _latest_minute_stats(db, "index_minute_kline")
    has_any = bool(daily_stats.get("row_count") or minute_stats.get("row_count"))
    return _card(
        card_id="index-kline",
        title="指数日线 / 分钟线",
        category="market",
        source_label="TDX / QMT / AkShare/Sina -> index_*_kline",
        status_label="已有指数数据" if has_any else "指数表暂无当日数据",
        status_tone="good" if has_any else "warn",
        schedule=f"回测订阅默认 15:05 收盘后同步指数日线/分钟线，失败后 30 分钟重试；当前 QMT 行情 worker：{_on_off(_env_flag('ENABLE_QMT_MARKET_SYNC_WORKER', '0'))}",
        mechanism="指数日线与指数分钟线当前不走股票 raw/norm/pub 管道，TDX/QMT/AkShare 结果直接 upsert 到 index_daily_kline / index_minute_kline。",
        tables=["index_daily_kline", "index_minute_kline"],
        last_updated_at=daily_stats.get("last_updated_at") or minute_stats.get("last_updated_at"),
        watermark=minute_stats.get("latest_trade_time") or daily_stats.get("latest_date"),
        metrics=[
            _metric("指数日线日期", daily_stats.get("latest_date")),
            _metric("指数日线行数", _format_int(daily_stats.get("row_count"))),
            _metric("指数分钟", minute_stats.get("latest_trade_time")),
            _metric("指数分钟行数", _format_int(minute_stats.get("row_count"))),
        ],
        sources=[
            {"key": "qmt", "label": "QMT 指数行情"},
            {"key": "akshare", "label": "AkShare/Sina 指数数据"},
        ],
        notes=[
            "市场总览的指数快照会优先使用 QMT 实时 quote；quote 不可用时才看 index_daily_kline。",
        ],
    )


def _build_qmt_runtime_card(db: Session) -> dict[str, Any]:
    profile = _query_one(
        db,
        """
        SELECT account_key, is_active, sync_interval_seconds, last_synced_at,
               last_status, last_error, consecutive_failures, updated_at
        FROM qmt_sync_profiles
        ORDER BY updated_at DESC NULLS LAST, id DESC
        LIMIT 1
        """,
        table_name="qmt_sync_profiles",
    )
    return _card(
        card_id="qmt-runtime",
        title="QMT 实时行情与账户",
        category="account",
        source_label="QMT Bridge / XtQuant",
        status_label=(profile or {}).get("last_status") or ("已开启" if _env_flag("ENABLE_QMT_SYNC_WORKER", "0") else "未开启"),
        status_tone="good" if (profile or {}).get("last_status") in {"success", "ok", "completed"} else "neutral",
        schedule=(
            f"账户同步 worker：{_on_off(_env_flag('ENABLE_QMT_SYNC_WORKER', '0'))}；"
            f"行情 worker：{_on_off(_env_flag('ENABLE_QMT_MARKET_SYNC_WORKER', '0'))}；"
            f"分钟订阅 worker：{_on_off(_env_flag('ENABLE_QMT_MINUTE_SUBSCRIPTION_WORKER', '0'))}"
        ),
        mechanism="QMT 账户链路读取资产、持仓、委托和成交；行情链路通过 bridge 或 xtdata 获取实时 quote、日线和分钟线。",
        tables=["qmt_sync_profiles", "qmt_sync_snapshots", "stock_minute_kline"],
        last_run_at=(profile or {}).get("updated_at"),
        last_success_at=(profile or {}).get("last_synced_at"),
        last_updated_at=(profile or {}).get("updated_at"),
        metrics=[
            _metric("默认历史账户", os.getenv("QMT_HISTORY_ACCOUNT_KEY", "paper_sim")),
            _metric("分钟历史账户", os.getenv("QMT_MINUTE_HISTORY_ACCOUNT_KEY", "live_real")),
            _metric("同步间隔", f"{(profile or {}).get('sync_interval_seconds') or os.getenv('QMT_REFRESH_INTERVAL_SECONDS', '10')} 秒"),
            _metric("连续失败", (profile or {}).get("consecutive_failures")),
        ],
        sources=[
            {"key": "qmt", "label": "QMT 实时行情链路"},
            {"key": "xtquant", "label": "XtQuant 账户链路"},
            {"key": "qmt_bridge", "label": "QMT Bridge"},
        ],
        notes=[
            "QMT 账户状态、行情状态和分钟历史同步是三条相关但不同的链路。",
            "bridge token 和数据库连接不会在本模块展示。",
        ],
    )


def _build_daily_review_card(db: Session, *, user_id: str | None = None) -> dict[str, Any]:
    config = _daily_review_config(db, user_id=user_id)
    latest = _latest_daily_review(db, user_id=user_id)
    trigger_time = (config or {}).get("trigger_time") or "21:10"
    enabled = bool((config or {}).get("enabled"))
    worker_enabled = _env_flag("ENABLE_DAILY_REVIEW_WORKER", "1")
    return _card(
        card_id="daily-review",
        title="每日复盘",
        category="review",
        source_label="市场总览 + 新闻缓存 + 持仓/自选 + LLM 增强",
        status_label="已开启" if enabled and worker_enabled else ("配置未开启" if not enabled else "worker 未开启"),
        status_tone="good" if enabled and worker_enabled else "warn",
        schedule=f"每日 {trigger_time}（Asia/Shanghai）后由 worker 轮询触发；轮询间隔 {_env_int('DAILY_REVIEW_POLL_SECONDS', 60)} 秒",
        mechanism="到点后生成当日 market snapshot、新闻摘要、组合诊断和规则复盘；若 LLM 配置可用则增强文案，随后按配置推送企业微信/邮件。",
        tables=["daily_reviews", "user_daily_review_configs"],
        last_run_at=(config or {}).get("updated_at") or (latest or {}).get("updated_at"),
        last_success_at=(latest or {}).get("updated_at"),
        last_updated_at=(latest or {}).get("updated_at") or (config or {}).get("updated_at"),
        watermark=(latest or {}).get("trade_date") or (config or {}).get("last_run_date"),
        metrics=[
            _metric("触发时间", trigger_time),
            _metric("自动生成", "已开启" if enabled else "未开启"),
            _metric("自动推送", "已开启" if (config or {}).get("push_enabled") else "未开启"),
            _metric("最近交易日", (latest or {}).get("trade_date") or (config or {}).get("last_run_date")),
            _metric("最近状态", (latest or {}).get("status") or (config or {}).get("last_run_status")),
        ],
        sources=[
            {"key": "postgresql", "label": "stock_daily_kline / market snapshot"},
            {"key": "news_cache", "label": "market_news_items"},
            {"key": "live", "label": "用户持仓、自选与配置"},
        ],
        notes=[
            "每日复盘依赖当天日线最终表；如果复盘早于日线发布完成，会出现市场总览缺字段。",
            "建议触发时间晚于日线自动订阅完成时间，或在生成前增加 readiness gate。",
        ],
    )


def _latest_backtest_config(db: Session, *, data_type: str) -> dict[str, Any] | None:
    rows = _query_all(
        db,
        """
        SELECT id, config_name, enabled_data_types, data_source_preference, auto_download,
               update_frequency, schedule_time, timezone, only_trading_day,
               last_run_at, last_success_at, updated_at
        FROM backtest_data_configs
        ORDER BY updated_at DESC NULLS LAST, id DESC
        LIMIT 20
        """,
        table_name="backtest_data_configs",
    )
    for row in rows:
        enabled_types = _coerce_list(row.get("enabled_data_types"))
        if data_type in enabled_types:
            return row
    return rows[0] if rows else None


def _latest_watermark(db: Session, data_type: str) -> dict[str, Any] | None:
    return _query_one(
        db,
        """
        SELECT data_type, data_source, scope_key, last_data_date, last_run_started_at,
               last_success_at, last_status, last_error, updated_at
        FROM backtest_data_watermarks
        WHERE data_type = :data_type
        ORDER BY updated_at DESC NULLS LAST, id DESC
        LIMIT 1
        """,
        {"data_type": data_type},
        table_name="backtest_data_watermarks",
    )


def _daily_review_config(db: Session, *, user_id: str | None) -> dict[str, Any] | None:
    if user_id:
        row = _query_one(
            db,
            """
            SELECT enabled, trigger_time, push_enabled, last_run_date,
                   last_run_status, last_error, updated_at
            FROM user_daily_review_configs
            WHERE user_id = :user_id
            ORDER BY updated_at DESC NULLS LAST
            LIMIT 1
            """,
            {"user_id": user_id},
            table_name="user_daily_review_configs",
        )
        if row:
            return row
    return _query_one(
        db,
        """
        SELECT enabled, trigger_time, push_enabled, last_run_date,
               last_run_status, last_error, updated_at
        FROM user_daily_review_configs
        ORDER BY updated_at DESC NULLS LAST
        LIMIT 1
        """,
        table_name="user_daily_review_configs",
    )


def _latest_daily_review(db: Session, *, user_id: str | None) -> dict[str, Any] | None:
    params: dict[str, Any] = {}
    user_filter = ""
    if user_id:
        user_filter = "WHERE user_id = :user_id"
        params["user_id"] = user_id
    return _query_one(
        db,
        f"""
        SELECT trade_date, status, push_status, updated_at, created_at
        FROM daily_reviews
        {user_filter}
        ORDER BY trade_date DESC NULLS LAST, updated_at DESC NULLS LAST
        LIMIT 1
        """,
        params,
        table_name="daily_reviews",
    )


def _latest_daily_stats(db: Session, table_name: str) -> dict[str, Any]:
    latest_date = _scalar(db, f"SELECT MAX(trade_date) FROM {table_name}", table_name=table_name)
    if not latest_date:
        return {}
    row = _query_one(
        db,
        f"""
        SELECT COUNT(*) AS row_count, MAX(updated_at) AS last_updated_at
        FROM {table_name}
        WHERE trade_date = :trade_date
        """,
        {"trade_date": latest_date},
        table_name=table_name,
    )
    return {
        "latest_date": _json_value(latest_date),
        "row_count": int((row or {}).get("row_count") or 0),
        "last_updated_at": (row or {}).get("last_updated_at"),
    }


def _latest_minute_stats(db: Session, table_name: str) -> dict[str, Any]:
    latest_time = _scalar(db, f"SELECT MAX(trade_time) FROM {table_name}", table_name=table_name)
    if not latest_time:
        return {}
    latest_date = latest_time.date() if isinstance(latest_time, datetime) else _parse_date(str(latest_time)[:10])
    if latest_date is None:
        return {"latest_trade_time": _json_value(latest_time)}
    row = _query_one(
        db,
        f"""
        SELECT COUNT(*) AS row_count, MAX(updated_at) AS last_updated_at
        FROM {table_name}
        WHERE trade_time >= :start_dt AND trade_time < :end_dt
        """,
        {
            "start_dt": datetime.combine(latest_date, time.min),
            "end_dt": datetime.combine(latest_date + timedelta(days=1), time.min),
        },
        table_name=table_name,
    )
    return {
        "latest_date": latest_date.isoformat(),
        "latest_trade_time": _json_value(latest_time),
        "row_count": int((row or {}).get("row_count") or 0),
        "last_updated_at": (row or {}).get("last_updated_at"),
    }


def _source_counts_for_date(db: Session, table_name: str, trade_date: Any) -> dict[str, dict[str, Any]]:
    if not trade_date:
        return {}
    rows = _query_all(
        db,
        f"""
        SELECT source, COUNT(*) AS row_count, MAX(updated_at) AS last_updated_at
        FROM {table_name}
        WHERE trade_date = :trade_date
        GROUP BY source
        ORDER BY row_count DESC
        """,
        {"trade_date": trade_date},
        table_name=table_name,
    )
    return {
        str(row.get("source") or "unknown"): {
            "row_count": int(row.get("row_count") or 0),
            "last_updated_at": row.get("last_updated_at"),
        }
        for row in rows
    }


def _raw_counts_for_date(
    db: Session,
    table_map: dict[str, str],
    trade_date: Any,
    *,
    date_column: str,
) -> dict[str, dict[str, Any]]:
    if not trade_date:
        return {}
    result: dict[str, dict[str, Any]] = {}
    for source, table_name in table_map.items():
        if date_column == "trade_time":
            parsed = _parse_date(str(trade_date)[:10])
            if parsed is None:
                continue
            row = _query_one(
                db,
                f"""
                SELECT COUNT(*) AS row_count, MAX(updated_at) AS last_updated_at
                FROM {table_name}
                WHERE trade_time >= :start_dt AND trade_time < :end_dt
                """,
                {
                    "start_dt": datetime.combine(parsed, time.min),
                    "end_dt": datetime.combine(parsed + timedelta(days=1), time.min),
                },
                table_name=table_name,
            )
        else:
            row = _query_one(
                db,
                f"""
                SELECT COUNT(*) AS row_count, MAX(updated_at) AS last_updated_at
                FROM {table_name}
                WHERE {date_column} = :trade_date
                """,
                {"trade_date": trade_date},
                table_name=table_name,
            )
        result[source] = {
            "row_count": int((row or {}).get("row_count") or 0),
            "last_updated_at": (row or {}).get("last_updated_at"),
        }
    return result


def _build_worker_status() -> list[dict[str, Any]]:
    specs = [
        ("ENABLE_NEWS_EYE_WORKER", "资讯后台抓取", "1"),
        ("ENABLE_BACKTEST_AUTO_UPDATE_WORKER", "回测数据自动订阅", "0"),
        ("ENABLE_QMT_MARKET_SYNC_WORKER", "QMT 行情同步", "0"),
        ("ENABLE_QMT_MINUTE_SUBSCRIPTION_WORKER", "QMT 分钟订阅采集", "0"),
        ("ENABLE_MINUTE_KLINE_GAP_FILLER_WORKER", "分钟线缺口补齐", "0"),
        ("ENABLE_DAILY_REVIEW_WORKER", "每日复盘", "1"),
        ("ENABLE_QMT_SYNC_WORKER", "QMT 账户同步", "0"),
    ]
    return [
        {
            "key": key,
            "label": label,
            "enabled": _env_flag(key, default),
            "value": os.getenv(key, default),
        }
        for key, label, default in specs
    ]


def _card(
    *,
    card_id: str,
    title: str,
    category: str,
    source_label: str,
    status_label: str,
    status_tone: str,
    schedule: str,
    mechanism: str,
    tables: list[str],
    metrics: list[dict[str, Any]],
    sources: list[dict[str, Any]],
    notes: list[str],
    last_run_at: Any = None,
    last_success_at: Any = None,
    last_updated_at: Any = None,
    watermark: Any = None,
) -> dict[str, Any]:
    return {
        "id": card_id,
        "title": title,
        "category": category,
        "source_label": source_label,
        "status_label": status_label,
        "status_tone": status_tone,
        "schedule": schedule,
        "mechanism": mechanism,
        "tables": tables,
        "last_run_at": _json_value(last_run_at),
        "last_success_at": _json_value(last_success_at),
        "last_updated_at": _json_value(last_updated_at),
        "watermark": _json_value(watermark),
        "metrics": metrics,
        "sources": sources,
        "notes": notes,
    }


def _metric(label: str, value: Any, detail: str | None = None, tone: str = "neutral") -> dict[str, Any]:
    return {
        "label": label,
        "value": str(value if value not in (None, "") else "--"),
        "detail": detail,
        "tone": tone,
    }


def _query_one(
    db: Session,
    sql: str,
    params: dict[str, Any] | None = None,
    *,
    table_name: str | None = None,
) -> dict[str, Any] | None:
    rows = _query_all(db, sql, params, table_name=table_name, limit_one=True)
    return rows[0] if rows else None


def _query_all(
    db: Session,
    sql: str,
    params: dict[str, Any] | None = None,
    *,
    table_name: str | None = None,
    limit_one: bool = False,
) -> list[dict[str, Any]]:
    if table_name and not _table_exists(db, table_name):
        return []
    try:
        result = db.execute(text(sql), params or {})
        rows = result.mappings().fetchmany(1) if limit_one else result.mappings().all()
        return [dict(row) for row in rows]
    except Exception:
        return []


def _scalar(db: Session, sql: str, params: dict[str, Any] | None = None, *, table_name: str | None = None) -> Any:
    if table_name and not _table_exists(db, table_name):
        return None
    try:
        return db.execute(text(sql), params or {}).scalar()
    except Exception:
        return None


def _table_exists(db: Session, table_name: str) -> bool:
    try:
        return bool(db.execute(text("SELECT to_regclass(:table_name)"), {"table_name": table_name}).scalar())
    except Exception:
        try:
            return bool(
                db.execute(
                    text(
                        """
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_name = :table_name
                        LIMIT 1
                        """
                    ),
                    {"table_name": table_name},
                ).scalar()
            )
        except Exception:
            return False


def _env_flag(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default) or default).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except Exception:
        return default


def _on_off(value: bool) -> str:
    return "已开启" if value else "未开启"


def _loads_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def _coerce_list(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if str(item)]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(item) for item in parsed if str(item)]
        except Exception:
            pass
        stripped = value.strip("{}[]")
        return [item.strip().strip("'\"") for item in stripped.split(",") if item.strip()]
    return []


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.isoformat()
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except Exception:
        return None


def _format_int(value: Any) -> str:
    try:
        return f"{int(value or 0):,}"
    except Exception:
        return "--"


def _top_source(source_counts: dict[str, dict[str, Any]]) -> str | None:
    if not source_counts:
        return None
    return max(source_counts.items(), key=lambda item: int(item[1].get("row_count") or 0))[0]


def _format_source_counts(source_counts: dict[str, dict[str, Any]]) -> str | None:
    if not source_counts:
        return None
    parts = [
        f"{_source_display(source)} {int(payload.get('row_count') or 0):,}"
        for source, payload in source_counts.items()
        if int(payload.get("row_count") or 0) > 0
    ]
    return " / ".join(parts) if parts else None


def _source_count_sources(source_counts: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "key": source,
            "label": _source_display(source),
            "row_count": int(payload.get("row_count") or 0),
            "last_updated_at": _json_value(payload.get("last_updated_at")),
        }
        for source, payload in source_counts.items()
    ]


def _source_display(value: Any) -> str:
    mapping = {
        "quantclass": "量化课堂",
        "akshare": "AkShare",
        "baostock": "BaoStock",
        "efinance": "EFinance",
        "postgresql": "PostgreSQL",
        "qmt": "QMT",
        "tdx": "通达信/TDX",
    }
    key = str(value or "").strip().lower()
    return mapping.get(key, str(value or "--"))
