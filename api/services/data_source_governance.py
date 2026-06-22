from __future__ import annotations

import re
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping


_SOURCE_REGISTRY: dict[str, dict[str, Any]] = {
    "qmt": {
        "label": "QMT 实时行情链路",
        "category": "market",
        "kind": "live",
        "reliability": "high",
        "description": "直接通过 QMT / xtquant 获取实时行情或分钟线。",
        "caveat": "依赖桥接进程、QMT 客户端与本地网络状态。",
    },
    "xtquant": {
        "label": "XtQuant / QMT 账户链路",
        "category": "account",
        "kind": "live",
        "reliability": "high",
        "description": "直接从 XtQuant 读取账户、持仓、委托与成交。",
        "caveat": "若桥接或终端异常，页面会回退到本地缓存快照。",
    },
    "qmt_bridge": {
        "label": "QMT Bridge 回退链路",
        "category": "market",
        "kind": "fallback",
        "reliability": "medium",
        "description": "QMT 主行情不可用时，桥接层通过分钟数据或最近价做回退。",
        "caveat": "不保证逐笔精度，适合页面兜底，不适合严肃归因。",
    },
    "postgresql": {
        "label": "PostgreSQL 本地行情库",
        "category": "market",
        "kind": "database",
        "reliability": "medium",
        "description": "使用本地数据库中的日线、指数或榜单缓存。",
        "caveat": "更新时间取决于同步任务，不等同于实时行情。",
    },
    "akshare": {
        "label": "AkShare 外部数据",
        "category": "market",
        "kind": "external",
        "reliability": "medium",
        "description": "通过 AkShare 拉取板块、榜单或补充行情数据。",
        "caveat": "接口稳定性和限流不由本项目控制。",
    },
    "cache": {
        "label": "本地缓存快照",
        "category": "cache",
        "kind": "cache",
        "reliability": "medium",
        "description": "页面优先展示最近一次成功同步的本地缓存数据。",
        "caveat": "缓存可用于兜底，但存在滞后风险。",
    },
    "cache_recent": {
        "label": "近期内存快照",
        "category": "cache",
        "kind": "cache",
        "reliability": "medium",
        "description": "使用最近一次成功拉取的内存快照直接回显页面。",
        "caveat": "通常比数据库缓存更新，但仍不等于当前实时状态。",
    },
    "live": {
        "label": "实时直连结果",
        "category": "account",
        "kind": "live",
        "reliability": "high",
        "description": "本次请求直接命中实时链路，没有经过缓存回退。",
        "caveat": "仍受上游终端状态影响。",
    },
    "empty": {
        "label": "空结果兜底",
        "category": "fallback",
        "kind": "fallback",
        "reliability": "low",
        "description": "当前链路没有拿到有效数据，页面只能展示空状态。",
        "caveat": "不应对空状态做收益、仓位或行情解释。",
    },
    "synthetic": {
        "label": "Synthetic 合成数据",
        "category": "backtest",
        "kind": "synthetic",
        "reliability": "low",
        "description": "为保证流程可运行而生成的合成行情或成交结果。",
        "caveat": "只能验证流程，不能解释收益、胜率和成交质量。",
    },
    "true_engine": {
        "label": "真引擎",
        "category": "backtest",
        "kind": "engine",
        "reliability": "high",
        "description": "回测使用了真实数据与正式执行链路。",
        "caveat": "仍需结合分钟数据缺口与成交模拟参数理解结果。",
    },
    "fallback_engine": {
        "label": "回退引擎",
        "category": "backtest",
        "kind": "fallback",
        "reliability": "medium",
        "description": "回测未完全走正式链路，部分逻辑来自回退实现。",
        "caveat": "适合排查流程，不适合作为最终业绩结论。",
    },
    "news_cache": {
        "label": "资讯缓存层",
        "category": "news",
        "kind": "cache",
        "reliability": "medium",
        "description": "页面读取的是本地新闻缓存表，而不是直接请求外部资讯站。",
        "caveat": "页面时间代表入库时间，不代表新闻原始发布时间。",
    },
    "news_external": {
        "label": "外部资讯抓取源",
        "category": "news",
        "kind": "external",
        "reliability": "medium",
        "description": "后台轮询任务从外部资讯源抓取新闻后再入库。",
        "caveat": "抓取成功不代表每条新闻都能被完整结构化。",
    },
    "realtime_event_stream": {
        "label": "实时事件流",
        "category": "realtime",
        "kind": "stream",
        "reliability": "medium",
        "description": "监控页面依靠事件流持续刷新实例状态和信号行为。",
        "caveat": "前端流中断时，页面状态可能滞后于后端实际运行状态。",
    },
    "unknown": {
        "label": "未登记来源",
        "category": "unknown",
        "kind": "unknown",
        "reliability": "unknown",
        "description": "当前来源字符串还没有接入统一注册表。",
        "caveat": "建议继续补登记，避免页面口径再次分散。",
    },
}

_SURFACE_REGISTRY: list[dict[str, Any]] = [
    {
        "id": "virtual-warehouse",
        "name": "虚拟仓",
        "route": "/virtual-warehouse",
        "description": "查看 QMT 账户、持仓、委托、成交与后台刷新状态。",
        "domains": ["virtual_warehouse"],
        "source_keys": ["xtquant", "live", "cache_recent", "cache", "empty"],
        "notes": [
            "页面优先展示近期快照，后台异步刷新 QMT 实时数据。",
            "账户、持仓、委托、成交应按同一账户链路解释。",
        ],
    },
    {
        "id": "realtime-monitor",
        "name": "实时监控",
        "route": "/realtime",
        "description": "查看监控实例、事件流、分钟线判定和账户快照。",
        "domains": ["realtime_monitor", "realtime_positions"],
        "source_keys": ["qmt", "qmt_bridge", "xtquant", "realtime_event_stream"],
        "notes": [
            "监控主行情源、持仓快照和事件流状态需要分开判断。",
            "事件流中断时，页面状态可能滞后于后端实例状态。",
        ],
    },
    {
        "id": "stock-market",
        "name": "股票市场",
        "route": "/stock-market",
        "description": "查看指数、涨跌榜、板块热度与资金流。",
        "domains": ["stock_market"],
        "source_keys": ["qmt", "qmt_bridge", "postgresql", "akshare"],
        "notes": [
            "指数行情、榜单和板块资金流可能来自不同链路。",
            "页面更新时间不代表所有子模块完全同频。",
        ],
    },
    {
        "id": "news-eye",
        "name": "资讯之眼",
        "route": "/news-eye",
        "description": "查看资讯缓存、后台轮询状态和外部新闻抓取来源。",
        "domains": ["news_eye"],
        "source_keys": ["news_cache", "news_external", "unknown"],
        "notes": [
            "页面展示的是缓存层，不是直接命中外部资讯站。",
            "外部源名称仍可能继续细化登记。",
        ],
    },
    {
        "id": "backtest-result",
        "name": "回测结果",
        "route": "/backtest/runs/:runId",
        "description": "查看回测可信度、原始数据源、执行链路与分钟数据缺口。",
        "domains": ["backtest_result"],
        "source_keys": ["true_engine", "fallback_engine", "synthetic", "postgresql", "akshare"],
        "notes": [
            "真实链路、回退链路和 Synthetic 结果必须明确区分。",
            "收益和胜率只能在可信链路下解释。",
        ],
    },
    {
        "id": "settings-governance",
        "name": "设置页治理中心",
        "route": "/settings",
        "description": "汇总查看系统内已登记来源与页面使用映射。",
        "domains": ["registry"],
        "source_keys": ["unknown"],
        "notes": [
            "这里是治理总表，不是单独的数据生产链路。",
        ],
    },
]

_NEWS_SOURCE_LINKS: tuple[dict[str, str], ...] = (
    {
        "key": "cninfo_disclosure_report",
        "name": "巨潮资讯公告",
        "url": "https://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search",
        "tier": "一级",
        "role": "官方公告",
    },
    {
        "key": "sse_announcement",
        "name": "上交所公告",
        "url": "https://www.sse.com.cn/disclosure/listedinfo/announcement/",
        "tier": "一级",
        "role": "官方公告",
    },
    {
        "key": "szse_announcement",
        "name": "深交所公告",
        "url": "https://www.szse.cn/disclosure/listed/bulletinDetail/index.html",
        "tier": "一级",
        "role": "官方公告",
    },
    {
        "key": "stock_info_global_cls",
        "name": "财联社电报",
        "url": "https://www.cls.cn/telegraph",
        "tier": "四级",
        "role": "背景快讯",
    },
    {
        "key": "stock_info_global_em",
        "name": "东方财富全球快讯",
        "url": "https://kuaixun.eastmoney.com/7_24.html",
        "tier": "四级",
        "role": "背景快讯",
    },
    {
        "key": "stock_info_cjzc_em",
        "name": "东方财富财经早餐",
        "url": "https://stock.eastmoney.com/a/czpnc.html",
        "tier": "四级",
        "role": "背景快讯",
    },
    {
        "key": "stock_info_global_sina",
        "name": "新浪7x24",
        "url": "https://finance.sina.com.cn/7x24",
        "tier": "四级",
        "role": "背景快讯",
    },
    {
        "key": "stock_info_global_futu",
        "name": "富途快讯",
        "url": "https://news.futunn.com/main/live",
        "tier": "四级",
        "role": "背景快讯",
    },
    {
        "key": "stock_info_global_ths",
        "name": "同花顺全球直播",
        "url": "https://news.10jqka.com.cn/realtimenews.html",
        "tier": "四级",
        "role": "背景快讯",
    },
    {
        "key": "stock_news_em",
        "name": "东方财富个股新闻",
        "url": "https://so.eastmoney.com/news/s?keyword=000001",
        "tier": "二级",
        "role": "个股新闻",
    },
    {
        "key": "stock_notice_report",
        "name": "东方财富公告",
        "url": "https://data.eastmoney.com/notices/hsa/5.html",
        "tier": "二级",
        "role": "公告聚合",
    },
    {
        "key": "stock_research_report_em",
        "name": "东方财富个股研报",
        "url": "https://data.eastmoney.com/report/stock.jshtml",
        "tier": "三级",
        "role": "研报",
    },
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _unique_strings(values: Iterable[Any]) -> list[str]:
    results: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
    return results


def split_source_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        parts: list[str] = []
        for item in value:
            parts.extend(split_source_values(item))
        return _unique_strings(parts)
    text = str(value).strip()
    if not text:
        return []
    return _unique_strings(re.split(r"\s*(?:\+|/|,|\|)\s*", text))


def _normalize_registry_key(value: str | None) -> str:
    token = str(value or "").strip().lower()
    if not token:
        return "unknown"
    if token in _SOURCE_REGISTRY:
        return token
    if token.startswith("cache:market_news_items"):
        return "news_cache"
    if token.startswith("cache:"):
        return "cache"
    if token.startswith("postgresql"):
        return "postgresql"
    if token.startswith("qmt_bridge"):
        return "qmt_bridge"
    if token.startswith("qmt"):
        return "qmt"
    if token.startswith("xtquant"):
        return "xtquant"
    if token.startswith("akshare"):
        return "akshare"
    if token.startswith("synthetic"):
        return "synthetic"
    if token.startswith("live"):
        return "live"
    if token.startswith("empty"):
        return "empty"
    if token.startswith("true_engine"):
        return "true_engine"
    if token.startswith("fallback_engine"):
        return "fallback_engine"
    if token.startswith("event_stream") or token.startswith("realtime_event"):
        return "realtime_event_stream"
    return "unknown"


def describe_source(value: str | None) -> dict[str, Any]:
    token = str(value or "").strip()
    key = _normalize_registry_key(token)
    payload = deepcopy(_SOURCE_REGISTRY.get(key) or _SOURCE_REGISTRY["unknown"])
    payload["key"] = key
    payload["token"] = token or key
    return payload


def describe_sources(values: Any) -> list[dict[str, Any]]:
    return [describe_source(token) for token in split_source_values(values)]


def build_item(
    label: str,
    value: Any,
    detail: str | None = None,
    *,
    tone: str = "neutral",
) -> dict[str, Any]:
    return {
        "label": label,
        "value": str(value if value not in (None, "") else "--"),
        "detail": detail,
        "tone": tone,
    }


def build_governance_payload(
    *,
    domain: str,
    title: str,
    description: str,
    items: list[dict[str, Any]],
    warnings: Iterable[Any] | None = None,
    source_values: Iterable[Any] | None = None,
    updated_at: str | None = None,
) -> dict[str, Any]:
    resolved_sources = describe_sources(list(source_values or []))
    return {
        "domain": domain,
        "title": title,
        "description": description,
        "items": items,
        "warnings": _unique_strings(warnings or []),
        "sources": resolved_sources,
        "updated_at": updated_at or _now_iso(),
    }


def list_registered_sources() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for key, payload in sorted(_SOURCE_REGISTRY.items(), key=lambda item: item[0]):
        record = deepcopy(payload)
        record["key"] = key
        items.append(record)
    return items


def list_surface_registry() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for surface in _SURFACE_REGISTRY:
        record = deepcopy(surface)
        source_keys = _unique_strings(record.get("source_keys") or [])
        record["source_keys"] = source_keys
        record["sources"] = [describe_source(key) for key in source_keys]
        record["notes"] = _unique_strings(record.get("notes") or [])
        items.append(record)
    return items


def list_news_source_links() -> list[dict[str, str]]:
    return [dict(item) for item in _NEWS_SOURCE_LINKS]


def build_virtual_warehouse_governance(payload: Mapping[str, Any]) -> dict[str, Any]:
    connection = dict(payload.get("connection") or {})
    background_refresh = dict(payload.get("background_refresh") or {})
    data_source = str(payload.get("data_source") or connection.get("provider") or "--")
    is_stale = bool(payload.get("is_stale", False))
    fetched_at = str(payload.get("fetched_at") or "")
    last_synced_at = str(payload.get("last_synced_at") or "")
    health_label = str(connection.get("health_label") or ("已连接" if connection.get("connected") else "未连接"))
    health_message = str(connection.get("health_message") or connection.get("message") or "")
    health_status = str(connection.get("health_status") or "")
    effective_connected = bool(connection.get("effective_connected") or connection.get("connected"))
    if health_status == "snapshot_available":
        health_tone = "warn"
    elif effective_connected:
        health_tone = "good"
    else:
        health_tone = "bad"
    if background_refresh.get("active"):
        refresh_value = "刷新中"
        refresh_detail = f"后台任务开始于 {background_refresh.get('started_at') or '--'}"
        refresh_tone = "info"
    elif background_refresh.get("last_error"):
        refresh_value = "最近失败"
        refresh_detail = str(background_refresh.get("last_error"))
        refresh_tone = "bad"
    elif background_refresh.get("last_success_at"):
        refresh_value = "最近成功"
        refresh_detail = f"上次成功时间 {background_refresh.get('last_success_at')}"
        refresh_tone = "good"
    else:
        refresh_value = "等待任务"
        refresh_detail = "页面默认优先使用快照，后台异步补最新 QMT 数据。"
        refresh_tone = "neutral"
    warnings = []
    if connection.get("message"):
        warnings.append(connection.get("message"))
    if background_refresh.get("last_error"):
        warnings.append(f"后台刷新异常：{background_refresh.get('last_error')}")
    return build_governance_payload(
        domain="virtual_warehouse",
        title="数据源治理",
        description="区分当前是实时 QMT、近期缓存、数据库快照，还是后台刷新中的中间状态。",
        items=[
            build_item(
                "QMT 链路状态",
                health_label,
                health_message or "账户、持仓、委托与成交都沿用同一账户链路返回。",
                tone=health_tone,
            ),
            build_item(
                "账户数据源",
                data_source,
                "账户、持仓、委托与成交都沿用同一账户链路返回。",
                tone="warn" if is_stale else ("good" if effective_connected else "bad"),
            ),
            build_item(
                "页面状态",
                "缓存快照" if is_stale else "最新同步",
                "缓存快照用于兜底，最新同步代表本次请求直连成功。",
                tone="warn" if is_stale else "good",
            ),
            build_item(
                "最近同步",
                last_synced_at or fetched_at or "--",
                f"页面读取时间 {fetched_at}" if fetched_at else "等待下一次读取。",
                tone="info",
            ),
            build_item("后台刷新", refresh_value, refresh_detail, tone=refresh_tone),
        ],
        warnings=warnings,
        source_values=[data_source, connection.get("provider")],
        updated_at=fetched_at or None,
    )


def build_market_overview_governance(payload: Mapping[str, Any]) -> dict[str, Any]:
    indices = list(payload.get("indices") or [])
    top_gainers = list(payload.get("top_gainers") or [])
    top_losers = list(payload.get("top_losers") or [])
    sector_items = [
        *(payload.get("sector_gainers") or []),
        *(payload.get("sector_losers") or []),
        *(payload.get("sector_fund_inflows") or []),
        *(payload.get("sector_fund_outflows") or []),
    ]
    market_stats = dict(payload.get("market_stats") or {})
    index_sources = _unique_strings(item.get("source") for item in indices if isinstance(item, Mapping))
    ranking_sources = _unique_strings(item.get("source") for item in [*top_gainers, *top_losers] if isinstance(item, Mapping))
    sector_sources = _unique_strings(item.get("source") for item in sector_items if isinstance(item, Mapping))
    stats_source = str(market_stats.get("source") or "--")
    fallback = bool(payload.get("fallback", False))
    updated_at = str(payload.get("updated_at") or "")
    warnings = []
    if fallback:
        warnings.append("当前市场页已经进入 fallback 链路，指数、个股榜单与板块来源可能不同步。")
    if not market_stats.get("total_amount") and not market_stats.get("index_turnover_amount"):
        warnings.append("市场宽度缺少两市成交额，主线强弱和流动性判断会降级。")
    if market_stats.get("up_count") is None or market_stats.get("down_count") is None:
        warnings.append("市场宽度缺少涨跌家数，赚钱效应判断会降级。")
    return build_governance_payload(
        domain="stock_market",
        title="数据源治理",
        description="市场页会混合实时行情、榜单缓存和板块资金流，不能把所有数字视为同一时点结果。",
        items=[
            build_item(
                "页面主数据源",
                payload.get("source") or "--",
                "这里表示总览接口的组合来源，而不是单一数据库表。",
                tone="warn" if fallback else "good",
            ),
            build_item(
                "指数行情",
                " / ".join(index_sources) if index_sources else "--",
                "三大指数和宽基指数的来源集合。",
                tone="good" if any("qmt" in item.lower() for item in index_sources) else "neutral",
            ),
            build_item(
                "个股与榜单",
                " / ".join(ranking_sources) if ranking_sources else "--",
                "涨跌榜和搜索结果使用的来源集合。",
                tone="good" if any("qmt" in item.lower() for item in ranking_sources) else "neutral",
            ),
            build_item(
                "板块与资金流",
                " / ".join(sector_sources) if sector_sources else "--",
                "板块热度与资金流通常不是逐笔实时行情。",
                tone="info" if sector_sources else "neutral",
            ),
            build_item(
                "市场宽度与成交额",
                stats_source,
                "两市成交额、涨跌家数和涨跌停统计由日线库聚合，用于判断主线是否得到盘面确认。",
                tone="good" if stats_source != "--" else "warn",
            ),
            build_item(
                "页面更新时间",
                updated_at or "--",
                "这是市场总览接口更新时间，不保证所有子模块完全同频。",
                tone="info",
            ),
        ],
        warnings=warnings,
        source_values=[payload.get("source"), index_sources, ranking_sources, sector_sources, stats_source],
        updated_at=updated_at or None,
    )


def build_news_eye_governance(payload: Mapping[str, Any]) -> dict[str, Any]:
    background = dict(payload.get("background") or {})
    active_sources = _unique_strings(background.get("active_sources") or [])
    page_source = str(payload.get("source") or "--")
    fallback = bool(payload.get("fallback", False))
    updated_at = str(payload.get("updated_at") or "")
    status = str(background.get("status") or "").strip() or "idle"
    status_label = {
        "running": "轮询中",
        "idle": "空闲",
        "error": "异常",
        "degraded": "降级",
    }.get(status, status)
    warnings = []
    if background.get("last_error"):
        warnings.append(f"资讯采集异常：{background.get('last_error')}")
    if not active_sources:
        warnings.append("当前没有识别到活跃外部源，页面可能主要在读取历史缓存。")
    return build_governance_payload(
        domain="news_eye",
        title="数据源治理",
        description="资讯页本质上读取的是缓存层，外部新闻源通过后台轮询入库后再展示。",
        items=[
            build_item(
                "页面主数据源",
                page_source,
                "页面读取的是资讯缓存层返回结果，而不是直接命中外部站点。",
                tone="warn" if fallback else "good",
            ),
            build_item(
                "外部活跃源",
                " / ".join(active_sources) if active_sources else "暂无活跃源",
                "这是真正对外抓取资讯的源头列表。",
                tone="info" if active_sources else "warn",
            ),
            build_item(
                "后台轮询状态",
                status_label,
                f"{background.get('interval_seconds') or '--'} 秒 / 次",
                tone="bad" if status == "error" else ("warn" if status == "degraded" else ("good" if status in {"running", "idle"} else "neutral")),
            ),
            build_item(
                "最近成功入库",
                background.get("last_success_at") or updated_at or "--",
                "这里表示最近一次成功入库时间，不代表新闻原始发布时间。",
                tone="info",
            ),
        ],
        warnings=warnings,
        source_values=[page_source, active_sources],
        updated_at=updated_at or None,
    )


def build_realtime_monitor_governance(payload: Mapping[str, Any]) -> dict[str, Any]:
    state = dict(payload.get("state") or {})
    circuit_breaker = dict(payload.get("circuit_breaker") or {})
    warnings = []
    if circuit_breaker.get("active") and circuit_breaker.get("reason"):
        warnings.append(f"当前实例已熔断：{circuit_breaker.get('reason')}")
    return build_governance_payload(
        domain="realtime_monitor",
        title="数据源治理",
        description="实时监控依赖主行情源、符号池解析和循环状态，三者需要分开判断。",
        items=[
            build_item(
                "监控数据源",
                payload.get("quote_source") or "--",
                "这是监控实例声明的主行情源，不等同于持仓快照来源。",
                tone="good" if payload.get("quote_source") else "neutral",
            ),
            build_item(
                "股票池口径",
                f"{int(payload.get('display_symbol_count') or 0)} 只",
                "默认按监控实例解析出的最终股票池运行。",
                tone="info",
            ),
            build_item(
                "实例状态",
                payload.get("status") or "--",
                "这里是后端实例状态，不依赖前端事件流连接是否成功。",
                tone="bad" if payload.get("status") == "fused" else ("good" if payload.get("status") == "running" else "neutral"),
            ),
            build_item(
                "最近循环上报",
                state.get("last_updated_at") or "--",
                "表示后端最近一次监控循环完成并更新实例状态的时间。",
                tone="info" if state.get("last_updated_at") else "warn",
            ),
        ],
        warnings=warnings,
        source_values=[payload.get("quote_source"), "realtime_event_stream"],
        updated_at=state.get("last_updated_at"),
    )


def build_realtime_positions_governance(payload: Mapping[str, Any]) -> dict[str, Any]:
    connection = dict(payload.get("connection") or {})
    fetched_at = str(payload.get("fetched_at") or "")
    return build_governance_payload(
        domain="realtime_positions",
        title="数据源治理",
        description="监控页的持仓快照来自账户链路，不应和分钟线判定源混为一谈。",
        items=[
            build_item(
                "持仓快照",
                connection.get("provider") or "--",
                f"快照时间 {fetched_at}" if fetched_at else "当前尚未拉到持仓快照。",
                tone="good" if fetched_at else "warn",
            ),
        ],
        warnings=[connection.get("message")] if connection.get("message") else [],
        source_values=[connection.get("provider")],
        updated_at=fetched_at or None,
    )


def build_backtest_governance(run_like: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(run_like.get("result") or {})
    summary = dict(result.get("summary") or {})
    diagnostics = dict(result.get("diagnostics") or {})
    data_source = str(summary.get("data_source") or "--")
    engine_mode = str(summary.get("engine_mode") or "--")
    truth_label = "真实链路"
    truth_tone = "good"
    truth_detail = "当前结果来自真实数据链路，可继续做正式分析。"
    if data_source.startswith("synthetic:"):
        truth_label = "Synthetic / 合成"
        truth_tone = "bad"
        truth_detail = "当前结果使用合成数据，只适合验证流程，不应解释收益、胜率和成交质量。"
    elif engine_mode == "fallback_engine" or diagnostics.get("fallback_mode"):
        truth_label = "回退链路"
        truth_tone = "warn"
        truth_detail = "当前结果经过回退链路，适合排查流程，不适合作为正式收益结论。"
    warnings = []
    if data_source.startswith("synthetic:"):
        warnings.append("当前回测使用 Synthetic 数据，只适合验证流程，不应解释收益、胜率和成交质量。")
    if diagnostics.get("fallback_mode"):
        warnings.append("当前结果经过 fallback_engine 回退链路，精度与真实性应以真引擎结果为准。")
    minute_missing = int(diagnostics.get("minute_data_missing") or 0)
    return build_governance_payload(
        domain="backtest_result",
        title="数据源治理",
        description="回测结果必须同时看到原始数据源、执行链路和是否进入 synthetic / fallback。",
        items=[
            build_item("结果可信度", truth_label, truth_detail, tone=truth_tone),
            build_item(
                "原始数据源",
                data_source,
                "这是回测引擎 summary 中写入的原始数据源标识。",
                tone="bad" if data_source.startswith("synthetic:") else "info",
            ),
            build_item(
                "执行链路",
                "真引擎" if engine_mode == "true_engine" else ("回退链路" if engine_mode == "fallback_engine" else engine_mode),
                "真引擎与回退链路的行为边界并不相同。",
                tone="good" if engine_mode == "true_engine" else "warn",
            ),
            build_item(
                "分钟数据缺口",
                minute_missing,
                f"分钟聚合周期 {summary.get('minute_aggregation') or '--'}",
                tone="warn" if minute_missing > 0 else "good",
            ),
            build_item(
                "运行完成时间",
                run_like.get("completed_at") or run_like.get("created_at") or "--",
                f"结果目录 {run_like.get('artifact_root')}" if run_like.get("artifact_root") else "当前未记录结果目录。",
                tone="info",
            ),
        ],
        warnings=warnings,
        source_values=[data_source, engine_mode],
        updated_at=str(run_like.get("completed_at") or run_like.get("created_at") or ""),
    )
