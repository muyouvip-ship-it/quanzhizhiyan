from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, urlencode, urlparse

import requests

if TYPE_CHECKING:
    from api.database import DailyReviewDB
    from api.database import ReportDB

logger = logging.getLogger(__name__)
_WECOM_WEBHOOK_HOST = "qyapi.weixin.qq.com"
_WECOM_WEBHOOK_PATH = "/cgi-bin/webhook/send"


def _clip_text(text: str | None, limit: int = 720) -> str:
    if not text:
        return ""
    compact = " ".join(str(text).split()).strip()
    return compact[:limit]


def _pick_attr(obj, key: str, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _normalize_lines(values, *, limit: int, fallback: list[str] | None = None) -> list[str]:
    results: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        text = _clip_text(raw, 88)
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
        if len(results) >= limit:
            break
    if results or not fallback:
        return results
    return [_clip_text(item, 88) for item in fallback[:limit] if _clip_text(item, 88)]


def _extract_sentence_candidates(text: str | None) -> list[str]:
    compact = _clip_text(text, 500)
    if not compact:
        return []
    parts = re.split(r"[。！？；;\n]+", compact)
    return [item.strip(" -:：") for item in parts if item.strip()]


def _build_reason_lines(report: "ReportDB") -> list[str]:
    metric_lines = [
        f"{item.get('name')}：{item.get('value')}"
        for item in (_pick_attr(report, "key_metrics") or [])[:3]
        if isinstance(item, dict) and item.get("name") and item.get("value")
    ]
    analyst_lines = [
        f"{item.get('agent')}：{_clip_text(item.get('key_finding'), 44)}"
        for item in (_pick_attr(report, "analyst_traces") or [])[:3]
        if isinstance(item, dict) and (item.get("agent") or item.get("key_finding"))
    ]
    fallback_text = (
        _pick_attr(report, "final_trade_decision")
        or _pick_attr(report, "trader_investment_plan")
        or _pick_attr(report, "investment_plan")
    )
    fallback_lines = _extract_sentence_candidates(fallback_text)
    return _normalize_lines(metric_lines + analyst_lines + fallback_lines, limit=3)


def _build_risk_line(report: "ReportDB") -> str:
    risk_items = _pick_attr(report, "risk_items") or []
    for item in risk_items:
        if isinstance(item, dict):
            name = _clip_text(item.get("name"), 28)
            description = _clip_text(item.get("description"), 54)
            if name and description:
                return f"{name}：{description}"
            if description:
                return description
    for line in _extract_sentence_candidates(
        _pick_attr(report, "final_trade_decision") or _pick_attr(report, "trader_investment_plan")
    ):
        if any(keyword in line for keyword in ("风险", "止损", "回撤", "波动", "跌破", "不及预期")):
            return _clip_text(line, 68)
    return "关注仓位控制、量能持续性与次日情绪分歧。"


def _build_execution_line(report: "ReportDB") -> str:
    decision = str(_pick_attr(report, "decision") or "").strip() or "观察"
    direction = str(_pick_attr(report, "direction") or "").strip() or "中性"
    fallback_text = (
        _pick_attr(report, "trader_investment_plan")
        or _pick_attr(report, "investment_plan")
        or _pick_attr(report, "final_trade_decision")
    )
    for line in _extract_sentence_candidates(fallback_text):
        if any(keyword in line for keyword in ("仓位", "分批", "止盈", "止损", "等待", "确认", "执行", "纪律", "买点", "卖点")):
            return _clip_text(line, 68)
    return f"围绕“{decision}/{direction}”执行，先确认开盘强弱与量价配合，再决定是否动作。"


def build_report_message(report: "ReportDB") -> str:
    lines = [
        "量化之神定时分析完成",
        f"标的：{report.symbol}",
        f"交易日：{report.trade_date}",
    ]
    if getattr(report, "decision", None):
        lines.append(f"决策：{report.decision}")
    if getattr(report, "direction", None):
        lines.append(f"方向：{report.direction}")
    if getattr(report, "confidence", None) is not None:
        lines.append(f"置信度：{report.confidence}%")
    reasons = _build_reason_lines(report)
    if reasons:
        lines.append("")
        lines.append("核心理由：")
        lines.extend(f"{index}. {reason}" for index, reason in enumerate(reasons, start=1))
    lines.append("")
    lines.append(f"主要风险：{_build_risk_line(report)}")
    lines.append(f"执行提醒：{_build_execution_line(report)}")
    return "\n".join(lines)[:900]


def build_daily_review_message(review: "DailyReviewDB | dict") -> str:
    market_summary = _pick_attr(review, "market_summary") or {}
    portfolio_summary = _pick_attr(review, "portfolio_summary") or {}
    current_main_themes = _pick_attr(review, "current_main_themes") or []
    next_main_themes = _pick_attr(review, "next_main_themes") or []
    next_candidate_stocks = _pick_attr(review, "next_candidate_stocks") or []
    risk_watchpoints = _pick_attr(review, "risk_watchpoints") or []
    diagnostics = _pick_attr(review, "portfolio_technical_diagnostics") or []

    def _theme_line(item) -> str:
        theme = _clip_text(_pick_attr(item, "theme"), 18)
        summary = _clip_text(_pick_attr(item, "summary"), 34)
        return f"{theme}：{summary}" if summary else theme

    def _stock_line(item) -> str:
        name = _clip_text(_pick_attr(item, "name"), 12)
        symbol = _clip_text(_pick_attr(item, "symbol"), 12)
        reason = _clip_text(_pick_attr(item, "reason"), 30)
        title = f"{name}({symbol})" if name or symbol else ""
        return f"{title} {reason}".strip()

    lines = [
        "量化之神每日复盘",
        f"交易日：{_pick_attr(review, 'trade_date', '')}",
        "",
        f"市场总览：{_clip_text(market_summary.get('headline'), 72)}",
    ]
    lines.extend(f"- {item}" for item in _normalize_lines(market_summary.get("bullets"), limit=3))
    lines.append("")
    lines.append(f"持仓复盘：{_clip_text(portfolio_summary.get('headline'), 72)}")
    lines.extend(f"- {item}" for item in _normalize_lines(portfolio_summary.get("bullets"), limit=3))
    if current_main_themes:
        lines.append("")
        lines.append("今日主线：")
        lines.extend(f"- {_theme_line(item)}" for item in current_main_themes[:3])
    if next_main_themes:
        lines.append("")
        lines.append("次日主线：")
        lines.extend(f"- {_theme_line(item)}" for item in next_main_themes[:3])
    if next_candidate_stocks:
        lines.append("")
        lines.append("次日候选股：")
        lines.extend(f"- {_stock_line(item)}" for item in next_candidate_stocks[:5])
    if diagnostics:
        lines.append("")
        lines.append("持仓技术提示：")
        for item in diagnostics[:2]:
            t0_plan = _pick_attr(item, "t0_plan") or {}
            volume_price = _pick_attr(item, "volume_price") or {}
            pressure = _pick_attr(_pick_attr(t0_plan, "pressure_zone") or {}, "label") or "需盘中确认"
            support = _pick_attr(_pick_attr(t0_plan, "support_zone") or {}, "label") or "需盘中确认"
            tags = "、".join((_pick_attr(volume_price, "tags") or [])[:2]) if isinstance(_pick_attr(volume_price, "tags"), list) else ""
            lines.append(f"- {_clip_text(_pick_attr(item, 'name'), 10)}({_clip_text(_pick_attr(item, 'symbol'), 12)}) 压力 {pressure} / 支撑 {support} {tags}".strip())
    if risk_watchpoints:
        lines.append("")
        lines.append("风险观察：")
        lines.extend(
            f"- {_clip_text(_pick_attr(item, 'title'), 18)}：{_clip_text(_pick_attr(item, 'detail'), 40)}"
            for item in risk_watchpoints[:3]
        )
    return "\n".join(lines)[:1200]


def build_test_message(content: str | None = None) -> str:
    custom = " ".join(str(content or "").split()).strip()
    message = custom or "量化之神 Webhook 预热\n这是一条企业微信机器人测试消息。"
    return message[:1800]


def normalize_webhook_url(webhook_url: str) -> str:
    normalized = str(webhook_url or "").strip()
    if not normalized:
        raise ValueError("企业微信 Webhook 不能为空")

    if not normalized.startswith("http"):
        if not all(char.isalnum() or char == "-" for char in normalized):
            raise ValueError("企业微信 Webhook key 格式不正确")
        return f"https://{_WECOM_WEBHOOK_HOST}{_WECOM_WEBHOOK_PATH}?key={normalized}"

    parsed = urlparse(normalized)
    if parsed.scheme != "https":
        raise ValueError("企业微信 Webhook 必须使用 HTTPS")
    if parsed.netloc != _WECOM_WEBHOOK_HOST or parsed.path != _WECOM_WEBHOOK_PATH:
        raise ValueError("仅支持企业微信机器人的官方 Webhook 地址")
    if parsed.params or parsed.fragment:
        raise ValueError("企业微信 Webhook 地址格式不正确")

    query = parse_qs(parsed.query, keep_blank_values=False)
    if set(query.keys()) != {"key"}:
        raise ValueError("企业微信 Webhook 地址必须仅包含 key 参数")
    keys = query.get("key") or []
    if len(keys) != 1:
        raise ValueError("企业微信 Webhook 地址格式不正确")
    key = keys[0].strip()
    if not key or not all(char.isalnum() or char == "-" for char in key):
        raise ValueError("企业微信 Webhook key 格式不正确")

    return f"https://{_WECOM_WEBHOOK_HOST}{_WECOM_WEBHOOK_PATH}?{urlencode({'key': key})}"


def send_message(content: str, webhook_url: str) -> bool:
    if not webhook_url:
        return False
    payload = {
        "msgtype": "text",
        "text": {"content": content},
    }
    url = normalize_webhook_url(webhook_url)
    response = requests.post(
        url,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json;charset=utf-8"},
        timeout=10,
    )
    response.raise_for_status()
    try:
        body = response.json()
    except Exception:
        logger.warning(
            "[wecom] non-JSON response body=%s",
            _clip_text(getattr(response, "text", None), 240),
        )
        return False
    return int(body.get("errcode", -1)) == 0


async def send_message_with_retry(content: str, webhook_url: str, *, label: str) -> bool:
    try:
        ok = await asyncio.to_thread(send_message, content, webhook_url)
        if ok:
            logger.info("[wecom] sent OK for %s", label)
            return True
    except Exception as exc:
        logger.warning("[wecom] first send failed for %s: %s", label, exc)

    await asyncio.sleep(15)
    try:
        ok = await asyncio.to_thread(send_message, content, webhook_url)
        if ok:
            logger.info("[wecom] retry sent OK for %s", label)
            return True
    except Exception as exc:
        logger.error("[wecom] retry failed for %s: %s", label, exc)
    return False


async def send_report_message_with_retry(report: "ReportDB", webhook_url: str) -> bool:
    return await send_message_with_retry(
        build_report_message(report),
        webhook_url,
        label=getattr(report, "symbol", "report"),
    )


async def send_daily_review_message_with_retry(review: "DailyReviewDB | dict", webhook_url: str) -> bool:
    return await send_message_with_retry(
        build_daily_review_message(review),
        webhook_url,
        label=_pick_attr(review, "trade_date", "daily-review"),
    )
