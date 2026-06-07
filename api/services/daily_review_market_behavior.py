from __future__ import annotations

from typing import Any


def interpret_market_behavior(market: dict[str, Any]) -> dict[str, Any]:
    """Map objective market data to auditable behavior labels for daily review.

    This layer owns market regime interpretation. LLM output should only restate
    these labels and must not infer unsupported motives from raw numbers.
    """

    market_stats = market.get("market_stats") or {}
    indices = market.get("indices") or []
    sector_gainers = market.get("sector_gainers") or []
    sector_losers = market.get("sector_losers") or []
    sector_inflows = market.get("sector_inflows") or []
    sector_outflows = market.get("sector_outflows") or []

    total_amount = _num(market_stats.get("index_turnover_amount") or market_stats.get("total_amount"))
    amount_change = _num(market_stats.get("amount_change"))
    up_count = _int_or_none(market_stats.get("up_count"))
    down_count = _int_or_none(market_stats.get("down_count"))
    limit_up_count = _int_or_none(market_stats.get("limit_up_count"))
    limit_down_count = _int_or_none(market_stats.get("limit_down_count"))
    promotion_rate = _num(market_stats.get("limit_up_promotion_rate"))
    failed_rate = _num(market_stats.get("failed_limit_up_rate"))
    promotion_count = _int_or_none(market_stats.get("limit_up_promotion_count"))
    promotion_base = _int_or_none(market_stats.get("limit_up_promotion_base"))
    failed_count = _int_or_none(market_stats.get("failed_limit_up_count"))
    touch_count = _int_or_none(market_stats.get("limit_up_touch_count"))

    amount_trillion = (total_amount / 1_000_000_000_000) if total_amount is not None else None
    breadth_sample_count = (
        up_count + down_count
        if up_count is not None and down_count is not None
        else None
    )
    breadth_ratio = (
        up_count / max(down_count, 1)
        if breadth_sample_count is not None and breadth_sample_count > 0
        else None
    )
    breadth_gap_pct = (
        (up_count - down_count) / max(up_count + down_count, 1) * 100
        if breadth_sample_count is not None and breadth_sample_count > 0
        else None
    )

    index_map = {str(item.get("symbol") or item.get("name") or "").upper(): item for item in indices}
    star50_pct = _index_pct(indices, {"000688.SH", "科创50"})
    chinext_pct = _index_pct(indices, {"399006.SZ", "创业板指"})
    sh_pct = _index_pct(indices, {"000001.SH", "上证指数"})
    positive_index_count = sum(1 for item in indices if (_num(item.get("change_pct")) or 0) > 0)

    leader_names = _sector_names(sector_gainers, limit=5)
    laggard_names = _sector_names(sector_losers, limit=5)
    inflow_names = _sector_names(sector_inflows, limit=5)
    outflow_names = _sector_names(sector_outflows, limit=5)
    tech_leaders = {"电子", "通信", "计算机", "半导体", "芯片", "消费电子"} & set(leader_names)
    defensive_laggards = {"银行", "非银金融", "保险", "证券", "食品饮料", "公用事业"} & set(laggard_names)

    missing_fields: list[str] = []
    if total_amount is None:
        missing_fields.append("total_amount")
    if up_count is None or down_count is None or breadth_sample_count == 0:
        missing_fields.append("breadth")
    if promotion_rate is None:
        missing_fields.append("limit_up_promotion_rate")
    if failed_rate is None:
        missing_fields.append("failed_limit_up_rate")
    if not indices:
        missing_fields.append("indices")
    if not sector_gainers and not sector_losers:
        missing_fields.append("sector_rankings")

    liquidity_state = _liquidity_state(amount_trillion, amount_change)
    breadth_state = _breadth_state(up_count, down_count, breadth_ratio, breadth_gap_pct)
    sentiment_state = _sentiment_state(
        promotion_rate=promotion_rate,
        promotion_count=promotion_count,
        promotion_base=promotion_base,
        failed_rate=failed_rate,
        failed_count=failed_count,
        touch_count=touch_count,
        limit_up_count=limit_up_count,
        limit_down_count=limit_down_count,
    )
    market_regime = _market_regime(
        liquidity_label=liquidity_state["label"],
        breadth_label=breadth_state["label"],
        breadth_ratio=breadth_ratio,
        positive_index_count=positive_index_count,
        star50_pct=star50_pct,
        chinext_pct=chinext_pct,
        sh_pct=sh_pct,
        tech_hot=bool(tech_leaders),
        promotion_rate=promotion_rate,
        failed_rate=failed_rate,
    )
    style_rotation = _style_rotation(
        leader_names=leader_names,
        laggard_names=laggard_names,
        inflow_names=inflow_names,
        outflow_names=outflow_names,
        defensive_laggards=defensive_laggards,
        amount_trillion=amount_trillion,
        breadth_ratio=breadth_ratio,
    )
    sector_battlefield = _sector_battlefield(leader_names, laggard_names, inflow_names, outflow_names)
    risk_pressure = _risk_pressure(
        liquidity_label=liquidity_state["label"],
        sentiment_label=sentiment_state["label"],
        breadth_label=breadth_state["label"],
        failed_rate=failed_rate,
        promotion_rate=promotion_rate,
        leader_names=leader_names,
        laggard_names=laggard_names,
    )

    locked_values = {
        "total_amount": total_amount,
        "total_amount_label": _format_amount_cn(total_amount),
        "amount_trillion": round(amount_trillion, 4) if amount_trillion is not None else None,
        "up_count": up_count,
        "down_count": down_count,
        "breadth_ratio": round(breadth_ratio, 4) if breadth_ratio is not None else None,
        "breadth_gap_pct": round(breadth_gap_pct, 2) if breadth_gap_pct is not None else None,
        "limit_up_count": limit_up_count,
        "limit_down_count": limit_down_count,
        "limit_up_promotion_rate": promotion_rate,
        "limit_up_promotion_rate_label": _format_rate(promotion_rate),
        "limit_up_promotion_count": promotion_count,
        "limit_up_promotion_base": promotion_base,
        "failed_limit_up_rate": failed_rate,
        "failed_limit_up_rate_label": _format_rate(failed_rate),
        "failed_limit_up_count": failed_count,
        "limit_up_touch_count": touch_count,
        "star50_change_pct": star50_pct,
        "chinext_change_pct": chinext_pct,
        "shanghai_change_pct": sh_pct,
    }

    labels = {
        "liquidity_state": liquidity_state,
        "breadth_state": breadth_state,
        "market_regime": market_regime,
        "sentiment_state": sentiment_state,
        "style_rotation": style_rotation,
        "sector_battlefield": sector_battlefield,
        "risk_pressure": risk_pressure,
        "locked_values": locked_values,
        "data_quality": {
            "missing_fields": missing_fields,
            "source": {
                "market_stats": market_stats.get("source"),
                "sentiment": market_stats.get("sentiment_source"),
                "sector_rankings": "market_snapshot.sector_gainers/sector_losers",
            },
        },
    }
    labels["narrative_anchors"] = [
        item.get("detail") or item.get("label")
        for item in [
            liquidity_state,
            breadth_state,
            market_regime,
            sentiment_state,
            style_rotation,
            sector_battlefield,
            risk_pressure,
        ]
        if item.get("label") or item.get("detail")
    ]
    return labels


def _liquidity_state(amount_trillion: float | None, amount_change: float | None) -> dict[str, Any]:
    if amount_trillion is None:
        return {"label": "成交额数据缺失", "detail": "当前缺少两市成交额，不能定性流动性强弱。", "score": None}
    if amount_trillion >= 2.5:
        label = "流动性极度充沛"
    elif amount_trillion >= 2.0:
        label = "流动性高位外溢"
    elif amount_trillion >= 1.5:
        label = "流动性充沛"
    elif amount_trillion >= 1.0:
        label = "流动性活跃"
    else:
        label = "存量博弈/缩量约束"
    direction = ""
    if amount_change is not None:
        direction = "，较前一交易日放量" if amount_change > 0 else ("，较前一交易日缩量" if amount_change < 0 else "，较前一交易日持平")
    return {
        "label": label,
        "detail": f"{label}：两市成交约 {_format_amount_cn(amount_trillion * 1_000_000_000_000)}{direction}。",
        "score": round(amount_trillion, 4),
    }


def _breadth_state(
    up_count: int | None,
    down_count: int | None,
    breadth_ratio: float | None,
    breadth_gap_pct: float | None,
) -> dict[str, Any]:
    if up_count is None or down_count is None or breadth_ratio is None:
        return {"label": "涨跌家数缺失", "detail": "当前缺少涨跌家数，不能判断赚钱效应扩散。", "score": None}
    if breadth_ratio >= 2.0:
        label = "全市场右侧多头普涨修复"
    elif breadth_ratio >= 1.2:
        label = "赚钱效应温和扩散"
    elif breadth_ratio >= 0.8:
        label = "结构性分化轮动"
    else:
        label = "个股失血/指数失真压力"
    return {
        "label": label,
        "detail": f"{label}：上涨 {up_count} 家，下跌 {down_count} 家，涨跌比 {breadth_ratio:.2f}，广度差 {breadth_gap_pct:+.1f}%。",
        "score": round(breadth_ratio, 4),
    }


def _sentiment_state(
    *,
    promotion_rate: float | None,
    promotion_count: int | None,
    promotion_base: int | None,
    failed_rate: float | None,
    failed_count: int | None,
    touch_count: int | None,
    limit_up_count: int | None,
    limit_down_count: int | None,
) -> dict[str, Any]:
    metric_parts: list[str] = []
    if promotion_rate is not None:
        metric_parts.append(f"连板晋级率 {_format_rate(promotion_rate)}（{promotion_count or 0}/{promotion_base or 0}）")
    else:
        metric_parts.append("连板晋级率未覆盖")
    if failed_rate is not None:
        metric_parts.append(f"炸板率 {_format_rate(failed_rate)}（炸板 {failed_count or 0}/触板 {touch_count or 0}）")
    else:
        metric_parts.append("炸板率未覆盖")
    if limit_up_count is not None or limit_down_count is not None:
        metric_parts.append(f"涨停/近涨停 {limit_up_count or 0} 只，跌停/近跌停 {limit_down_count or 0} 只")

    if promotion_rate is None and failed_rate is None:
        label = "短线情绪数据不足"
    elif promotion_rate is not None and failed_rate is not None and promotion_rate >= 40 and failed_rate <= 25:
        label = "接力情绪强修复/主升偏好"
    elif (promotion_rate is not None and promotion_rate < 15) or (failed_rate is not None and failed_rate >= 45):
        label = "高位接力强分歧/退潮压力"
    elif promotion_rate is not None and promotion_rate >= 25 and (failed_rate is None or failed_rate <= 35):
        label = "接力情绪修复但后排需承接"
    else:
        label = "结构性投机博弈"
    return {
        "label": label,
        "detail": f"{label}：{'；'.join(metric_parts)}。",
        "score": {
            "promotion_rate": promotion_rate,
            "failed_rate": failed_rate,
        },
    }


def _market_regime(
    *,
    liquidity_label: str,
    breadth_label: str,
    breadth_ratio: float | None,
    positive_index_count: int,
    star50_pct: float | None,
    chinext_pct: float | None,
    sh_pct: float | None,
    tech_hot: bool,
    promotion_rate: float | None,
    failed_rate: float | None,
) -> dict[str, Any]:
    star_or_chinext_strong = max(star50_pct or -999, chinext_pct or -999) >= 2.0
    if failed_rate is not None and failed_rate >= 45 and (promotion_rate is None or promotion_rate < 20):
        label = "高位接力强分歧"
    elif breadth_ratio is not None and breadth_ratio < 0.8 and positive_index_count > 0:
        label = "指数托举/个股失血"
    elif star_or_chinext_strong and tech_hot and (breadth_ratio is None or breadth_ratio < 1.5):
        label = "硬科技权重抱团/主线虹吸"
    elif breadth_ratio is not None and breadth_ratio >= 2.0 and "流动性" in liquidity_label:
        label = "流动性外溢普涨修复"
    elif promotion_rate is not None and promotion_rate >= 40 and (failed_rate is None or failed_rate <= 25):
        label = "情绪主升逼空"
    else:
        label = "结构性轮动"
    index_parts = []
    if sh_pct is not None:
        index_parts.append(f"上证 {_format_signed_pct(sh_pct)}")
    if chinext_pct is not None:
        index_parts.append(f"创业板 {_format_signed_pct(chinext_pct)}")
    if star50_pct is not None:
        index_parts.append(f"科创50 {_format_signed_pct(star50_pct)}")
    return {
        "label": label,
        "detail": f"{label}：{breadth_label}；{'、'.join(index_parts) if index_parts else '指数数据不足'}。",
        "score": {
            "star50_change_pct": star50_pct,
            "chinext_change_pct": chinext_pct,
            "shanghai_change_pct": sh_pct,
        },
    }


def _style_rotation(
    *,
    leader_names: list[str],
    laggard_names: list[str],
    inflow_names: list[str],
    outflow_names: list[str],
    defensive_laggards: set[str],
    amount_trillion: float | None,
    breadth_ratio: float | None,
) -> dict[str, Any]:
    if not leader_names and not laggard_names:
        return {"label": "风格轮动数据不足", "detail": "当前缺少板块涨跌数据，不能判断风格切换。", "score": None}
    if defensive_laggards and amount_trillion is not None and amount_trillion >= 1.5 and breadth_ratio is not None and breadth_ratio >= 1.2:
        label = "防御资产承压/进攻风格占优"
    elif leader_names and laggard_names:
        label = "主线与弱势板块跷跷板"
    else:
        label = "板块强弱单边偏移"
    detail = f"{label}：强势板块 {'、'.join(leader_names[:4]) or '未覆盖'}；承压板块 {'、'.join(laggard_names[:4]) or '未覆盖'}。"
    if inflow_names or outflow_names:
        detail += f" 资金流入 {'、'.join(inflow_names[:3]) or '未覆盖'}，流出 {'、'.join(outflow_names[:3]) or '未覆盖'}。"
    return {
        "label": label,
        "detail": detail,
        "score": {
            "leaders": leader_names[:5],
            "laggards": laggard_names[:5],
            "inflows": inflow_names[:5],
            "outflows": outflow_names[:5],
        },
    }


def _sector_battlefield(
    leader_names: list[str],
    laggard_names: list[str],
    inflow_names: list[str],
    outflow_names: list[str],
) -> dict[str, Any]:
    if not leader_names and not inflow_names:
        return {"label": "主线未确认", "detail": "当前板块强度与资金流数据不足，绝对主线不能硬判。", "score": None}
    leaders = leader_names or inflow_names
    label = "主线战场集中"
    detail = f"{label}：盘面强度集中在 {'、'.join(leaders[:4])}。"
    if laggard_names or outflow_names:
        detail += f" 弱势/流出方向为 {'、'.join((laggard_names or outflow_names)[:4])}。"
    return {
        "label": label,
        "detail": detail,
        "score": {
            "leaders": leaders[:5],
            "laggards": (laggard_names or outflow_names)[:5],
        },
    }


def _risk_pressure(
    *,
    liquidity_label: str,
    sentiment_label: str,
    breadth_label: str,
    failed_rate: float | None,
    promotion_rate: float | None,
    leader_names: list[str],
    laggard_names: list[str],
) -> dict[str, Any]:
    if failed_rate is not None and failed_rate >= 45:
        label = "封板质量风险"
        detail = "炸板率偏高，次日先看前排回封和高位承接，后排冲高按套利处理。"
    elif promotion_rate is not None and promotion_rate < 15:
        label = "连板接力降温风险"
        detail = "连板晋级偏弱，短线高位股不适合无条件追涨。"
    elif "普涨" in breadth_label and "流动性" in liquidity_label:
        label = "普涨后分化风险"
        detail = "流动性扩散后容易进入强弱切换，次日要看主线前排是否继续承接。"
    else:
        label = "结构性执行风险"
        detail = "指数、板块和个股节奏可能不同步，执行上以开盘半小时量能和主线承接为锚。"
    if leader_names:
        detail += f" 强势方向关注 {'、'.join(leader_names[:3])}。"
    if laggard_names:
        detail += f" 弱势方向回避 {'、'.join(laggard_names[:3])} 的无量反抽。"
    return {
        "label": label,
        "detail": f"{label}：{detail}",
        "score": {
            "sentiment_label": sentiment_label,
            "failed_rate": failed_rate,
            "promotion_rate": promotion_rate,
        },
    }


def _index_pct(indices: list[dict[str, Any]], aliases: set[str]) -> float | None:
    normalized_aliases = {item.upper() for item in aliases}
    for item in indices:
        symbol = str(item.get("symbol") or "").upper()
        name = str(item.get("name") or "").upper()
        if symbol in normalized_aliases or name in normalized_aliases:
            return _num(item.get("change_pct"))
    return None


def _sector_names(items: list[dict[str, Any]], limit: int = 5) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for item in items:
        name = str(item.get("sector_name") or item.get("theme") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        names.append(name)
        if len(names) >= limit:
            break
    return names


def _num(value: Any) -> float | None:
    try:
        number = float(value)
    except Exception:
        return None
    return number if number == number else None


def _int_or_none(value: Any) -> int | None:
    number = _num(value)
    return int(number) if number is not None else None


def _format_rate(value: Any) -> str:
    number = _num(value)
    return "--" if number is None else f"{number:.2f}%"


def _format_signed_pct(value: Any) -> str:
    number = _num(value)
    return "--" if number is None else f"{number:+.2f}%"


def _format_amount_cn(value: Any) -> str:
    amount = _num(value)
    if amount is None:
        return "--"
    if amount >= 1_000_000_000_000:
        return f"{amount / 1_000_000_000_000:.2f} 万亿元"
    if amount >= 100_000_000:
        return f"{amount / 100_000_000:.0f} 亿元"
    if amount >= 10_000:
        return f"{amount / 10_000:.0f} 万元"
    return f"{amount:.0f} 元"
