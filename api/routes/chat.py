from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import threading
from collections import OrderedDict
from contextlib import suppress
from datetime import datetime
from statistics import mean
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import text

from api.core.runtime_config import has_mixed_account_llm_runtime, llm_runtime_source_payload
from api.core.progress_tracker import AgentProgressTracker
from api.database import get_db_ctx
from api.services.market_data_pipeline_service import preferred_daily_kline_table
from api.deps import require_api_user
from api.schemas.analysis import AnalyzeRequest
from api.services import report_service

router = APIRouter(prefix="/v1", tags=["Chat"])
logger = logging.getLogger(__name__)
_GRAPH_CACHE_MAX_SIZE = int(os.getenv("TRADING_GRAPH_CACHE_SIZE", "4"))
_GRAPH_CACHE: OrderedDict[str, Any] = OrderedDict()
_GRAPH_CACHE_LOCK = threading.Lock()


def _mixed_runtime_error_message(config: dict[str, Any]) -> str:
    runtime_sources = llm_runtime_source_payload(config)
    package_source = runtime_sources.get("runtime_package_source") or "unknown"
    account_sources = ",".join(runtime_sources.get("account_runtime_sources") or []) or "-"
    return (
        "账号 LLM 字段未形成同源运行包；provider、Base URL、模型和 Key 必须来自同一套账号配置。"
        f" 当前 runtime_package_source={package_source}, account_sources={account_sources}。"
    )


def _reject_mixed_account_runtime(config: dict[str, Any]) -> None:
    if has_mixed_account_llm_runtime(config):
        raise HTTPException(status_code=400, detail=_mixed_runtime_error_message(config))


def _get_deep_analysis_graph(selected_analysts: list[str], config: dict[str, Any]):
    from tradingagents.graph.trading_graph import TradingAgentsGraph

    if _GRAPH_CACHE_MAX_SIZE <= 0:
        return TradingAgentsGraph(
            selected_analysts=selected_analysts,
            debug=False,
            config=dict(config),
        )

    cache_key = _trading_graph_cache_key(selected_analysts, config)
    with _GRAPH_CACHE_LOCK:
        cached = _GRAPH_CACHE.get(cache_key)
        if cached is not None:
            _GRAPH_CACHE.move_to_end(cache_key)
            return cached

    graph = TradingAgentsGraph(
        selected_analysts=selected_analysts,
        debug=False,
        config=dict(config),
    )
    with _GRAPH_CACHE_LOCK:
        existing = _GRAPH_CACHE.get(cache_key)
        if existing is not None:
            return existing
        _GRAPH_CACHE[cache_key] = graph
        _GRAPH_CACHE.move_to_end(cache_key)
        while len(_GRAPH_CACHE) > _GRAPH_CACHE_MAX_SIZE:
            _GRAPH_CACHE.popitem(last=False)
    return graph


def _trading_graph_cache_key(selected_analysts: list[str], config: dict[str, Any]) -> str:
    api_key = str(config.get("api_key") or "")
    payload = {
        "selected_analysts": sorted({str(item) for item in selected_analysts}),
        "llm_provider": config.get("llm_provider"),
        "deep_think_llm": config.get("deep_think_llm"),
        "quick_think_llm": config.get("quick_think_llm"),
        "backend_url": config.get("backend_url"),
        "api_key_hash": hashlib.sha256(api_key.encode("utf-8")).hexdigest() if api_key else "",
        "max_debate_rounds": config.get("max_debate_rounds"),
        "max_risk_discuss_rounds": config.get("max_risk_discuss_rounds"),
        "max_recur_limit": config.get("max_recur_limit"),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


@router.post("/chat/completions")
async def chat_completions(request: dict = Body(...), current_user=Depends(require_api_user)):
    from api import main as compat

    text = compat._extract_chat_text(request.get("messages") or [])
    config = compat._build_runtime_config({}, user_id=current_user.id)
    _reject_mixed_account_runtime(config)
    symbol, trade_date, horizons, focus_areas, specific_questions, user_context = compat._ai_extract_symbol_and_date(text, config)
    del horizons, focus_areas, specific_questions, user_context
    if not symbol:
        fallback_symbol_raw = str(request.get("symbol") or request.get("current_symbol") or "").strip()
        if fallback_symbol_raw:
            candidate = compat.search_cn_stock_by_name(fallback_symbol_raw) or compat.normalize_symbol(fallback_symbol_raw)
            reverse_map = compat._get_reverse_stock_map()
            if candidate and (not reverse_map or candidate in reverse_map or candidate.endswith((".SH", ".SZ"))):
                symbol = candidate
    if not symbol:
        raise HTTPException(status_code=400, detail="抱歉，我没能从您的消息中识别出股票标的。请输入代码（如 600519.SH）或可识别的公司名称。")

    selected_analysts = _resolve_selected_analysts(
        requested=request.get("selected_analysts"),
        user_id=current_user.id,
    )

    analyze_request = AnalyzeRequest(
        symbol=symbol,
        trade_date=trade_date or compat.cn_today_str(),
        query=text,
        dry_run=bool(request.get("dry_run")),
        selected_analysts=selected_analysts,
    )
    job_id = f"{datetime.now().timestamp():.0f}".replace(".", "") + symbol.replace(".", "")
    job_id = job_id[-24:]

    if request.get("stream", True):
        async def event_stream():
            tracker = AgentProgressTracker(
                on_update=lambda snapshot: compat._set_job(
                    job_id,
                    progress=snapshot,
                    current_agent=snapshot.get("current_agent"),
                    current_stage=snapshot.get("current_stage"),
                    analysis_stage=snapshot.get("analysis_stage"),
                )
            )
            try:
                compat._set_job(
                    job_id,
                    user_id=current_user.id,
                    status="pending",
                    symbol=symbol,
                    trade_date=analyze_request.trade_date,
                    current_stage="queued",
                    analysis_stage="queued",
                )
                tracker.mark_stage("ready", "queued")
                yield _sse_pack("job.ready", {"job_id": job_id, "symbol": symbol})
                yield _sse_pack("job.created", {"job_id": job_id, "symbol": symbol})
                await asyncio.sleep(0.05)
                compat._set_job(job_id, status="running")
                tracker.mark_stage("running", "initializing")
                yield _sse_pack("job.running", {"job_id": job_id, "symbol": symbol, "msg": f"开始分析 {symbol}"})

                analysis_task = asyncio.create_task(
                    _run_analysis_with_fallback(
                        symbol=symbol,
                        trade_date=analyze_request.trade_date or compat.cn_today_str(),
                        query=text,
                        user_id=current_user.id,
                        selected_analysts=selected_analysts,
                        tracker=tracker,
                        job_id=job_id,
                    )
                )

                while True:
                    if analysis_task.done() and tracker.empty():
                        break
                    pending_event = await tracker.next_event()
                    if pending_event is None:
                        continue
                    event_name, data = pending_event
                    yield _sse_pack(event_name, data)

                result_payload, risk_items, key_metrics, decision = await analysis_task

                tracker.mark_stage("finalizing", "result_persistence")
                for event_name, data in _finalize_section_events(tracker, result_payload):
                    yield _sse_pack(event_name, data)

                with get_db_ctx() as db:
                    report_service.create_report(
                        db,
                        symbol=symbol,
                        trade_date=analyze_request.trade_date or compat.cn_today_str(),
                        decision=decision,
                        result_data=result_payload,
                        user_id=current_user.id,
                        risk_items=risk_items,
                        key_metrics=key_metrics,
                        analyst_traces=list(result_payload.get("analyst_traces") or []),
                        report_id=job_id,
                    )

                compat._set_job(
                    job_id,
                    status="completed",
                    decision=decision,
                    result=result_payload,
                    symbol=symbol,
                    trade_date=analyze_request.trade_date,
                )
                tracker.mark_stage("completed", "completed")
                yield _sse_pack(
                    "job.completed",
                    {
                        "job_id": job_id,
                        "symbol": symbol,
                        "decision": decision,
                        "direction": result_payload.get("direction") or "中性",
                        "result": result_payload,
                        "risk_items": risk_items,
                        "key_metrics": key_metrics,
                        "confidence": _extract_confidence(result_payload.get("final_trade_decision")),
                        "target_price": result_payload.get("target_price"),
                        "stop_loss_price": result_payload.get("stop_loss_price"),
                    },
                )
            except Exception as exc:
                logger.exception("Streaming chat analysis failed for %s", symbol)
                with suppress(Exception):
                    compat._set_job(job_id, status="failed", error=str(exc), symbol=symbol, trade_date=analyze_request.trade_date)
                tracker.mark_stage("failed", "failed")
                yield _sse_pack("job.failed", {"job_id": job_id, "symbol": symbol, "error": f"智能分析失败：{exc}"})
            finally:
                yield "event: done\ndata: [DONE]\n\n"

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    if request.get("dry_run"):
        with get_db_ctx() as db:
            imported_context = compat._build_manual_imported_user_context(db, current_user.id, symbol)
        result = {
            "status": "completed",
            "decision": "DRY_RUN",
            "job_id": job_id,
            "symbol": symbol,
            "query": text,
            "selected_analysts": analyze_request.selected_analysts,
            "user_context": imported_context,
        }
        compat._set_job(
            job_id,
            user_id=current_user.id,
            status="completed",
            decision="DRY_RUN",
            symbol=symbol,
            trade_date=analyze_request.trade_date,
            result=result,
        )
        return {
            "id": f"chatcmpl-{job_id}",
            "object": "chat.completion",
            "created": int(datetime.now().timestamp()),
            "model": request.get("model"),
            "choices": [{"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": f"已完成分析任务：{job_id}"}}],
        }

    compat._set_job(
        job_id,
        user_id=current_user.id,
        status="queued",
        symbol=symbol,
        trade_date=analyze_request.trade_date,
        current_stage="queued",
        analysis_stage="queued",
        progress={
            "status": {},
            "current_agent": None,
            "current_stage": "queued",
            "analysis_stage": "queued",
            "debate": {"name": None, "agent": None, "round": None, "is_verdict": False},
            "completed_agents": [],
            "has_streamed_content": False,
        },
    )
    asyncio.create_task(
        _run_background_analysis_job(
            job_id=job_id,
            symbol=symbol,
            trade_date=analyze_request.trade_date,
            query=text,
            user_id=current_user.id,
            selected_analysts=selected_analysts,
        )
    )
    return {
        "id": f"chatcmpl-{job_id}",
        "object": "chat.completion",
        "created": int(datetime.now().timestamp()),
        "model": request.get("model"),
        "choices": [{"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": f"已启动分析任务：{job_id}"}}],
    }


def _build_lightweight_analysis(symbol: str, trade_date: str, query: str, user_id: str) -> tuple[dict, list[dict], list[dict], str]:
    code = symbol.split(".", 1)[0]
    rows = _load_recent_daily_rows(code=code, symbol=symbol)

    closes = [float(row["close"]) for row in rows if row.get("close") is not None]
    latest_close = closes[0] if closes else None
    prev_close = closes[1] if len(closes) > 1 else None
    ma5 = mean(closes[:5]) if len(closes) >= 5 else latest_close
    ma20 = mean(closes[:20]) if len(closes) >= 20 else latest_close
    ret5 = ((latest_close / closes[4]) - 1) * 100 if latest_close and len(closes) >= 5 and closes[4] else None
    day_change = ((latest_close / prev_close) - 1) * 100 if latest_close and prev_close else None
    latest_turnover = _safe_float(rows[0].get("turnover_rate")) if rows else None
    latest_mcap = _safe_float(rows[0].get("float_market_cap")) if rows else None
    latest_profit = _safe_float(rows[0].get("net_profit_ttm")) if rows else None
    target_price = None
    stop_loss_price = None

    if latest_close is None:
        direction = "中性"
        decision = "WATCH"
        confidence = 38
        market_report = f"## 市场分析\n\n未能读取 {symbol} 的近期日线数据，当前无法做出高置信度判断。建议先检查数据源或稍后再试。"
        volume_price_report = "## 量价分析\n\n缺少可用行情数据，量价分析暂不可用。"
    else:
        bullish = bool(ma20 and latest_close > ma20 and (ret5 or 0) > 0)
        bearish = bool(ma20 and latest_close < ma20 and (ret5 or 0) < 0)
        if bullish:
            direction, decision, confidence = "偏多", "BUY", 68
        elif bearish:
            direction, decision, confidence = "偏空", "WATCH", 64
        else:
            direction, decision, confidence = "中性", "HOLD", 52
        if decision == "BUY":
            target_price = round(latest_close * 1.06, 2)
            stop_loss_price = round(latest_close * 0.96, 2)
        elif decision == "HOLD":
            target_price = round(latest_close * 1.03, 2)
            stop_loss_price = round(latest_close * 0.97, 2)
        else:
            target_price = round(latest_close * 1.02, 2)
            stop_loss_price = round(latest_close * 0.97, 2)
        market_report = (
            f"## 市场分析\n\n- 标的：`{symbol}`\n"
            f"- 分析日期：`{trade_date}`\n"
            f"- 最新收盘：`{latest_close:.2f}`\n"
            f"- 单日涨跌：`{(day_change or 0):.2f}%`\n"
            f"- 近 5 日涨跌：`{(ret5 or 0):.2f}%`\n"
            f"- MA5 / MA20：`{(ma5 or 0):.2f}` / `{(ma20 or 0):.2f}`\n\n"
            f"当前依据本地轻量分析结果，趋势判断为 **{direction}**。"
        )
        volume_price_report = (
            "## 量价分析\n\n"
            f"- 最近换手率：`{latest_turnover:.2f}%`\n" if latest_turnover is not None else "## 量价分析\n\n"
        ) + (
            f"- 当前价格相对 MA20 {'上方' if ma20 and latest_close > ma20 else '下方'}\n"
            f"- 近 5 日动量 {'为正' if (ret5 or 0) > 0 else '为负或走平'}\n"
        )

    sentiment_report = f"## 舆情分析\n\n当前请求：{query or f'分析 {symbol}'}。\n\n当前版本已恢复请求闭环，但舆情大模型链路暂未接入实时输出，先给出行情侧结论。"
    news_report = "## 新闻分析\n\n当前未接入实时新闻抓取流，本次结果未纳入突发公告与新闻事件影响。"
    fundamentals_report = (
        "## 基本面分析\n\n"
        f"- 流通市值：`{latest_mcap / 1e8:.2f} 亿` \n" if latest_mcap else "## 基本面分析\n\n"
    ) + (
        f"- 近一期净利润 TTM：`{latest_profit / 1e8:.2f} 亿`\n" if latest_profit else "- 当前未读取到净利润 TTM 字段。\n"
    )
    macro_report = "## 宏观板块分析\n\n本次先以标的自身行情为主，宏观与板块结论待完整图谱恢复后补齐。"
    smart_money_report = "## 主力资金分析\n\n当前轻量模式未接入龙虎榜与主力净流入明细，仅保留量价和换手代理判断。"
    investment_plan = f"## 研究结论\n\n结合当前本地行情特征，建议结论为 **{direction}**，执行动作倾向 **{decision}**。"
    trader_investment_plan = (
        "## 交易计划\n\n"
        f"- 建议动作：`{decision}`\n"
        f"- 建议跟踪位：`{latest_close:.2f}`\n"
        f"- 目标价：`{target_price:.2f}`\n"
        f"- 止损价：`{stop_loss_price:.2f}`\n" if latest_close is not None and target_price is not None and stop_loss_price is not None else "## 交易计划\n\n- 当前无足够数据生成交易计划。\n"
    )
    final_trade_decision = (
        f"方向：{direction}\n"
        f"动作：{decision}\n"
        f"置信度：{confidence}%\n"
        + (f"目标价：{target_price:.2f}\n止损价：{stop_loss_price:.2f}\n" if target_price is not None and stop_loss_price is not None else "")
        +
        "说明：当前为本地轻量分析结果，用于恢复智能分析闭环和快速排障。"
    )

    risk_items = [
        {"name": "实时链路", "level": "medium", "description": "当前为轻量流式分析链路，完整多 Agent 深度分析未完全恢复。"},
        {"name": "新闻舆情缺口", "level": "medium", "description": "未纳入实时新闻、舆情和公告突发影响。"},
    ]
    key_metrics = [
        {"name": "当前方向", "value": direction, "status": "neutral" if direction == "中性" else ("good" if direction == "偏多" else "bad")},
        {"name": "执行动作", "value": decision, "status": "neutral" if decision == "HOLD" else ("good" if decision == "BUY" else "bad")},
        {"name": "近5日涨跌", "value": f"{(ret5 or 0):.2f}%", "status": "good" if (ret5 or 0) > 0 else ("bad" if (ret5 or 0) < 0 else "neutral")},
    ]

    result_payload = {
        "symbol": symbol,
        "trade_date": trade_date,
        "query": query,
        "decision": decision,
        "direction": direction,
        "analysis_mode": "lightweight",
        "market_report": market_report,
        "sentiment_report": sentiment_report,
        "news_report": news_report,
        "fundamentals_report": fundamentals_report,
        "macro_report": macro_report,
        "smart_money_report": smart_money_report,
        "volume_price_report": volume_price_report,
        "investment_plan": investment_plan,
        "trader_investment_plan": trader_investment_plan,
        "final_trade_decision": final_trade_decision,
        "target_price": target_price,
        "stop_loss_price": stop_loss_price,
        "user_context": _build_user_context_snapshot(symbol, user_id),
    }
    return result_payload, risk_items, key_metrics, decision


async def _run_analysis_with_fallback(
    symbol: str,
    trade_date: str,
    query: str,
    user_id: str,
    selected_analysts: list[str],
    tracker: AgentProgressTracker,
    job_id: str,
    allow_lightweight_fallback: bool = True,
) -> tuple[dict, list[dict], list[dict], str]:
    try:
        return await _run_deep_analysis_pipeline(
            symbol=symbol,
            trade_date=trade_date,
            query=query,
            user_id=user_id,
            selected_analysts=selected_analysts,
            tracker=tracker,
            job_id=job_id,
        )
    except Exception:
        logger.exception("Deep multi-agent analysis failed for %s", symbol)
        if tracker.has_streamed_content or not allow_lightweight_fallback:
            raise
        return _build_lightweight_analysis(
            symbol=symbol,
            trade_date=trade_date,
            query=query,
            user_id=user_id,
        )


async def _run_background_analysis_job(
    job_id: str,
    symbol: str,
    trade_date: str,
    query: str,
    user_id: str,
    selected_analysts: list[str],
) -> None:
    from api import main as compat

    tracker = AgentProgressTracker(
        emit_events=False,
        on_update=lambda snapshot: compat._set_job(
            job_id,
            progress=snapshot,
            current_agent=snapshot.get("current_agent"),
            current_stage=snapshot.get("current_stage"),
            analysis_stage=snapshot.get("analysis_stage"),
        ),
    )
    compat._set_job(job_id, status="running", symbol=symbol, trade_date=trade_date)
    tracker.mark_stage("running", "initializing")
    try:
        result_payload, risk_items, key_metrics, decision = await _run_analysis_with_fallback(
            symbol=symbol,
            trade_date=trade_date,
            query=query,
            user_id=user_id,
            selected_analysts=selected_analysts,
            tracker=tracker,
            job_id=job_id,
            allow_lightweight_fallback=False,
        )
        tracker.mark_stage("finalizing", "result_persistence")
        with get_db_ctx() as db:
            report_service.create_report(
                db,
                symbol=symbol,
                trade_date=trade_date,
                decision=decision,
                result_data=result_payload,
                user_id=user_id,
                risk_items=risk_items,
                key_metrics=key_metrics,
                analyst_traces=list(result_payload.get("analyst_traces") or []),
                report_id=job_id,
            )
        compat._set_job(
            job_id,
            status="completed",
            decision=decision,
            result=result_payload,
            symbol=symbol,
            trade_date=trade_date,
        )
        tracker.mark_stage("completed", "completed")
    except Exception as exc:
        logger.exception("Background chat analysis failed for %s", symbol)
        error_message = f"智能分析失败：{exc}"
        with get_db_ctx() as db:
            report_service.update_report_partial(db, job_id, status="failed", error=error_message)
        compat._set_job(
            job_id,
            status="failed",
            error=error_message,
            symbol=symbol,
            trade_date=trade_date,
        )
        tracker.mark_stage("failed", "failed")


async def _run_deep_analysis_pipeline(
    symbol: str,
    trade_date: str,
    query: str,
    user_id: str,
    selected_analysts: list[str],
    tracker: AgentProgressTracker,
    job_id: str,
) -> tuple[dict, list[dict], list[dict], str]:
    from api import main as compat
    from tradingagents.agents.utils.agent_states import current_tracker_var
    from tradingagents.graph.intent_parser import parse_intent

    config = compat._build_runtime_config({}, user_id=user_id)
    _reject_mixed_account_runtime(config)
    merged_user_context = _build_user_context_snapshot(symbol, user_id)
    graph = _get_deep_analysis_graph(selected_analysts, config)

    tracker_token = current_tracker_var.set(tracker)
    graph.data_collector.ref(symbol, trade_date)
    try:
        user_intent = parse_intent(query or f"分析 {symbol}", graph.quick_thinking_llm, fallback_ticker=symbol)
        user_intent["user_context"] = _merge_user_contexts(
            merged_user_context,
            user_intent.get("user_context") or {},
        )

        await asyncio.to_thread(
            graph.data_collector.collect,
            symbol,
            trade_date,
            selected_analysts=selected_analysts,
        )
        state = graph.propagator.create_initial_state(
            company_name=symbol,
            trade_date=trade_date,
            user_context=user_intent.get("user_context") or {},
            selected_analysts=selected_analysts,
            request_source="chat",
            user_intent=user_intent,
            horizon="short",
        )
        args = graph.propagator.get_graph_args()
        args["config"]["configurable"] = {"thread_id": f"chat:{job_id}"}
        final_state = await graph.graph.ainvoke(state, **args)
    finally:
        current_tracker_var.reset(tracker_token)
        with suppress(Exception):
            graph.data_collector.evict(symbol, trade_date)

    return _build_deep_result_payload(
        symbol=symbol,
        trade_date=trade_date,
        query=query,
        selected_analysts=selected_analysts,
        graph=graph,
        final_state=final_state,
    )


def _build_deep_result_payload(
    symbol: str,
    trade_date: str,
    query: str,
    selected_analysts: list[str],
    graph,
    final_state: dict[str, Any],
) -> tuple[dict, list[dict], list[dict], str]:
    final_trade_decision = str(final_state.get("final_trade_decision") or "")
    trader_investment_plan = str(final_state.get("trader_investment_plan") or "")
    investment_plan = str(final_state.get("investment_plan") or "")
    analyst_traces = list(final_state.get("analyst_traces") or [])
    decision = _resolve_deep_decision(
        graph=graph,
        final_trade_decision=final_trade_decision,
        trader_investment_plan=trader_investment_plan,
        investment_plan=investment_plan,
        analyst_traces=analyst_traces,
    )
    direction = _derive_direction(final_trade_decision, decision, analyst_traces)
    resolved_fields = report_service.resolve_report_fields(final_state)

    result_payload = {
        "symbol": symbol,
        "trade_date": trade_date,
        "query": query,
        "decision": decision,
        "direction": direction,
        "analysis_mode": "deep",
        "selected_analysts": selected_analysts,
        "market_report": final_state.get("market_report") or "",
        "sentiment_report": final_state.get("sentiment_report") or "",
        "news_report": final_state.get("news_report") or "",
        "fundamentals_report": final_state.get("fundamentals_report") or "",
        "macro_report": final_state.get("macro_report") or "",
        "smart_money_report": final_state.get("smart_money_report") or "",
        "volume_price_report": final_state.get("volume_price_report") or "",
        "investment_plan": investment_plan,
        "trader_investment_plan": trader_investment_plan,
        "final_trade_decision": final_trade_decision,
        "analyst_traces": analyst_traces,
        "investment_debate_state": final_state.get("investment_debate_state") or {},
        "risk_debate_state": final_state.get("risk_debate_state") or {},
        "risk_feedback_state": final_state.get("risk_feedback_state") or {},
        "instrument_context": final_state.get("instrument_context") or {},
        "market_context": final_state.get("market_context") or {},
        "workflow_context": final_state.get("workflow_context") or {},
        "user_context": final_state.get("user_context") or {},
        "target_price": resolved_fields.get("target_price"),
        "stop_loss_price": resolved_fields.get("stop_loss_price"),
    }
    risk_items = _build_deep_risk_items(final_state, selected_analysts)
    key_metrics = _build_deep_key_metrics(final_state, decision, direction, selected_analysts)
    return result_payload, risk_items, key_metrics, decision


def _build_deep_risk_items(final_state: dict[str, Any], selected_analysts: list[str]) -> list[dict[str, Any]]:
    investment_debate = final_state.get("investment_debate_state") or {}
    risk_debate = final_state.get("risk_debate_state") or {}
    risk_feedback = final_state.get("risk_feedback_state") or {}
    unresolved_investment = len(investment_debate.get("unresolved_claim_ids") or [])
    unresolved_risk = len(risk_debate.get("unresolved_claim_ids") or [])
    items = [
        {
            "name": "多Agent链路",
            "level": "low",
            "description": f"本次启用 {len(selected_analysts)} 个分析师与完整研究/风控链路。",
        }
    ]
    if unresolved_investment:
        items.append(
            {
                "name": "研究分歧",
                "level": "medium",
                "description": f"研究辩论仍有 {unresolved_investment} 个未完全收敛观点，建议关注结论中的条件前提。",
            }
        )
    if unresolved_risk or risk_feedback.get("revision_required"):
        items.append(
            {
                "name": "风控约束",
                "level": "medium",
                "description": f"风险讨论存在 {unresolved_risk} 个未完全收敛风险点，请结合止损和仓位建议执行。",
            }
        )
    return items


def _build_deep_key_metrics(
    final_state: dict[str, Any],
    decision: str,
    direction: str,
    selected_analysts: list[str],
) -> list[dict[str, Any]]:
    confidence = _extract_confidence(final_state.get("final_trade_decision"))
    traces = list(final_state.get("analyst_traces") or [])
    return [
        {
            "name": "执行动作",
            "value": decision,
            "status": "neutral" if decision == "HOLD" else ("good" if decision == "BUY" else "bad"),
        },
        {
            "name": "方向倾向",
            "value": direction,
            "status": "neutral" if direction == "中性" else ("good" if direction == "偏多" else "bad"),
        },
        {
            "name": "分析师数量",
            "value": str(len(selected_analysts)),
            "status": "good" if len(selected_analysts) >= 4 else "neutral",
        },
        {
            "name": "观点追踪数",
            "value": str(len(traces)),
            "status": "neutral",
        },
        {
            "name": "置信度",
            "value": f"{confidence}%" if confidence is not None else "未提取",
            "status": "good" if (confidence or 0) >= 70 else ("neutral" if confidence is None or confidence >= 50 else "bad"),
        },
    ]


def _finalize_section_events(
    tracker: AgentProgressTracker,
    result_payload: dict[str, Any],
) -> list[tuple[str, dict[str, Any]]]:
    events: list[tuple[str, dict[str, Any]]] = []
    for agent_name, section in _section_definitions():
        content = str(result_payload.get(section) or "").strip()
        events.extend(tracker.finalize_report(agent_name, section, content))
    return events


def _section_definitions() -> list[tuple[str, str]]:
    return [
        ("Market Analyst", "market_report"),
        ("Social Analyst", "sentiment_report"),
        ("News Analyst", "news_report"),
        ("Fundamentals Analyst", "fundamentals_report"),
        ("Macro Analyst", "macro_report"),
        ("Smart Money Analyst", "smart_money_report"),
        ("Volume Price Analyst", "volume_price_report"),
        ("Research Manager", "investment_plan"),
        ("Trader", "trader_investment_plan"),
        ("Portfolio Manager", "final_trade_decision"),
    ]


def _resolve_selected_analysts(requested: Any, user_id: str) -> list[str]:
    requested_list = [str(item).strip() for item in (requested or []) if str(item).strip()]
    if requested_list:
        return requested_list
    try:
        import json as _json
        from api.services import auth_service

        with get_db_ctx() as db:
            user_cfg = auth_service.get_user_llm_config(db, user_id)
        parsed = _json.loads(user_cfg.default_analysts) if user_cfg and user_cfg.default_analysts else []
        resolved = [str(item).strip() for item in parsed if str(item).strip()]
        if resolved:
            return resolved
    except Exception:
        logger.exception("Failed to resolve default analysts for %s", user_id)
    return ["market", "social", "news", "fundamentals", "macro", "smart_money", "volume_price"]


def _merge_user_contexts(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base or {})
    for key, value in (overlay or {}).items():
        if value in (None, "", []):
            continue
        merged[key] = value
    return merged


_VALID_TRADE_DECISIONS = {"BUY", "SELL", "HOLD"}


def _normalize_trade_decision(value: Any) -> str | None:
    decision = str(value or "").strip().upper()
    return decision if decision in _VALID_TRADE_DECISIONS else None


def _has_explicit_decision_marker(text_value: str) -> bool:
    text_value = str(text_value or "")
    return any(
        marker in text_value
        for marker in (
            "FINAL TRANSACTION PROPOSAL",
            "最终交易建议",
            "建议动作",
            "执行动作",
            "最终裁决",
            "最终建议",
            "<!-- VERDICT:",
        )
    )


def _resolve_deep_decision(
    *,
    graph,
    final_trade_decision: str,
    trader_investment_plan: str,
    investment_plan: str,
    analyst_traces: list[dict[str, Any]],
) -> str:
    from tradingagents.graph.signal_processing import _extract_decision_keyword

    candidates: list[str] = []
    if _has_explicit_decision_marker(final_trade_decision):
        candidates.append(final_trade_decision)
    candidates.extend([trader_investment_plan, investment_plan])
    if final_trade_decision not in candidates:
        candidates.append(final_trade_decision)

    for candidate in candidates:
        decision = _normalize_trade_decision(_extract_decision_keyword(candidate))
        if decision:
            return _align_decision_with_trace_consensus(decision, analyst_traces)

    for candidate in (trader_investment_plan, investment_plan, final_trade_decision):
        try:
            decision = _normalize_trade_decision(graph.process_signal(candidate))
        except Exception:
            logger.exception("Failed to extract trade decision from candidate report")
            decision = None
        if decision:
            return _align_decision_with_trace_consensus(decision, analyst_traces)

    consensus_decision, _ = _trace_consensus_decision(analyst_traces)
    return consensus_decision or "HOLD"


def _align_decision_with_trace_consensus(decision: str, analyst_traces: list[dict[str, Any]]) -> str:
    consensus_decision, confidence = _trace_consensus_decision(analyst_traces)
    if not consensus_decision or consensus_decision == "HOLD" or confidence < 0.7:
        return decision
    if decision == "BUY" and consensus_decision == "SELL":
        logger.warning("Final BUY conflicts with bearish analyst consensus; using SELL")
        return "SELL"
    if decision == "SELL" and consensus_decision == "BUY":
        logger.warning("Final SELL conflicts with bullish analyst consensus; using BUY")
        return "BUY"
    return decision


def _trace_consensus_decision(analyst_traces: list[dict[str, Any]]) -> tuple[str | None, float]:
    bullish = 0
    bearish = 0
    neutral = 0
    for trace in analyst_traces:
        verdict = str(trace.get("verdict") or "").upper()
        if any(keyword in verdict for keyword in ("BULL", "看多", "偏多", "BUY")):
            bullish += 1
        elif any(keyword in verdict for keyword in ("BEAR", "看空", "偏空", "SELL")):
            bearish += 1
        elif any(keyword in verdict for keyword in ("NEUTRAL", "中性", "HOLD", "观望")):
            neutral += 1
    directional_total = bullish + bearish
    if directional_total == 0:
        return None, 0.0
    confidence = max(bullish, bearish) / max(directional_total + neutral, directional_total)
    if bullish > bearish:
        return "BUY", confidence
    if bearish > bullish:
        return "SELL", confidence
    return "HOLD", confidence


def _extract_verdict_direction_label(text_value: str) -> str | None:
    marker = "<!-- VERDICT:"
    if marker not in str(text_value or ""):
        return None
    try:
        start = text_value.index(marker) + len(marker)
        end = text_value.index("-->", start)
        payload = json.loads(text_value[start:end].strip())
    except Exception:
        return None
    direction = str(payload.get("direction") or "").strip().upper()
    if direction in {"看多", "偏多", "BULLISH", "LEAN_BULLISH", "BUY"}:
        return "偏多"
    if direction in {"看空", "偏空", "BEARISH", "LEAN_BEARISH", "SELL"}:
        return "偏空"
    if direction in {"中性", "NEUTRAL", "HOLD"}:
        return "中性"
    return None


def _derive_direction(final_trade_decision: str, decision: str, analyst_traces: list[dict[str, Any]]) -> str:
    explicit_direction = _extract_verdict_direction_label(final_trade_decision)
    if explicit_direction:
        return explicit_direction
    normalized_decision = _normalize_trade_decision(decision)
    if normalized_decision == "BUY":
        return "偏多"
    if normalized_decision == "SELL":
        return "偏空"
    consensus_decision, confidence = _trace_consensus_decision(analyst_traces)
    if consensus_decision == "BUY" and confidence >= 0.5:
        return "偏多"
    if consensus_decision == "SELL" and confidence >= 0.5:
        return "偏空"
    return "中性"


def _build_user_context_snapshot(symbol: str, user_id: str) -> dict:
    try:
        with get_db_ctx() as db:
            return compat_portfolio_context(db, user_id, symbol)
    except Exception:
        logger.exception("Failed to build user context snapshot for %s", symbol)
        return {}


def _load_recent_daily_rows(code: str, symbol: str) -> list[dict]:
    try:
        with get_db_ctx() as db:
            table_name = preferred_daily_kline_table()
            columns = _get_table_columns(db, table_name)
            required_columns = {"symbol", "trade_date", "close"}
            if not required_columns.issubset(columns):
                logger.warning("%s is unavailable or missing required columns: %s", table_name, sorted(required_columns - columns))
                return []

            selected_columns = [
                column
                for column in (
                    "trade_date",
                    "close",
                    "volume",
                    "amount",
                    "turnover_rate",
                    "float_market_cap",
                    "net_profit_ttm",
                )
                if column in columns
            ]
            rows = db.execute(
                text(
                    f"""
                    SELECT {", ".join(selected_columns)}
                    FROM {table_name}
                    WHERE symbol IN (:code, :symbol)
                    ORDER BY trade_date DESC
                    LIMIT 30
                    """
                ),
                {"code": code, "symbol": symbol},
            ).mappings().all()
            return [dict(row) for row in rows]
    except Exception:
        logger.exception("Failed to load recent daily kline for %s", symbol)
        return []


def _get_table_columns(db, table_name: str) -> set[str]:
    try:
        rows = db.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table_name
                """
            ),
            {"table_name": table_name},
        ).fetchall()
        return {row[0] for row in rows}
    except Exception:
        logger.exception("Failed to inspect table columns for %s", table_name)
        return set()


def compat_portfolio_context(db, user_id: str, symbol: str) -> dict:
    from api import main as compat

    return compat._build_manual_imported_user_context(db, user_id, symbol)


def _safe_float(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _extract_confidence(text: str | None) -> int | None:
    if not text:
        return None
    for line in str(text).splitlines():
        if "置信度" in line:
            digits = "".join(char for char in line if char.isdigit())
            if digits:
                return int(digits)
    return None


def _sse_pack(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
