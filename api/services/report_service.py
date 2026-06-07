from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

from sqlalchemy.orm import Session

from api.database import ReportDB
from api.core.utils import safe_float as _safe_float

ACTIVE_REPORT_STATUSES = ("pending", "running")
STALE_REPORT_ERROR_MESSAGE = "分析任务已中断，请重新发起分析"


def _extract_price_regex(text: Optional[str], price_type: str) -> Optional[float]:
    if not text:
        return None
    normalized = re.sub(r"[*_`#>\[\]（）()]", "", str(text))
    if price_type == "target":
        patterns = (
            r"目标(?:价|价格|位|价位)?(?:区间)?\s*[:：]?\s*[¥$]?\s*(\d+(?:\.\d+)?)",
            r"(?:target|target\s*price)\s*[:：]?\s*[¥$]?\s*(\d+(?:\.\d+)?)",
        )
    else:
        patterns = (
            r"止损(?:价|价格|位|价位)?\s*[:：]?\s*[¥$]?\s*(\d+(?:\.\d+)?)",
            r"(?:stop[-\s_]?loss|stop\s*price)\s*[:：]?\s*[¥$]?\s*(\d+(?:\.\d+)?)",
        )
    for pattern in patterns:
        match = re.search(pattern, normalized, re.IGNORECASE)
        if match:
            return float(match.group(1))
    return None


def _extract_confidence_regex(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    match = re.search(r"(?:置信度|confidence)[:：]\s*(\d+)%?", text, re.IGNORECASE)
    if not match:
        return None
    value = int(match.group(1))
    return value if 0 <= value <= 100 else None


def resolve_report_fields(
    result_data: Optional[Dict[str, Any]] = None,
    confidence_override: Optional[int] = None,
    target_price_override: Optional[float] = None,
    stop_loss_override: Optional[float] = None,
) -> Dict[str, Any]:
    result_data = result_data or {}
    final_trade_decision = result_data.get("final_trade_decision")
    trader_investment_plan = result_data.get("trader_investment_plan")
    confidence = confidence_override if confidence_override is not None else _extract_confidence_regex(final_trade_decision)
    target_price = target_price_override if target_price_override is not None else _safe_float(result_data.get("target_price"))
    if target_price is None:
        target_price = _extract_price_regex(final_trade_decision, "target") or _extract_price_regex(trader_investment_plan, "target")
    stop_loss_price = stop_loss_override if stop_loss_override is not None else _safe_float(result_data.get("stop_loss_price"))
    if stop_loss_price is None:
        stop_loss_price = _extract_price_regex(final_trade_decision, "stop_loss") or _extract_price_regex(trader_investment_plan, "stop_loss")
    return {
        "market_report": result_data.get("market_report"),
        "sentiment_report": result_data.get("sentiment_report"),
        "news_report": result_data.get("news_report"),
        "fundamentals_report": result_data.get("fundamentals_report"),
        "macro_report": result_data.get("macro_report"),
        "smart_money_report": result_data.get("smart_money_report"),
        "volume_price_report": result_data.get("volume_price_report"),
        "game_theory_report": result_data.get("game_theory_report"),
        "investment_plan": result_data.get("investment_plan"),
        "trader_investment_plan": trader_investment_plan,
        "final_trade_decision": final_trade_decision,
        "direction": None,
        "confidence": confidence,
        "target_price": target_price,
        "stop_loss_price": stop_loss_price,
    }




def init_report(db: Session, report_id: str, symbol: str, trade_date: str, user_id: Optional[str] = None) -> ReportDB:
    now = datetime.now(timezone.utc)
    report = ReportDB(
        id=report_id,
        user_id=user_id,
        symbol=symbol,
        trade_date=trade_date,
        status="pending",
        created_at=now,
        updated_at=now,
    )
    db.add(report)
    db.commit()
    db.refresh(report)
    return report


def update_report_partial(db: Session, report_id: str, status: Optional[str] = None, **fields: Any) -> Optional[ReportDB]:
    report = db.query(ReportDB).filter(ReportDB.id == report_id).first()
    if not report:
        return None
    if status:
        report.status = status
    for key, value in fields.items():
        if hasattr(report, key):
            setattr(report, key, value)
    report.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(report)
    return report


def finalize_orphan_report(
    db: Session,
    report: ReportDB,
    *,
    error_message: str = STALE_REPORT_ERROR_MESSAGE,
) -> ReportDB:
    if str(report.status or "") not in ACTIVE_REPORT_STATUSES:
        return report
    report.status = "failed"
    report.error = error_message
    report.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(report)
    return report


def recover_stale_active_reports(
    db: Session,
    *,
    active_job_ids: Optional[Iterable[str]] = None,
    error_message: str = STALE_REPORT_ERROR_MESSAGE,
) -> Dict[str, int]:
    active_job_id_set = {str(job_id) for job_id in (active_job_ids or []) if str(job_id).strip()}
    rows = db.query(ReportDB).filter(ReportDB.status.in_(ACTIVE_REPORT_STATUSES)).all()
    failed = 0
    for row in rows:
        if str(row.id) in active_job_id_set:
            continue
        row.status = "failed"
        row.error = error_message
        row.updated_at = datetime.now(timezone.utc)
        failed += 1
    if failed:
        db.commit()
    return {"total": failed, "completed": 0, "failed": failed}


def create_report(
    db: Session,
    symbol: str,
    trade_date: str,
    decision: Optional[str] = None,
    result_data: Optional[Dict[str, Any]] = None,
    user_id: Optional[str] = None,
    risk_items: Optional[List[dict]] = None,
    key_metrics: Optional[List[dict]] = None,
    analyst_traces: Optional[List[dict]] = None,
    confidence_override: Optional[int] = None,
    target_price_override: Optional[float] = None,
    stop_loss_override: Optional[float] = None,
    report_id: Optional[str] = None,
) -> ReportDB:
    resolved = resolve_report_fields(
        result_data=result_data,
        confidence_override=confidence_override,
        target_price_override=target_price_override,
        stop_loss_override=stop_loss_override,
    )
    now = datetime.now(timezone.utc)
    report = db.query(ReportDB).filter(ReportDB.id == report_id).first() if report_id else None
    if not report:
        report = ReportDB(
            id=report_id or str(uuid4()),
            user_id=user_id,
            symbol=symbol,
            trade_date=trade_date,
            created_at=now,
        )
        db.add(report)
    report.status = "completed"
    report.decision = decision
    report.direction = resolved["direction"]
    report.confidence = resolved["confidence"]
    report.target_price = resolved["target_price"]
    report.stop_loss_price = resolved["stop_loss_price"]
    report.result_data = result_data
    report.risk_items = risk_items
    report.key_metrics = key_metrics
    report.analyst_traces = analyst_traces
    for key in (
        "market_report",
        "sentiment_report",
        "news_report",
        "fundamentals_report",
        "macro_report",
        "smart_money_report",
        "volume_price_report",
        "game_theory_report",
        "investment_plan",
        "trader_investment_plan",
        "final_trade_decision",
    ):
        setattr(report, key, resolved[key])
    report.updated_at = now
    db.commit()
    db.refresh(report)
    return report


def get_report(db: Session, report_id: str, user_id: Optional[str] = None) -> Optional[ReportDB]:
    query = db.query(ReportDB).filter(ReportDB.id == report_id)
    if user_id:
        query = query.filter(ReportDB.user_id == user_id)
    return query.first()


def get_reports_by_user(
    db: Session,
    user_id: Optional[str] = None,
    symbol: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
) -> List[ReportDB]:
    query = db.query(ReportDB)
    if user_id:
        query = query.filter(ReportDB.user_id == user_id)
    if symbol:
        query = query.filter(ReportDB.symbol == symbol)
    return query.order_by(ReportDB.created_at.desc()).offset(skip).limit(limit).all()


def count_reports_by_user(
    db: Session,
    user_id: Optional[str] = None,
    symbol: Optional[str] = None,
) -> int:
    query = db.query(ReportDB)
    if user_id:
        query = query.filter(ReportDB.user_id == user_id)
    if symbol:
        query = query.filter(ReportDB.symbol == symbol)
    return int(query.count())


def get_latest_reports_by_symbols(
    db: Session,
    symbols: List[str],
    user_id: Optional[str] = None,
) -> List[ReportDB]:
    normalized_symbols = [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]
    if not normalized_symbols:
        return []
    query = db.query(ReportDB).filter(ReportDB.symbol.in_(normalized_symbols))
    if user_id:
        query = query.filter(ReportDB.user_id == user_id)
    rows = query.order_by(ReportDB.symbol.asc(), ReportDB.created_at.desc()).all()
    latest_by_symbol: dict[str, ReportDB] = {}
    for row in rows:
        symbol = str(row.symbol or "").upper()
        if symbol and symbol not in latest_by_symbol:
            latest_by_symbol[symbol] = row
    return [latest_by_symbol[symbol] for symbol in normalized_symbols if symbol in latest_by_symbol]


def delete_report(db: Session, report_id: str, user_id: Optional[str] = None) -> bool:
    report = get_report(db, report_id, user_id=user_id)
    if not report:
        return False
    db.delete(report)
    db.commit()
    return True


def batch_delete_reports(db: Session, report_ids: Iterable[str], user_id: Optional[str] = None) -> dict:
    normalized_ids: list[str] = []
    seen: set[str] = set()
    for raw_report_id in report_ids:
        report_id = str(raw_report_id or "").strip()
        if not report_id or report_id in seen:
            continue
        seen.add(report_id)
        normalized_ids.append(report_id)
    if not normalized_ids:
        raise ValueError("请至少选择 1 份报告")

    query = db.query(ReportDB).filter(ReportDB.id.in_(normalized_ids))
    if user_id:
        query = query.filter(ReportDB.user_id == user_id)
    rows = query.all()
    row_by_id = {str(row.id): row for row in rows}
    deleted_ids: list[str] = []
    missing_ids: list[str] = []
    for report_id in normalized_ids:
        row = row_by_id.get(report_id)
        if row is None:
            missing_ids.append(report_id)
            continue
        db.delete(row)
        deleted_ids.append(report_id)
    if deleted_ids:
        db.commit()
    return {"deleted_ids": deleted_ids, "missing_ids": missing_ids}
