from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import re
from typing import Any

from sqlalchemy.orm import Session

from api.models.strategy_models import (
    BacktestJobDB,
    EvolutionCandidateDB,
    EvolutionExperimentDB,
    PaperOrderDB,
    RealtimeApprovalDB,
    RealtimeEventDB,
    RealtimeMonitorDB,
    RealtimeSignalExecutionDB,
    BacktestResultDB,
    StrategyDB,
    StrategyStatus,
    StrategyType,
    TradeRecordDB,
)


PLATFORM_META_KEY = "strategy_platform"
_REALTIME_TEST_STRATEGY_NAME_RE = re.compile(
    r"^(实时测试策略|实盘监控策略|审批策略|单轮执行策略|同K线去重策略|同K线延迟信号策略|"
    r"不可卖卖出策略|非交易时段策略|禁用缓存快照策略|撤单补单策略|首日波段回补策略)-[0-9a-f]{6}$"
)
_KNOWN_TEST_STRATEGY_NAMES = {
    "模板策略保存测试",
    "克隆策略测试",
    "仓储测试策略",
    "进化仓储测试策略",
}


def list_platform_strategies(
    db: Session,
    *,
    strategy_type: str | None = None,
    status: str | None = None,
    search: str | None = None,
    include_test: bool = False,
) -> list[dict[str, Any]]:
    rows = db.query(StrategyDB).order_by(StrategyDB.updated_at.desc()).all()
    items = [_strategy_to_payload(row) for row in rows if _is_platform_strategy(row)]
    if not include_test:
        items = [item for item in items if not _is_test_strategy(item)]
    if strategy_type:
        items = [item for item in items if item["strategy_type"] == strategy_type]
    if status:
        items = [item for item in items if item["status"] == status]
    if search:
        lowered = search.lower()
        items = [
            item for item in items
            if lowered in item["name"].lower() or lowered in (item.get("description") or "").lower()
        ]
    return items


def get_platform_strategy(db: Session, strategy_id: str) -> dict[str, Any] | None:
    row = db.query(StrategyDB).filter(StrategyDB.id == strategy_id).first()
    if not row or not _is_platform_strategy(row):
        return None
    return _strategy_to_payload(row)


def get_platform_strategy_versions(db: Session, strategy_id: str) -> list[dict[str, Any]]:
    row = db.query(StrategyDB).filter(StrategyDB.id == strategy_id).first()
    if not row or not _is_platform_strategy(row):
        return []
    meta = deepcopy((row.parameters or {}).get(PLATFORM_META_KEY) or {})
    versions = list(meta.get("versions") or [])
    current_version = meta.get("current_version")
    if current_version and not any(item.get("id") == current_version.get("id") for item in versions):
        versions.append(current_version)
    return sorted(versions, key=lambda item: int(item.get("version") or 0), reverse=True)


def save_platform_strategy(db: Session, payload: dict[str, Any]) -> dict[str, Any]:
    strategy_id = str(payload.get("id") or "")
    row = db.query(StrategyDB).filter(StrategyDB.id == strategy_id).first() if strategy_id else None
    if row is None:
        row = StrategyDB()
        if strategy_id:
            row.id = strategy_id
        db.add(row)

    performance = payload.get("performance") or {}
    row.name = payload["name"]
    row.strategy_type = _to_strategy_type(payload["strategy_type"])
    row.description = payload.get("description")
    row.status = _to_strategy_status(payload.get("status"))
    row.is_active = bool(payload.get("is_active")) or payload.get("status") == "active"
    row.version = int(payload.get("version") or 1)
    row.run_count = int(payload.get("run_count") or 0)
    row.last_run_time = _parse_datetime(payload.get("last_run_time"))
    row.total_return = performance.get("total_return")
    row.sharpe_ratio = performance.get("sharpe_ratio")
    row.max_drawdown = performance.get("max_drawdown")
    row.win_rate = performance.get("win_rate")

    params = deepcopy(row.parameters or {})
    existing_meta = deepcopy(params.get(PLATFORM_META_KEY) or {})
    current_version = deepcopy(payload.get("current_version"))
    versions = list(deepcopy(payload.get("versions") or existing_meta.get("versions") or []))
    if current_version:
        versions = [item for item in versions if item.get("id") != current_version.get("id")]
        versions.append(current_version)
        versions = sorted(versions, key=lambda item: int(item.get("version") or 0))

    params[PLATFORM_META_KEY] = {
        "source": payload.get("source") or "manual",
        "status": payload.get("status") or "draft",
        "current_version_id": payload.get("current_version_id"),
        "current_version": current_version,
        "versions": versions,
        "tags": list(payload.get("tags") or []),
        "template": {
            "id": payload.get("template_id"),
            "name": payload.get("template_name"),
            "parameters": deepcopy(payload.get("template_parameters") or {}),
        },
        "performance_extras": {
            "annual_return": performance.get("annual_return"),
            "calmar_ratio": performance.get("calmar_ratio"),
        },
    }
    row.parameters = params

    db.commit()
    db.refresh(row)
    return _strategy_to_payload(row)


def delete_platform_strategy(db: Session, strategy_id: str) -> bool:
    row = db.query(StrategyDB).filter(StrategyDB.id == strategy_id).first()
    if row is None or not _is_platform_strategy(row):
        return False

    backtest_ids = [
        item.id
        for item in db.query(BacktestJobDB.id).filter(BacktestJobDB.strategy_id == strategy_id).all()
    ]
    if backtest_ids:
        db.query(BacktestResultDB).filter(BacktestResultDB.job_id.in_(backtest_ids)).delete(synchronize_session=False)
    db.query(PaperOrderDB).filter(PaperOrderDB.strategy_id == strategy_id).delete()
    db.query(TradeRecordDB).filter(TradeRecordDB.strategy_id == strategy_id).delete()
    db.query(BacktestJobDB).filter(BacktestJobDB.strategy_id == strategy_id).delete()
    db.query(RealtimeApprovalDB).filter(RealtimeApprovalDB.strategy_id == strategy_id).delete()
    db.query(RealtimeSignalExecutionDB).filter(RealtimeSignalExecutionDB.strategy_id == strategy_id).delete()
    db.query(RealtimeEventDB).filter(RealtimeEventDB.strategy_id == strategy_id).delete()
    db.query(RealtimeMonitorDB).filter(RealtimeMonitorDB.strategy_id == strategy_id).delete()

    experiments = (
        db.query(EvolutionExperimentDB)
        .filter(EvolutionExperimentDB.strategy_id == strategy_id)
        .all()
    )
    for experiment in experiments:
        db.query(EvolutionCandidateDB).filter(EvolutionCandidateDB.experiment_id == experiment.id).delete()
        db.delete(experiment)

    db.query(StrategyDB).filter(StrategyDB.parent_id == strategy_id).update({"parent_id": None})
    db.delete(row)
    db.commit()
    return True


def update_platform_strategy_metrics(db: Session, strategy_id: str, metrics: dict[str, Any]) -> dict[str, Any] | None:
    row = db.query(StrategyDB).filter(StrategyDB.id == strategy_id).first()
    if not row or not _is_platform_strategy(row):
        return None

    params = deepcopy(row.parameters or {})
    meta = deepcopy(params.get(PLATFORM_META_KEY) or {})
    performance_extras = deepcopy(meta.get("performance_extras") or {})

    row.total_return = metrics.get("total_return")
    row.sharpe_ratio = metrics.get("sharpe_ratio")
    row.max_drawdown = metrics.get("max_drawdown")
    row.win_rate = metrics.get("win_rate")
    row.run_count = int(row.run_count or 0) + 1
    row.last_run_time = _now_dt()
    performance_extras["annual_return"] = metrics.get("annual_return")
    performance_extras["calmar_ratio"] = metrics.get("calmar_ratio")
    meta["performance_extras"] = performance_extras
    params[PLATFORM_META_KEY] = meta
    row.parameters = params

    db.commit()
    db.refresh(row)
    return _strategy_to_payload(row)


def save_platform_backtest_run(db: Session, payload: dict[str, Any]) -> dict[str, Any]:
    row = db.query(BacktestJobDB).filter(BacktestJobDB.id == payload["id"]).first()
    if row is None:
        row = BacktestJobDB(id=payload["id"], strategy_id=payload["strategy_id"])
        db.add(row)

    result_payload = deepcopy(payload.get("result") or {})
    result_payload.setdefault("_strategy_platform", {})
    result_payload["_strategy_platform"].update(
        {
            "enabled": True,
            "frequency": payload["frequency"],
            "backtest_mode": payload.get("backtest_mode") or (payload.get("result") or {}).get("_strategy_platform", {}).get("backtest_mode"),
            "request_config": (payload.get("result") or {}).get("_strategy_platform", {}).get("request_config")
            or (payload.get("result") or {}).get("summary", {}).get("request_config")
            or {},
            "universe": payload.get("universe") or {},
            "cost_config": payload.get("cost_config") or {},
            "minute_config": payload.get("minute_config") or {},
            "walk_forward": payload.get("walk_forward") or {},
            "strategy_version_id": payload.get("strategy_version_id"),
            "artifact_root": payload.get("artifact_root"),
        }
    )

    row.strategy_id = payload["strategy_id"]
    row.backtest_mode = payload["frequency"]
    row.start_date = _parse_datetime(payload["start_date"])
    row.end_date = _parse_datetime(payload["end_date"])
    row.initial_capital = float(payload["initial_capital"])
    row.benchmark = payload.get("benchmark") or "沪深300"
    row.status = payload["status"]
    row.progress = float(payload.get("progress") or 0.0)
    row.error_message = payload.get("error_message")
    row.result = result_payload
    row.created_at = _parse_datetime(payload.get("created_at")) or _now_dt()
    row.started_at = _parse_datetime(payload.get("started_at"))
    row.completed_at = _parse_datetime(payload.get("completed_at"))

    db.commit()
    db.refresh(row)
    return _backtest_to_payload(row)


def get_platform_backtest_run(db: Session, run_id: str) -> dict[str, Any] | None:
    row = db.query(BacktestJobDB).filter(BacktestJobDB.id == run_id).first()
    if not row or not _is_platform_backtest(row):
        return None
    return _backtest_to_payload(row)


def list_platform_backtest_runs(
    db: Session,
    *,
    strategy_id: str | None = None,
    limit: int = 8,
) -> list[dict[str, Any]]:
    query = db.query(BacktestJobDB)
    if strategy_id:
        query = query.filter(BacktestJobDB.strategy_id == strategy_id)
    rows = (
        query
        .order_by(BacktestJobDB.created_at.desc(), BacktestJobDB.completed_at.desc())
        .limit(limit)
        .all()
    )
    return [_backtest_to_payload(row) for row in rows if _is_platform_backtest(row)]


def get_latest_completed_platform_backtest(db: Session, strategy_id: str) -> dict[str, Any] | None:
    rows = (
        db.query(BacktestJobDB)
        .filter(BacktestJobDB.strategy_id == strategy_id, BacktestJobDB.status == "completed")
        .order_by(BacktestJobDB.completed_at.desc(), BacktestJobDB.created_at.desc())
        .all()
    )
    for row in rows:
        if _is_platform_backtest(row):
            return _backtest_to_payload(row)
    return None


def platform_strategy_count(db: Session) -> int:
    return sum(1 for row in db.query(StrategyDB).all() if _is_platform_strategy(row))


def save_platform_evolution_experiment(db: Session, payload: dict[str, Any]) -> dict[str, Any]:
    row = db.query(EvolutionExperimentDB).filter(EvolutionExperimentDB.id == payload["id"]).first()
    if row is None:
        row = EvolutionExperimentDB(id=payload["id"])
        db.add(row)

    row.strategy_id = payload["strategy_id"]
    row.objective = payload["objective"]
    row.status = payload.get("status") or "completed"
    row.progress = float(payload.get("progress") or 0.0)
    row.search_space = deepcopy(payload.get("search_space") or {})
    row.base_backtest_run_id = payload.get("base_backtest_run_id")
    row.created_at = _parse_datetime(payload.get("created_at")) or _now_dt()
    row.updated_at = _now_dt()

    existing_candidates = {item.id: item for item in row.candidates}
    next_candidate_ids: set[str] = set()
    for candidate_payload in payload.get("candidates") or []:
        candidate_id = _normalize_candidate_id(candidate_payload["id"], payload["id"], candidate_payload.get("name"))
        next_candidate_ids.add(candidate_id)
        candidate_row = existing_candidates.get(candidate_id)
        if candidate_row is None:
            candidate_row = EvolutionCandidateDB(id=candidate_id, experiment=row)
            db.add(candidate_row)
        candidate_row.name = candidate_payload["name"]
        candidate_row.score = float(candidate_payload.get("score") or 0.0)
        candidate_row.status = candidate_payload.get("status") or "candidate"
        candidate_row.improvement_summary = candidate_payload.get("improvement_summary")
        candidate_row.risk_flags = list(candidate_payload.get("risk_flags") or [])
        candidate_row.metrics = deepcopy(candidate_payload.get("metrics") or {})
        candidate_row.dsl_patch = deepcopy(candidate_payload.get("dsl_patch") or {})
        candidate_row.updated_at = _now_dt()
        if candidate_row.created_at is None:
            candidate_row.created_at = _now_dt()
        if candidate_row.status == "accepted" and candidate_row.accepted_at is None:
            candidate_row.accepted_at = _now_dt()

    for candidate_id, candidate_row in existing_candidates.items():
        if candidate_id not in next_candidate_ids:
            db.delete(candidate_row)

    db.commit()
    db.refresh(row)
    return _experiment_to_payload(row)


def get_platform_evolution_experiment(db: Session, experiment_id: str) -> dict[str, Any] | None:
    row = db.query(EvolutionExperimentDB).filter(EvolutionExperimentDB.id == experiment_id).first()
    if row is None:
        return None
    return _experiment_to_payload(row)


def list_platform_evolution_candidates(db: Session, experiment_id: str) -> list[dict[str, Any]]:
    row = db.query(EvolutionExperimentDB).filter(EvolutionExperimentDB.id == experiment_id).first()
    if row is None:
        return []
    return [_candidate_to_payload(item) for item in row.candidates]


def get_platform_evolution_candidate(db: Session, candidate_id: str) -> dict[str, Any] | None:
    row = db.query(EvolutionCandidateDB).filter(EvolutionCandidateDB.id == candidate_id).first()
    if row is None:
        return None
    return _candidate_to_payload(row)


def update_platform_evolution_candidate_status(
    db: Session,
    candidate_id: str,
    *,
    status: str,
) -> dict[str, Any] | None:
    row = db.query(EvolutionCandidateDB).filter(EvolutionCandidateDB.id == candidate_id).first()
    if row is None:
        return None
    row.status = status
    row.updated_at = _now_dt()
    if status == "accepted":
        row.accepted_at = _now_dt()
    db.commit()
    db.refresh(row)
    return _candidate_to_payload(row)


def _strategy_to_payload(row: StrategyDB) -> dict[str, Any]:
    params = deepcopy(row.parameters or {})
    meta = deepcopy(params.get(PLATFORM_META_KEY) or {})
    performance_extras = deepcopy(meta.get("performance_extras") or {})
    performance = None
    if row.total_return is not None:
        performance = {
            "total_return": row.total_return,
            "annual_return": performance_extras.get("annual_return"),
            "sharpe_ratio": row.sharpe_ratio,
            "max_drawdown": row.max_drawdown,
            "win_rate": row.win_rate,
            "calmar_ratio": performance_extras.get("calmar_ratio"),
        }
    return {
        "id": row.id,
        "name": row.name,
        "strategy_type": row.strategy_type.value if row.strategy_type else "portfolio",
        "status": meta.get("status") or (row.status.value if row.status else "draft"),
        "description": row.description,
        "source": meta.get("source") or "manual",
        "current_version_id": meta.get("current_version_id"),
        "version": int(row.version or 1),
        "is_active": bool(row.is_active),
        "run_count": int(row.run_count or 0),
        "last_run_time": _iso_datetime(row.last_run_time),
        "created_at": _iso_datetime(row.created_at),
        "updated_at": _iso_datetime(row.updated_at),
        "performance": performance,
        "current_version": meta.get("current_version"),
        "tags": list(meta.get("tags") or []),
        "template_id": (meta.get("template") or {}).get("id"),
        "template_name": (meta.get("template") or {}).get("name"),
        "template_parameters": deepcopy((meta.get("template") or {}).get("parameters") or {}),
    }


def _backtest_to_payload(row: BacktestJobDB) -> dict[str, Any]:
    result = deepcopy(row.result or {})
    platform_meta = deepcopy((result.get("_strategy_platform") or {}))
    metrics = result.get("metrics")
    return {
        "id": row.id,
        "strategy_id": row.strategy_id,
        "strategy_version_id": platform_meta.get("strategy_version_id"),
        "status": row.status,
        "progress": float(row.progress or 0.0),
        "start_date": _iso_datetime(row.start_date),
        "end_date": _iso_datetime(row.end_date),
        "initial_capital": float(row.initial_capital or 0.0),
        "frequency": platform_meta.get("frequency") or row.backtest_mode or "daily_minute",
        "benchmark": row.benchmark or "沪深300",
        "metrics": metrics,
        "result": result if result else None,
        "artifact_root": platform_meta.get("artifact_root"),
        "error_message": row.error_message,
        "created_at": _iso_datetime(row.created_at),
        "started_at": _iso_datetime(row.started_at),
        "completed_at": _iso_datetime(row.completed_at),
    }


def _experiment_to_payload(row: EvolutionExperimentDB) -> dict[str, Any]:
    return {
        "id": row.id,
        "strategy_id": row.strategy_id,
        "objective": row.objective,
        "status": row.status,
        "progress": float(row.progress or 0.0),
        "search_space": deepcopy(row.search_space or {}),
        "base_backtest_run_id": row.base_backtest_run_id,
        "candidates": [_candidate_to_payload(item) for item in row.candidates],
        "created_at": _iso_datetime(row.created_at),
        "updated_at": _iso_datetime(row.updated_at),
    }


def _candidate_to_payload(row: EvolutionCandidateDB) -> dict[str, Any]:
    return {
        "id": row.id,
        "experiment_id": row.experiment_id,
        "name": row.name,
        "score": float(row.score or 0.0),
        "status": row.status or "candidate",
        "improvement_summary": row.improvement_summary or "",
        "risk_flags": list(row.risk_flags or []),
        "metrics": deepcopy(row.metrics or {}),
        "dsl_patch": deepcopy(row.dsl_patch or {}),
        "accepted_at": _iso_datetime(row.accepted_at),
        "created_at": _iso_datetime(row.created_at),
    }


def _is_platform_strategy(row: StrategyDB) -> bool:
    return isinstance(row.parameters, dict) and PLATFORM_META_KEY in row.parameters


def _is_test_strategy(item: dict[str, Any]) -> bool:
    name = str(item.get("name") or "")
    if item.get("source") == "test":
        return True
    if name in _KNOWN_TEST_STRATEGY_NAMES:
        return True
    tags = set(item.get("tags") or [])
    if "测试污染已归档" in tags:
        return True
    if "测试" in tags:
        return True
    if not {"AI创建", "待回测"}.issubset(tags):
        return False
    if int(item.get("run_count") or 0) != 0:
        return False
    return bool(_REALTIME_TEST_STRATEGY_NAME_RE.match(name))


def _is_platform_backtest(row: BacktestJobDB) -> bool:
    return isinstance(row.result, dict) and isinstance(row.result.get("_strategy_platform"), dict)


def _to_strategy_type(value: str) -> StrategyType:
    return StrategyType(value)


def _to_strategy_status(value: str | None) -> StrategyStatus:
    mapping = {
        "draft": StrategyStatus.DRAFT,
        "active": StrategyStatus.ACTIVE,
        "paused": StrategyStatus.PAUSED,
        "archived": StrategyStatus.ARCHIVED,
        "candidate": StrategyStatus.DRAFT,
    }
    return mapping.get(str(value or "draft"), StrategyStatus.DRAFT)


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    text = str(value)
    if len(text) == 10 and text.count("-") == 2:
        return datetime.fromisoformat(f"{text}T00:00:00+00:00")
    return datetime.fromisoformat(text)


def _iso_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_candidate_id(candidate_id: Any, experiment_id: Any, name: Any) -> str:
    raw = str(candidate_id or "")
    if raw and len(raw) <= 36:
        return raw
    seed = f"{experiment_id}:{name}:{raw}"
    digest = hashlib.md5(seed.encode("utf-8")).hexdigest()[:8]
    base = str(experiment_id or "").replace("-", "")[:27] or hashlib.md5(seed.encode("utf-8")).hexdigest()[:27]
    return f"{base}_{digest}"[:36]
