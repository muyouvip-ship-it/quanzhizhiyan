from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from api.services.factor_registry import get_factor_catalog_definition
from api.services.market_data_pipeline_service import preferred_minute_kline_table
from api.services.strategy_dsl_schema import StrategyDslSchema


ENTRY_RULE_CATALOG: dict[str, dict[str, Any]] = {
    "trend": {
        "required_fields": ["close"],
        "rule_key": "close_above_indicator",
    },
    "alligator_opening": {
        "required_fields": ["close", "high", "low"],
        "rule_key": "alligator_proxy",
    },
    "intraday_confirm": {
        "required_fields": ["minute_30m.close", "minute_30m.vwap"],
        "rule_key": "lazy_minute_confirm",
    },
    "cross_above": {
        "required_fields": ["close"],
        "rule_key": "cross_above",
    },
}


EXIT_RULE_CATALOG: dict[str, dict[str, Any]] = {
    "cross_below": {"required_fields": ["close"], "rule_key": "close_below_indicator"},
    "atr_trailing_stop": {"required_fields": ["high", "low", "close"], "rule_key": "atr_trailing_stop"},
    "factor_rank_drop": {"required_fields": ["factor_score"], "rule_key": "factor_rank_drop"},
}


@dataclass
class CompiledStrategy:
    status: str
    errors: list[str]
    warnings: list[str]
    required_fields: list[str]
    compiled_targets: list[str]
    normalized_dsl: dict[str, Any]
    factor_definitions: list[dict[str, Any]]
    selection: dict[str, Any]
    entry_rules: list[dict[str, Any]]
    exit_rules: list[dict[str, Any]]
    execution_rules: dict[str, Any]
    expressions: dict[str, Any]
    timeframes_required: list[str]
    minute_requirements: dict[str, Any]
    backend_resolution: dict[str, Any]
    selection_plan: dict[str, Any]
    entry_rule_plan: list[dict[str, Any]]
    exit_rule_plan: list[dict[str, Any]]
    pending_confirmations: list[dict[str, Any]]
    future_function_risks: list[dict[str, Any]]

    def to_response_payload(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "errors": self.errors,
            "warnings": self.warnings,
            "required_fields": self.required_fields,
            "compiled_targets": self.compiled_targets,
            "factor_count": len(self.factor_definitions),
            "entry_rule_count": len(self.entry_rules),
            "exit_rule_count": len(self.exit_rules),
            "runtime_engine": {
                "scan": self.execution_rules.get("scan_engine"),
                "compute": self.execution_rules.get("compute_engine"),
                "minute_loading": self.execution_rules.get("minute_loading"),
            },
            "execution_plan": {
                "selection_plan": self.selection_plan,
                "entry_rule_plan": self.entry_rule_plan,
                "exit_rule_plan": self.exit_rule_plan,
            },
            "timeframes_required": self.timeframes_required,
            "minute_requirements": self.minute_requirements,
            "backend_resolution": self.backend_resolution,
            "pending_confirmations": self.pending_confirmations,
            "future_function_risks": self.future_function_risks,
            "expression_preview": {
                "polars": self.expressions.get("polars", [])[:4],
                "duckdb": self.expressions.get("duckdb", [])[:4],
            },
        }


def compile_strategy_dsl(dsl: dict[str, Any]) -> CompiledStrategy:
    errors: list[str] = []
    warnings: list[str] = []
    normalized = dict(dsl or {})
    try:
        normalized = StrategyDslSchema.model_validate(dsl or {}).model_dump(exclude_none=True)
    except ValidationError as exc:
        errors.extend(_format_schema_errors(exc))

    # Normalize Chinese indicator names to internal column names
    _CN_INDICATOR_MAP = {"波段": "first_day_band", "B1": "first_day_band_b1"}
    for branch_key in ("entry", "exit"):
        for condition in (normalized.get(branch_key) or {}).get("conditions") or []:
            for key in ("left", "right", "field", "indicator"):
                value = condition.get(key)
                if isinstance(value, str) and value in _CN_INDICATOR_MAP:
                    condition[key] = _CN_INDICATOR_MAP[value]

    required_fields = {"close", "open"}
    compiled_targets = ["dsl_ast", "a_share_execution_rules"]
    factor_defs: list[dict[str, Any]] = []
    polars_exprs: list[str] = []
    duckdb_exprs: list[str] = []
    timeframes_required: set[str] = {"1d"}
    minute_timeframes: set[str] = set()
    pending_confirmations: list[dict[str, Any]] = []

    factor_model = normalized.get("factor_model") or {}
    factors = factor_model.get("factors") or []
    if not factors:
        errors.append("factor_model.factors 不能为空。")

    for index, factor in enumerate(factors):
        name = str(factor.get("name") or "").strip()
        if not name:
            errors.append(f"factor_model.factors[{index}].name 缺失。")
            continue
        catalog = get_factor_catalog_definition(name)
        direction = factor.get("direction") or "higher_better"
        transform = factor.get("transform") or "rank_pct"
        weight = float(factor.get("weight") or 0.0)
        if weight <= 0:
            warnings.append(f"因子 {name} 的权重 <= 0，回测时会被忽略。")
        if catalog is None:
            warnings.append(f"因子 {name} 暂未映射到原生编译器，当前按自定义占位因子处理。")
            pending_confirmations.append(
                {
                    "kind": "unknown_factor",
                    "path": f"factor_model.factors[{index}].name",
                    "value": name,
                    "message": "未知因子尚未注册，请确认字段映射或补充自定义实现。",
                }
            )
            factor_def = {
                "name": name,
                "source_column": name,
                "direction": direction,
                "transform": transform,
                "weight": weight,
                "required_fields": [],
                "window": None,
                "rank_scope": "cross_section:date",
                "backend": "custom_fallback",
                "polars_expr": f"custom_factor('{name}')",
                "duckdb_sql": f"custom_factor_{name}",
            }
        else:
            required_fields.update(catalog["required_fields"])
            factor_def = {
                "name": name,
                "source_column": catalog["source_column"],
                "direction": direction,
                "transform": transform,
                "weight": weight,
                "required_fields": catalog["required_fields"],
                "window": _infer_factor_window(name),
                "rank_scope": "cross_section:date" if transform == "rank_pct" else "symbol_series",
                "backend": "polars" if _has_module("polars") else "pandas_fallback",
                "polars_expr": catalog["polars_expr"],
                "duckdb_sql": catalog["duckdb_sql"],
            }
        factor_defs.append(factor_def)
        polars_exprs.append(f"{name} := {factor_def['polars_expr']}")
        duckdb_exprs.append(f"{name} := {factor_def['duckdb_sql']}")

    entry_rules = _compile_conditions(
        normalized.get("entry") or {},
        ENTRY_RULE_CATALOG,
        required_fields,
        warnings,
        pending_confirmations,
        "entry",
        timeframes_required,
        minute_timeframes,
    )
    exit_rules = _compile_conditions(
        normalized.get("exit") or {},
        EXIT_RULE_CATALOG,
        required_fields,
        warnings,
        pending_confirmations,
        "exit",
        timeframes_required,
        minute_timeframes,
    )

    universe = normalized.get("universe") or {}
    for item in universe.get("filters") or []:
        field = item.get("field")
        if field:
            required_fields.add(str(field))

    execution = normalized.get("execution") or {}
    data_engine = execution.get("data_engine") or {}
    minute_loading = execution.get("minute_loading") or {}
    if minute_loading.get("forbid_full_market_preload") is not True:
        warnings.append("建议明确禁止全市场分钟线预加载，避免分钟级 OOM。")

    compiled_targets.extend(["duckdb_scan_ir", "polars_expr_ir"])
    if _has_module("duckdb"):
        compiled_targets.append("duckdb_runtime")
    else:
        warnings.append("当前环境未安装 duckdb，扫描阶段将退回 SQLAlchemy / pandas。")
    if _has_module("polars"):
        compiled_targets.append("polars_runtime")
    else:
        warnings.append("当前环境未安装 polars，因子计算阶段将退回 pandas。")

    selection = {
        "top_n": int(((factor_model.get("select") or {}).get("top_n")) or 30),
        "min_score": float(((factor_model.get("select") or {}).get("min_score")) or 0.6),
        "score_method": factor_model.get("score_method") or "weighted_sum",
    }
    if not 0 <= selection["min_score"] <= 1:
        errors.append("factor_model.select.min_score 必须在 0 到 1 之间。")

    execution_rules = {
        "market": execution.get("market") or "A_SHARE",
        "lot_size": int(execution.get("lot_size") or 100),
        "fill_timing": execution.get("fill_timing") or "next_open",
        "scan_engine": data_engine.get("filter") or "duckdb",
        "compute_engine": (factor_model.get("engine") or data_engine.get("factor_compute") or "polars_expr"),
        "minute_loading": minute_loading.get("mode") or "lazy_by_watchlist",
    }
    if execution_rules["lot_size"] != 100:
        warnings.append("A 股默认按 100 股整数手撮合，当前 DSL 已偏离默认设置。")

    selection_plan = {
        "market": universe.get("market") or "A_SHARE",
        "filters": universe.get("filters") or [],
        "include_concepts": universe.get("include_concepts") or [],
        "exclude_st": bool(universe.get("exclude_st", True)),
        "exclude_suspended": bool(universe.get("exclude_suspended", True)),
        "rebalance_frequency": factor_model.get("rebalance_frequency") or "weekly",
        "score_method": selection["score_method"],
        "top_n": selection["top_n"],
        "min_score": selection["min_score"],
    }
    minute_requirements = {
        "enabled": bool(minute_timeframes),
        "timeframes": sorted(minute_timeframes),
        "source_table": preferred_minute_kline_table() if minute_timeframes else None,
        "fields": sorted([field for field in required_fields if field.startswith("minute_")]),
        "loading_mode": execution_rules["minute_loading"],
    }
    backend_resolution = {
        "scan": "duckdb" if _has_module("duckdb") else "sqlalchemy_fallback",
        "compute": "polars" if _has_module("polars") else "pandas_fallback",
        "artifact": "parquet" if _has_module("pyarrow") else "json",
        "fallback_mode": not (_has_module("duckdb") and _has_module("polars")),
    }
    future_function_risks = _detect_future_function_risks(normalized)
    for risk in future_function_risks:
        errors.append(f"疑似未来函数: {risk['path']} -> {risk['value']}")

    return CompiledStrategy(
        status="failed" if errors else "passed",
        errors=errors,
        warnings=_unique_keep_order(warnings),
        required_fields=sorted(required_fields),
        compiled_targets=_unique_keep_order(compiled_targets),
        normalized_dsl=normalized,
        factor_definitions=factor_defs,
        selection=selection,
        entry_rules=entry_rules,
        exit_rules=exit_rules,
        execution_rules=execution_rules,
        expressions={"polars": polars_exprs, "duckdb": duckdb_exprs},
        timeframes_required=sorted(timeframes_required, key=_timeframe_sort_key),
        minute_requirements=minute_requirements,
        backend_resolution=backend_resolution,
        selection_plan=selection_plan,
        entry_rule_plan=entry_rules,
        exit_rule_plan=exit_rules,
        pending_confirmations=pending_confirmations,
        future_function_risks=future_function_risks,
    )


def _compile_conditions(
    node: dict[str, Any],
    catalog: dict[str, dict[str, Any]],
    required_fields: set[str],
    warnings: list[str],
    pending_confirmations: list[dict[str, Any]],
    branch_name: str,
    timeframes_required: set[str],
    minute_timeframes: set[str],
) -> list[dict[str, Any]]:
    compiled: list[dict[str, Any]] = []
    allowed_ops = {
        "trend": {"above", "below"},
    }
    for condition in node.get("conditions") or []:
        condition_type = str(condition.get("type") or "").strip()
        if not condition_type:
            continue
        timeframe = str(condition.get("timeframe") or "1d")
        timeframes_required.add(timeframe)
        if timeframe.endswith("m"):
            minute_timeframes.add(timeframe)
        if condition_type in catalog:
            spec = catalog[condition_type]
            condition_requirements = list(spec["required_fields"])
            for side in ("left", "right", "field", "indicator"):
                value = condition.get(side)
                if isinstance(value, str) and value and not value.startswith("minute_"):
                    condition_requirements.append(value)
            condition_requirements = _unique_keep_order(condition_requirements)
            required_fields.update(condition_requirements)
            minute_fields = [field for field in condition_requirements if field.startswith("minute_")]
            if minute_fields and condition_type == "intraday_confirm":
                for minute_field in minute_fields:
                    if not minute_field.startswith(f"minute_{timeframe}"):
                        warnings.append(f"{branch_name} 条件 {condition_type} 期望使用 {timeframe} 分钟字段。")
            compiled.append(
                {
                    "type": condition_type,
                    "rule_key": spec["rule_key"],
                    "timeframe": timeframe,
                    "params": condition,
                    "data_requirements": condition_requirements,
                }
            )
            if condition_type in allowed_ops:
                op = condition.get("op")
                if op and op not in allowed_ops[condition_type]:
                    pending_confirmations.append(
                        {
                            "kind": "unknown_operator",
                            "path": f"{branch_name}.conditions[{len(compiled) - 1}].op",
                            "value": op,
                            "message": f"{condition_type} 条件暂不识别算子 {op}，请确认是否改为 above/below。",
                        }
                    )
            continue
        warnings.append(f"{branch_name} 条件 {condition_type} 暂未编译为原生规则，回测阶段将使用近似代理逻辑。")
        pending_confirmations.append(
            {
                "kind": "unknown_condition",
                "path": f"{branch_name}.conditions[{len(compiled)}].type",
                "value": condition_type,
                "message": "未知条件类型尚未注册，请确认算子名或补充原生编译支持。",
            }
        )
        compiled.append(
            {
                "type": condition_type,
                "rule_key": "fallback_proxy",
                "timeframe": timeframe,
                "params": condition,
                "data_requirements": [],
            }
        )
    return compiled


def _infer_factor_window(name: str) -> int | None:
    if "_" not in name:
        return None
    last = name.rsplit("_", 1)[-1]
    digits = "".join(ch for ch in last if ch.isdigit())
    return int(digits) if digits else None


def _timeframe_sort_key(value: str) -> tuple[int, str]:
    order = {"1m": 1, "5m": 2, "15m": 3, "30m": 4, "1d": 5, "1w": 6}
    return (order.get(value, 99), value)


def _unique_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _format_schema_errors(exc: ValidationError) -> list[str]:
    errors: list[str] = []
    for item in exc.errors():
        path = ".".join(str(part) for part in item.get("loc", []))
        message = item.get("msg", "DSL schema 校验失败")
        errors.append(f"DSL schema 校验失败: {path}: {message}")
    return errors


def _has_module(name: str) -> bool:
    try:
        __import__(name)
        return True
    except Exception:
        return False


def _detect_future_function_risks(dsl: dict[str, Any]) -> list[dict[str, Any]]:
    suspicious_tokens = (
        "future",
        "forward_return",
        "future_return",
        "ret_5d",
        "ret_20d",
        "lead(",
        "lead_",
        "next_bar",
        "next_close",
        "tomorrow",
        "label",
    )
    risks: list[dict[str, Any]] = []

    def check(path: str, value: Any) -> None:
        if not isinstance(value, str):
            return
        lowered = value.lower()
        if any(token in lowered for token in suspicious_tokens):
            risks.append(
                {
                    "path": path,
                    "value": value,
                    "message": "检测到疑似未来信息字段，请改为当前或历史可观测字段。",
                }
            )

    for index, factor in enumerate((dsl.get("factor_model") or {}).get("factors") or []):
        check(f"factor_model.factors[{index}].name", factor.get("name"))

    for branch_name in ("entry", "exit"):
        for index, condition in enumerate((dsl.get(branch_name) or {}).get("conditions") or []):
            for key in ("field", "indicator", "left", "right"):
                check(f"{branch_name}.conditions[{index}].{key}", condition.get(key))
            for sub_index, sub_condition in enumerate(condition.get("conditions") or []):
                check(f"{branch_name}.conditions[{index}].conditions[{sub_index}].left", sub_condition.get("left"))
                check(f"{branch_name}.conditions[{index}].conditions[{sub_index}].right", sub_condition.get("right"))

    return _unique_risk_items(risks)


def _unique_risk_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    result: list[dict[str, Any]] = []
    for item in items:
        key = (str(item.get("path") or ""), str(item.get("value") or ""))
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result
