from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.core.env import load_project_env


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"} if token else {}


def _get_json(url: str, *, token: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    response = requests.get(url, headers=_headers(token), params=params or {}, timeout=30)
    response.raise_for_status()
    return response.json()


def _post_json(
    url: str,
    *,
    token: str,
    payload: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = requests.post(url, headers=_headers(token), json=payload or {}, params=params or {}, timeout=30)
    response.raise_for_status()
    return response.json()


def _print_step(title: str, payload: dict[str, Any]) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


def _safe_paper_guard(account_key: str) -> None:
    if account_key != "paper_sim":
        raise SystemExit("拒绝执行：实时监控联调脚本当前仅允许连接虚拟仓 account_key=paper_sim")


def _obtain_access_token(base_url: str, email: str, access_token: str | None) -> str:
    if access_token:
        return access_token
    request_resp = requests.post(
        f"{base_url}/v1/auth/request-code",
        json={"email": email},
        timeout=20,
    )
    request_resp.raise_for_status()
    request_payload = request_resp.json()
    dev_code = request_payload.get("dev_code")
    if not dev_code:
        raise SystemExit("未获取到 dev_code，请改用 --access-token 传入登录令牌。")
    verify_resp = requests.post(
        f"{base_url}/v1/auth/verify-code",
        json={"email": email, "code": dev_code},
        timeout=20,
    )
    verify_resp.raise_for_status()
    verify_payload = verify_resp.json()
    token = verify_payload.get("access_token")
    if not token:
        raise SystemExit("登录失败：后端未返回 access_token")
    return str(token)


def _pick_strategy(base_url: str, token: str, strategy_id: str | None) -> dict[str, Any]:
    if strategy_id:
        return _get_json(f"{base_url}/v1/strategies/{strategy_id}", token=token)
    items = _get_json(f"{base_url}/v1/strategies", token=token).get("strategies") or []
    preferred = None
    for item in items:
        if item.get("status") == "active" and item.get("strategy_type") in {"trading", "portfolio", "selection"}:
            preferred = item
            break
    if preferred is None and items:
        preferred = items[0]
    if preferred is None:
        raise SystemExit("未找到可用策略，请先在策略管理中创建至少一个 DSL 策略。")
    return preferred


def main() -> int:
    load_project_env()
    parser = argparse.ArgumentParser(description="QMT 实时监控 P1 联调脚本：创建虚拟仓监控实例，启动并手动执行一轮。")
    parser.add_argument("--api-base-url", default=os.getenv("API_BASE_URL") or "http://127.0.0.1:8500")
    parser.add_argument("--access-token", default=os.getenv("TA_ACCESS_TOKEN"))
    parser.add_argument("--email", default=os.getenv("QMT_REALTIME_CHECK_EMAIL") or "realtime-check@test.com")
    parser.add_argument("--strategy-id", default=os.getenv("QMT_REALTIME_CHECK_STRATEGY_ID"))
    parser.add_argument("--account-key", default="paper_sim")
    parser.add_argument("--symbols", default="000001.SZ,002105.SZ")
    parser.add_argument("--execution-mode", choices=["auto", "monitor_only"], default="auto")
    parser.add_argument("--skip-start", action="store_true", help="只创建实例，不启动")
    parser.add_argument("--skip-run-once", action="store_true", help="启动后不执行手动单轮")
    parser.add_argument("--stop-after-run", action="store_true", help="联调结束后停机实例")
    args = parser.parse_args()

    base_url = str(args.api_base_url).rstrip("/")
    _safe_paper_guard(args.account_key)

    token = _obtain_access_token(base_url, str(args.email), args.access_token)

    try:
        health = _get_json(f"{base_url}/healthz", token=token)
    except Exception:
        health = _get_json(f"{base_url}/v1/health", token=token)
    _print_step("1. 后端健康检查", health)

    strategy = _pick_strategy(base_url, token, args.strategy_id)
    _print_step(
        "2. 选定策略",
        {
            "strategy_id": strategy.get("id"),
            "name": strategy.get("name"),
            "strategy_type": strategy.get("strategy_type"),
            "status": strategy.get("status"),
        },
    )

    symbols = [item.strip().upper() for item in str(args.symbols).split(",") if item.strip()]
    name = f"QMT实时联调-{datetime.now().strftime('%m%d-%H%M%S')}"
    created = _post_json(
        f"{base_url}/v1/realtime/monitors",
        token=token,
        payload={
            "name": name,
            "strategy_id": strategy["id"],
            "account_key": args.account_key,
            "execution_mode": args.execution_mode,
            "monitor_pool": {
                "mode": "strategy_positions_watchlist",
                "manual_symbols": symbols,
                "symbols": symbols,
            },
            "config": {
                "poll_interval_seconds": 20,
                "max_signals_per_cycle": 2,
            },
        },
    )
    monitor_id = created["id"]
    _print_step(
        "3. 创建实时监控实例",
        {
            "monitor_id": monitor_id,
            "name": created.get("name"),
            "status": created.get("status"),
            "account_key": created.get("account_key"),
            "execution_mode": created.get("execution_mode"),
            "resolved_symbols": (created.get("monitor_pool") or {}).get("resolved_symbols"),
        },
    )

    if not args.skip_start:
        started = _post_json(f"{base_url}/v1/realtime/monitors/{monitor_id}/start", token=token)
        _print_step(
            "4. 启动实例",
            {
                "monitor_id": monitor_id,
                "status": started.get("status"),
                "execution_mode": started.get("execution_mode"),
                "auto_trade_enabled": started.get("auto_trade_enabled"),
                "last_heartbeat_at": started.get("last_heartbeat_at"),
            },
        )
    else:
        print("\n已跳过启动步骤。")

    if not args.skip_run_once:
        run_once = _post_json(f"{base_url}/v1/realtime/monitors/{monitor_id}/run-once", token=token)
        events = run_once.get("events") or []
        event_types = [item.get("event_type") for item in events]
        _print_step(
            "5. 手动执行一轮监控",
            {
                "monitor_id": monitor_id,
                "monitor_status": (run_once.get("monitor") or {}).get("status"),
                "event_count": len(events),
                "event_types": event_types,
            },
        )
        if "order_submitted" not in event_types:
            print("\n提示：本轮未产生 order_submitted，可能原因是当前分钟确认未命中、风控拦截或策略本轮无信号。")
    else:
        print("\n已跳过单轮执行步骤。")

    time.sleep(1)
    detail = _get_json(f"{base_url}/v1/realtime/monitors/{monitor_id}", token=token)
    events_payload = _get_json(f"{base_url}/v1/realtime/monitors/{monitor_id}/events", token=token, params={"limit": 20})
    orders_payload = _get_json(f"{base_url}/v1/realtime/monitors/{monitor_id}/orders", token=token)
    trades_payload = _get_json(f"{base_url}/v1/realtime/monitors/{monitor_id}/trades", token=token)
    positions_payload = _get_json(f"{base_url}/v1/realtime/monitors/{monitor_id}/positions", token=token)
    performance_payload = _get_json(f"{base_url}/v1/realtime/monitors/{monitor_id}/performance", token=token)

    _print_step(
        "6. 联调结果汇总",
        {
            "monitor": {
                "id": detail.get("id"),
                "status": detail.get("status"),
                "execution_mode": detail.get("execution_mode"),
                "stats": (detail.get("state") or {}).get("stats"),
                "circuit_breaker": detail.get("circuit_breaker"),
                "resolved_symbols": (detail.get("monitor_pool") or {}).get("resolved_symbols"),
            },
            "events": len(events_payload.get("items") or []),
            "orders": len(orders_payload.get("items") or []),
            "trades": len(trades_payload.get("items") or []),
            "positions": len(positions_payload.get("positions") or []),
            "performance": {
                "strategy_pnl": (performance_payload.get("strategy") or {}).get("pnl"),
                "hold_pnl": (performance_payload.get("hold_baseline") or {}).get("pnl"),
                "excess_pnl": (performance_payload.get("excess") or {}).get("pnl"),
            },
        },
    )

    if args.stop_after_run:
        stopped = _post_json(f"{base_url}/v1/realtime/monitors/{monitor_id}/stop", token=token)
        _print_step("7. 停机实例", {"monitor_id": monitor_id, "status": stopped.get("status")})
    else:
        print(f"\n联调完成，实例已保留：{monitor_id}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
