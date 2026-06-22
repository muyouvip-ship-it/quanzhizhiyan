#!/usr/bin/env bash
set -euo pipefail

BACKEND_PORT="${BACKEND_PORT:-8500}"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"
BACKEND_HOST="${BACKEND_HOST:-127.0.0.1}"
FRONTEND_HOST="${FRONTEND_HOST:-127.0.0.1}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

cd "$ROOT"

pid_is_running() {
  local pid="${1:-}"
  [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1
}

read_pidfile() {
  local pidfile="$1"
  [[ -f "$pidfile" ]] || return 1
  tr -d '[:space:]' < "$pidfile"
}

refresh_pidfile_from_port() {
  local pidfile="$1"
  local port="$2"
  local pid

  pid="$(lsof -tiTCP:"$port" -sTCP:LISTEN | head -n 1 || true)"
  if [[ -n "$pid" ]]; then
    printf '%s\n' "$pid" > "$pidfile"
  fi
}

cleanup_stale_pidfile() {
  local pidfile="$1"
  local label="$2"
  local pid

  pid="$(read_pidfile "$pidfile" || true)"
  if [[ -n "$pid" ]] && ! pid_is_running "$pid"; then
    rm -f "$pidfile"
    echo "[status] removed stale ${label} pid file ($pid)"
  fi
}

curl_host() {
  local host="$1"
  if [[ "$host" == "0.0.0.0" ]]; then
    printf '127.0.0.1'
  else
    printf '%s' "$host"
  fi
}

cleanup_stale_pidfile .runtime/backend.pid backend
cleanup_stale_pidfile .runtime/frontend.pid frontend
cleanup_stale_pidfile .runtime/scheduler.pid scheduler

echo "[status] listening ports"
lsof -nP -iTCP -sTCP:LISTEN | egrep ":(${BACKEND_PORT}|${FRONTEND_PORT})\\b" || true

echo

echo "[status] backend health"
curl -sS --max-time 5 "http://${BACKEND_HOST}:${BACKEND_PORT}/healthz" || \
  curl -sS --max-time 5 "http://${BACKEND_HOST}:${BACKEND_PORT}/health" || true

echo

echo "[status] frontend"
curl -I -sS --max-time 5 "http://${FRONTEND_HOST}:${FRONTEND_PORT}" | head -n 1 || true
frontend_check_host="$(curl_host "$FRONTEND_HOST")"
if curl -sS --max-time 5 "http://${frontend_check_host}:${FRONTEND_PORT}/" | grep -q '/@vite/client'; then
  echo "vite dev server: ok"
else
  echo "vite dev server: failed"
fi
if curl -fsS --max-time 5 "http://${frontend_check_host}:${FRONTEND_PORT}/healthz" >/dev/null; then
  echo "frontend api proxy: ok"
else
  echo "frontend api proxy: failed"
fi

echo

echo "[status] scheduler"
if [[ -f .runtime/scheduler.pid ]]; then
  pid="$(read_pidfile .runtime/scheduler.pid)"
  if pid_is_running "$pid"; then
    ps -p "$pid" -o pid=,etime=,command=
  fi
elif pgrep -f "tradingagents-scheduler|python -m scheduler.main" >/dev/null 2>&1; then
  pgrep -fo "tradingagents-scheduler|python -m scheduler.main" > .runtime/scheduler.pid
  pgrep -fal "tradingagents-scheduler|python -m scheduler.main"
else
  echo "scheduler pid file not found"
fi

if lsof -iTCP:"$BACKEND_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  refresh_pidfile_from_port .runtime/backend.pid "$BACKEND_PORT"
fi

if lsof -iTCP:"$FRONTEND_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  refresh_pidfile_from_port .runtime/frontend.pid "$FRONTEND_PORT"
fi
