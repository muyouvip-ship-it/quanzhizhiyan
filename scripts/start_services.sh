#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BACKEND_PORT="${BACKEND_PORT:-8500}"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"
BACKEND_HOST="${BACKEND_HOST:-127.0.0.1}"
FRONTEND_HOST="${FRONTEND_HOST:-127.0.0.1}"
SCHEDULER_CMD_PATTERN="${SCHEDULER_CMD_PATTERN:-tradingagents-scheduler}"
BACKEND_SCREEN_NAME="${BACKEND_SCREEN_NAME:-tradingagents-backend}"
FRONTEND_SCREEN_NAME="${FRONTEND_SCREEN_NAME:-ta-frontend}"
SCHEDULER_SCREEN_NAME="${SCHEDULER_SCREEN_NAME:-tradingagents-scheduler}"

cd "$ROOT"
mkdir -p .runtime

pid_is_running() {
  local pid="${1:-}"
  [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1
}

read_pidfile() {
  local pidfile="$1"
  [[ -f "$pidfile" ]] || return 1
  tr -d '[:space:]' < "$pidfile"
}

remove_stale_pidfile() {
  local pidfile="$1"
  local label="$2"
  local pid

  pid="$(read_pidfile "$pidfile" || true)"
  if [[ -n "$pid" ]] && ! pid_is_running "$pid"; then
    rm -f "$pidfile"
    echo "[start] removed stale ${label} pid file ($pid)"
  fi
}

write_pidfile_from_port() {
  local pidfile="$1"
  local port="$2"
  local pid

  pid="$(lsof -tiTCP:"$port" -sTCP:LISTEN | head -n 1 || true)"
  [[ -n "$pid" ]] && printf '%s\n' "$pid" > "$pidfile"
}

wait_for_port() {
  local host="$1"
  local port="$2"
  local timeout="${3:-15}"
  local elapsed=0

  while (( elapsed < timeout )); do
    if curl -sS --max-time 2 "http://${host}:${port}" >/dev/null 2>&1 || \
       lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    ((elapsed+=1))
  done

  return 1
}

wait_for_pid() {
  local pid="$1"
  local timeout="${2:-10}"
  local elapsed=0

  while (( elapsed < timeout )); do
    if pid_is_running "$pid"; then
      return 0
    fi
    sleep 1
    ((elapsed+=1))
  done

  return 1
}

curl_host() {
  local host="$1"
  if [[ "$host" == "0.0.0.0" ]]; then
    printf '127.0.0.1'
  else
    printf '%s' "$host"
  fi
}

frontend_is_vite_dev() {
  local host
  host="$(curl_host "$FRONTEND_HOST")"
  curl -sS --max-time 5 "http://${host}:${FRONTEND_PORT}/" | grep -q '/@vite/client'
}

frontend_proxy_is_ready() {
  local host
  host="$(curl_host "$FRONTEND_HOST")"
  curl -fsS --max-time 5 "http://${host}:${FRONTEND_PORT}/healthz" >/dev/null
}

frontend_is_ready() {
  frontend_is_vite_dev && frontend_proxy_is_ready
}

frontend_port_owner_pids() {
  lsof -tiTCP:"$FRONTEND_PORT" -sTCP:LISTEN || true
}

frontend_port_owned_by_project() {
  local pids="$1"
  local pid

  [[ -n "$pids" ]] || return 1
  while read -r pid; do
    [[ -n "$pid" ]] || continue
    if lsof -nP -p "$pid" 2>/dev/null | grep -q "$ROOT"; then
      return 0
    fi
  done <<< "$pids"

  return 1
}

stop_project_frontend_port_owner() {
  local pids="$1"
  local elapsed=0

  [[ -n "$pids" ]] || return 0
  echo "[start] stopping stale frontend process on :$FRONTEND_PORT"
  echo "$pids" | xargs kill || true
  while (( elapsed < 5 )); do
    if ! lsof -iTCP:"$FRONTEND_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    ((elapsed+=1))
  done

  echo "[start] stale frontend process did not release :$FRONTEND_PORT"
  lsof -nP -iTCP:"$FRONTEND_PORT" -sTCP:LISTEN || true
  return 1
}

tail_log() {
  local logfile="$1"
  if [[ -f "$logfile" ]]; then
    echo "[start] recent log: $logfile"
    tail -n 40 "$logfile" || true
  fi
}

screen_exists() {
  local name="$1"
  screen -ls 2>/dev/null | grep -q "[.]${name}[[:space:]]"
}
start_screen_session() {
  local name="$1"
  local command="$2"
  if command -v screen >/dev/null 2>&1; then
    screen -dmS "$name" /bin/zsh -lc "$command"
    return 0
  fi
  return 1
}

echo "[start] root=$ROOT"
remove_stale_pidfile .runtime/backend.pid backend
remove_stale_pidfile .runtime/frontend.pid frontend
remove_stale_pidfile .runtime/scheduler.pid scheduler

status=0

if lsof -iTCP:"$BACKEND_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "[start] backend already listening on :$BACKEND_PORT"
  write_pidfile_from_port .runtime/backend.pid "$BACKEND_PORT"
else
  echo "[start] starting backend on :$BACKEND_PORT"
  backend_cmd="cd '$ROOT' && exec uv run uvicorn api.app:app --host '$BACKEND_HOST' --port '$BACKEND_PORT' >> '$ROOT/.runtime/backend.log' 2>&1"
  if start_screen_session "$BACKEND_SCREEN_NAME" "$backend_cmd"; then
    sleep 1
  else
    nohup uv run uvicorn api.app:app --host "$BACKEND_HOST" --port "$BACKEND_PORT" \
      > .runtime/backend.log 2>&1 &
    echo $! > .runtime/backend.pid
  fi
  if ! wait_for_port "$BACKEND_HOST" "$BACKEND_PORT" 20; then
    echo "[start] backend failed to become ready on :$BACKEND_PORT"
    tail_log .runtime/backend.log
    status=1
  else
    write_pidfile_from_port .runtime/backend.pid "$BACKEND_PORT"
  fi
fi

if [[ -f .runtime/scheduler.pid ]] && pid_is_running "$(read_pidfile .runtime/scheduler.pid)"; then
  echo "[start] scheduler already running with pid $(read_pidfile .runtime/scheduler.pid)"
elif pgrep -f "$SCHEDULER_CMD_PATTERN" >/dev/null 2>&1; then
  echo "[start] scheduler already running (matched pattern: $SCHEDULER_CMD_PATTERN)"
  pgrep -fo "$SCHEDULER_CMD_PATTERN" > .runtime/scheduler.pid
else
  echo "[start] starting scheduler"
  scheduler_cmd="cd '$ROOT' && exec uv run tradingagents-scheduler >> '$ROOT/.runtime/scheduler.log' 2>&1"
  if start_screen_session "$SCHEDULER_SCREEN_NAME" "$scheduler_cmd"; then
    sleep 1
    pgrep -fo "$SCHEDULER_CMD_PATTERN" > .runtime/scheduler.pid || true
  else
    nohup uv run tradingagents-scheduler > .runtime/scheduler.log 2>&1 &
    echo $! > .runtime/scheduler.pid
  fi
  scheduler_pid="$(read_pidfile .runtime/scheduler.pid || true)"
  if [[ -z "$scheduler_pid" ]] && pgrep -f "$SCHEDULER_CMD_PATTERN" >/dev/null 2>&1; then
    pgrep -fo "$SCHEDULER_CMD_PATTERN" > .runtime/scheduler.pid
    scheduler_pid="$(read_pidfile .runtime/scheduler.pid || true)"
  fi
  if [[ -z "$scheduler_pid" ]] || ! wait_for_pid "$scheduler_pid" 10; then
    echo "[start] scheduler exited during startup"
    tail_log .runtime/scheduler.log
    rm -f .runtime/scheduler.pid
    status=1
  fi
fi

frontend_pids="$(frontend_port_owner_pids)"
if [[ -n "$frontend_pids" ]]; then
  if frontend_is_ready; then
    echo "[start] frontend already listening on :$FRONTEND_PORT"
    write_pidfile_from_port .runtime/frontend.pid "$FRONTEND_PORT"
  elif frontend_port_owned_by_project "$frontend_pids"; then
    stop_project_frontend_port_owner "$frontend_pids" || status=1
  else
    echo "[start] frontend port :$FRONTEND_PORT is occupied but is not a healthy Vite dev server"
    lsof -nP -iTCP:"$FRONTEND_PORT" -sTCP:LISTEN || true
    status=1
  fi
fi

if [[ "$status" -eq 0 ]] && ! lsof -iTCP:"$FRONTEND_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "[start] starting frontend on :$FRONTEND_PORT"
  frontend_cmd="cd '$ROOT/frontend' && exec npm run dev -- --host '$FRONTEND_HOST' --port '$FRONTEND_PORT' --strictPort >> '$ROOT/.runtime/frontend.log' 2>&1"
  if start_screen_session "$FRONTEND_SCREEN_NAME" "$frontend_cmd"; then
    sleep 1
  else
    cd "$ROOT/frontend"
    nohup npm run dev -- --host "$FRONTEND_HOST" --port "$FRONTEND_PORT" --strictPort \
      > "$ROOT/.runtime/frontend.log" 2>&1 &
    echo $! > "$ROOT/.runtime/frontend.pid"
    cd "$ROOT"
  fi
  if ! wait_for_port "$FRONTEND_HOST" "$FRONTEND_PORT" 20 || ! frontend_is_ready; then
    echo "[start] frontend failed to become ready on :$FRONTEND_PORT"
    tail_log .runtime/frontend.log
    rm -f .runtime/frontend.pid
    status=1
  else
    write_pidfile_from_port .runtime/frontend.pid "$FRONTEND_PORT"
  fi
fi

echo "[check] backend"
curl -sS --max-time 8 "http://${BACKEND_HOST}:${BACKEND_PORT}/healthz" || \
  curl -sS --max-time 8 "http://${BACKEND_HOST}:${BACKEND_PORT}/health"

echo

echo "[check] frontend"
curl -I -sS --max-time 8 "http://${FRONTEND_HOST}:${FRONTEND_PORT}" | head -n 1
if frontend_is_ready; then
  echo "vite dev server: ok"
  echo "frontend api proxy: ok"
else
  echo "vite dev server / frontend api proxy: failed"
  status=1
fi

echo

if [[ -f .runtime/scheduler.pid ]] && pid_is_running "$(read_pidfile .runtime/scheduler.pid)"; then
  echo "[check] scheduler pid=$(read_pidfile .runtime/scheduler.pid)"
else
  echo "[check] scheduler not running"
  tail_log .runtime/scheduler.log
  status=1
fi

echo

echo "[done] frontend=http://${FRONTEND_HOST}:${FRONTEND_PORT} backend=http://${BACKEND_HOST}:${BACKEND_PORT}"
exit "$status"
