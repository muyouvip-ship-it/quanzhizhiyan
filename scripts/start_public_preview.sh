#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"
BACKEND_PORT="${BACKEND_PORT:-8500}"

cd "$ROOT"

echo "[public-preview] root=$ROOT"
echo "[public-preview] backend will stay on 127.0.0.1:${BACKEND_PORT}"
echo "[public-preview] frontend will listen on 0.0.0.0:${FRONTEND_PORT}"
echo "[public-preview] suitable for temporary LAN / tunnel / router port-forward access"

BACKEND_HOST=127.0.0.1 FRONTEND_HOST=0.0.0.0 ./scripts/start_services.sh

echo
echo "[public-preview] open your browser at:"
echo "  http://127.0.0.1:${FRONTEND_PORT}"
echo
echo "[public-preview] for internet access, expose ONLY frontend port ${FRONTEND_PORT}"
echo "[public-preview] frontend will proxy /v1 and /api back to local backend ${BACKEND_PORT}"
