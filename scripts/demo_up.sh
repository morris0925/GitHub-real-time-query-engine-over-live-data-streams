#!/usr/bin/env bash
#
# scripts/demo_up.sh — One-command demo bring-up
#
# Starts every layer of the demo in the right order, with a readiness check
# between each step, so a live demo never starts against a half-booted
# pipeline (the "did I start everything?" 503 / empty-feed embarrassment).
#
# Order: Kafka (docker compose) -> producer (STREAM_MODE=repo) -> consumer
#        -> one-shot ci_fetch -> FastAPI service -> Next.js dev server
#
# Does NOT build the knowledge base (ingest.py / embeddings.py) — that's a
# slow, rate-limited, one-time step you run separately. This script warns
# if it's missing rather than trying to run it.
#
# Usage:
#   ./scripts/demo_up.sh
#   Ctrl+C stops every process this script started (Kafka keeps running —
#   stop it separately with `docker compose down` when you're done for the day).

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

LOG_DIR="$PROJECT_ROOT/logs/demo"
mkdir -p "$LOG_DIR"

API_HOST="${API_HOST:-127.0.0.1}"
API_PORT="${API_PORT:-8000}"
FRONTEND_PORT="${FRONTEND_PORT:-3000}"

PIDS=()

cleanup() {
    echo ""
    echo "[demo_up] Stopping processes started by this script..."
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" >/dev/null 2>&1 || true
    done
    echo "[demo_up] Kafka is still running — stop it with: docker compose down"
}
trap cleanup EXIT INT TERM

step() { echo -e "\n[demo_up] ── $1 ──"; }

LAST_PID=""

spawn() {
    # spawn <log-file> <command...> — runs in background, records the PID
    # in both PIDS[] (for cleanup) and LAST_PID (for require_alive right
    # after this call — macOS ships bash 3.2, which has no ${array[-1]}).
    local log_file="$1"; shift
    "$@" > "$log_file" 2>&1 &
    LAST_PID="$!"
    PIDS+=("$LAST_PID")
}

require_alive() {
    # require_alive <pid> <label> <log-file> — fail fast if it already died.
    sleep 2
    if ! kill -0 "$1" 2>/dev/null; then
        echo "[demo_up] $2 exited immediately — see $3"
        exit 1
    fi
}

wait_for_kafka_healthy() {
    local timeout=90 waited=0 cid status
    step "Waiting for Kafka to become healthy"
    cid=$(docker compose ps -q kafka)
    while true; do
        status=$(docker inspect --format='{{.State.Health.Status}}' "$cid" 2>/dev/null || echo "unknown")
        [ "$status" = "healthy" ] && break
        sleep 2
        waited=$((waited + 2))
        if [ "$waited" -ge "$timeout" ]; then
            echo "[demo_up] TIMEOUT waiting for Kafka (status=$status) after ${timeout}s"
            echo "[demo_up] Check: docker compose logs kafka"
            exit 1
        fi
    done
    echo "[demo_up] Kafka is healthy (${waited}s)"
}

wait_for_http() {
    local url="$1" label="$2" timeout="${3:-60}" waited=0
    step "Waiting for $label ($url)"
    until curl -sf "$url" >/dev/null 2>&1; do
        sleep 1
        waited=$((waited + 1))
        if [ "$waited" -ge "$timeout" ]; then
            echo "[demo_up] TIMEOUT waiting for $label after ${timeout}s"
            exit 1
        fi
    done
    echo "[demo_up] $label is up (${waited}s)"
}

wait_for_tcp() {
    local host="$1" port="$2" label="$3" timeout="${4:-60}" waited=0
    step "Waiting for $label ($host:$port)"
    until (exec 3<>"/dev/tcp/$host/$port") >/dev/null 2>&1; do
        sleep 1
        waited=$((waited + 1))
        if [ "$waited" -ge "$timeout" ]; then
            echo "[demo_up] TIMEOUT waiting for $label after ${timeout}s"
            exit 1
        fi
    done
    exec 3>&- 3<&- 2>/dev/null || true
    echo "[demo_up] $label is up (${waited}s)"
}

# ── 0. Pre-flight ───────────────────────────────────────────────────────────

step "Pre-flight checks"
if [ ! -f .env ]; then
    echo "[demo_up] WARNING: .env not found. Copy .env.example -> .env and fill in"
    echo "          ANTHROPIC_API_KEY / VOYAGE_API_KEY, or /health and /diagnose"
    echo "          will fail loudly (by design — no synthetic fallback)."
fi
if [ ! -f data/knowledge/kb.parquet ] || [ ! -f data/knowledge/kb_embeddings.parquet ]; then
    echo "[demo_up] WARNING: knowledge base not built yet. Run first:"
    echo "    PYTHONPATH=src python src/knowledge/ingest.py"
    echo "    PYTHONPATH=src python src/knowledge/embeddings.py"
fi

# ── 1. Kafka ─────────────────────────────────────────────────────────────────

step "Starting Kafka (docker compose)"
docker compose up -d
wait_for_kafka_healthy

# ── 2. Producer ──────────────────────────────────────────────────────────────

step "Starting producer (STREAM_MODE=repo)"
spawn "$LOG_DIR/producer.log" env STREAM_MODE=repo PYTHONPATH=src python src/producer.py
require_alive "$LAST_PID" "Producer" "$LOG_DIR/producer.log"

# ── 3. Consumer ──────────────────────────────────────────────────────────────

step "Starting consumer"
spawn "$LOG_DIR/consumer.log" env PYTHONPATH=src python src/consumer.py
require_alive "$LAST_PID" "Consumer" "$LOG_DIR/consumer.log"

# ── 4. CI fetch (one-shot, not backgrounded) ──────────────────────────────────

step "Fetching CI runs (one-shot)"
PYTHONPATH=src python src/anomaly/ci_fetch.py 2>&1 | tee "$LOG_DIR/ci_fetch.log"

# ── 5. FastAPI service ────────────────────────────────────────────────────────

step "Starting FastAPI service"
spawn "$LOG_DIR/api.log" env PYTHONPATH=src uvicorn api.main:app --host "$API_HOST" --port "$API_PORT"
wait_for_http "http://$API_HOST:$API_PORT/health" "FastAPI /health" 60

# ── 6. Next.js dev server ─────────────────────────────────────────────────────

step "Starting Next.js dev server"
spawn "$LOG_DIR/frontend.log" npm run dev --prefix frontend
wait_for_tcp localhost "$FRONTEND_PORT" "Next.js dev server" 60

# ── Ready ──────────────────────────────────────────────────────────────────

step "Demo is up"
cat <<EOF
  AI dashboard:        http://localhost:$FRONTEND_PORT
  API:                 http://$API_HOST:$API_PORT
  API health:          http://$API_HOST:$API_PORT/health
  Terminal dashboard:  PYTHONPATH=src python src/dashboard/dashboard.py
  Logs:                $LOG_DIR/

Press Ctrl+C to stop producer / consumer / API / frontend.
EOF

wait
