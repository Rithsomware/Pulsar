#!/usr/bin/env bash

# PULSAR Judges Showcase
#
# Purpose:
# - Start backend + dashboard
# - Inject data into backend in timed stages
# - Let judges watch live processing on website
#
# Usage:
#   ./showcase_judges.sh
#   ./showcase_judges.sh --auto

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_DIR/venv"
PYTHON_BIN="$VENV_DIR/bin/python"
PORT_FILE="$PROJECT_DIR/.pulsar_webapp_port"
PORT=""
BASE_URL=""
AUTO_MODE="false"
SERVER_PID=""
SERVER_STARTED_BY_SCRIPT="false"
SHOWCASE_CONFIG="$PROJECT_DIR/.showcase_pulsar.yaml"
SHOWCASE_DB="$PROJECT_DIR/.showcase.db"
TORCH_AVAILABLE="unknown"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

print_header() {
  echo -e "\n${CYAN}==============================================================${NC}"
  echo -e "${CYAN}${BOLD}$1${NC}"
  echo -e "${CYAN}==============================================================${NC}\n"
}

print_step() { echo -e "${BLUE}> $1${NC}"; }
print_ok() { echo -e "${GREEN}[OK]${NC} $1"; }
print_info() { echo -e "${YELLOW}[INFO]${NC} $1"; }
print_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

wait_for_enter() {
  if [[ "$AUTO_MODE" == "true" ]]; then
    sleep 1
    return
  fi
  echo
  read -r -p "Press Enter to continue... "
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    print_error "Missing required command: $1"
    exit 1
  fi
}

setup_environment() {
  require_cmd python3
  require_cmd curl

  if [[ ! -x "$PYTHON_BIN" ]]; then
    print_info "Creating virtual environment"
    python3 -m venv --system-site-packages "$VENV_DIR"
  fi

  print_info "Installing dependencies"
  "$PYTHON_BIN" -m pip install -q --upgrade pip setuptools wheel
  "$PYTHON_BIN" -m pip install -q -e "$PROJECT_DIR"

  # Kill any stale GPU worker processes from previous runs
  if pgrep -f "pulsar.gpu_worker" >/dev/null 2>&1; then
    print_info "Killing stale GPU worker processes"
    pkill -f "pulsar.gpu_worker" >/dev/null 2>&1 || true
    sleep 1
  fi

  # Clean stale showcase database to prevent ghost-state recovery
  if [[ -f "$SHOWCASE_DB" ]]; then
    print_info "Removing old showcase database"
    rm -f "$SHOWCASE_DB"
  fi

  # Clear .pyc cache to prevent stale bytecode crashes
  print_info "Clearing Python bytecode cache"
  find "$PROJECT_DIR/src" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
  find "$PROJECT_DIR/src" -name "*.pyc" -delete 2>/dev/null || true

  # Verify GPU worker module is importable
  if ! "$PYTHON_BIN" -c "import pulsar.gpu_worker" >/dev/null 2>&1; then
    print_error "pulsar.gpu_worker module failed to import — check installation"
    "$PYTHON_BIN" -c "import pulsar.gpu_worker" 2>&1 || true
    exit 1
  fi

  print_ok "Environment ready (worker verified)"
}

run_pulsar_cli() {
  "$PYTHON_BIN" -m pulsar.cli "$@" --url "$BASE_URL"
}

detect_torch() {
  if "$PYTHON_BIN" -c "import torch" >/dev/null 2>&1; then
    TORCH_AVAILABLE="true"
  else
    TORCH_AVAILABLE="false"
  fi
}

resolve_port() {
  if [[ -n "${PULSAR_PORT:-}" ]]; then
    PORT="$PULSAR_PORT"
    print_info "Using port from PULSAR_PORT: $PORT"
  elif [[ -f "$PORT_FILE" ]]; then
    PORT="$(tr -d '[:space:]' < "$PORT_FILE")"
    if [[ -n "$PORT" ]]; then
      print_info "Using port from run_project.sh state file: $PORT"
    else
      PORT="8080"
      print_warn "Port file was empty. Falling back to default port $PORT."
    fi
  else
    PORT="8080"
    print_info "No port state file found. Using default port $PORT."
  fi

  BASE_URL="http://localhost:${PORT}"
}

check_port_free() {
  if command -v lsof >/dev/null 2>&1; then
    if lsof -iTCP:"$PORT" -sTCP:LISTEN -Pn >/dev/null 2>&1; then
      print_error "Port $PORT is already in use. Stop existing process or set PULSAR_PORT."
      exit 1
    fi
  fi
}

open_dashboard() {
  print_info "Open dashboard: ${BASE_URL}/"
  if command -v xdg-open >/dev/null 2>&1; then
    xdg-open "${BASE_URL}/" >/dev/null 2>&1 &
  elif command -v open >/dev/null 2>&1; then
    open "${BASE_URL}/" >/dev/null 2>&1 &
  fi
}

prepare_showcase_config() {
  rm -f "$SHOWCASE_DB"

  cat > "$SHOWCASE_CONFIG" <<CFG
cluster:
  total_gpus: 16
  gpu_memory_gb: 6
  gpu_memory_mb: 6144
  gpu_resource_name: nvidia.com/gpu
  dgpu_resource_name: nvidia.com/gpu
  igpu_resource_name: gpu.intel.com/i915
  nodes:
    - showcase-node-1
    - showcase-node-2

scheduling:
  policy: fair_share
  scheduling_interval_seconds: 2
  preemption:
    enabled: true
    grace_period_seconds: 30
    max_preemptions_per_cycle: 3
  fallback:
    preferred_gpu_class: dgpu
    fallback_gpu_class: igpu
    max_dgpu_wait_seconds: 0
  queue_controller:
    aging_enabled: true
    aging_boost_interval_seconds: 30.0
    starvation_threshold_seconds: 60.0

quotas:
  team-alpha:
    max_gpus: 16
    max_jobs: 20
    weight: 1.0
  team-beta:
    max_gpus: 16
    max_jobs: 20
    weight: 1.0
  team-gamma:
    max_gpus: 16
    max_jobs: 20
    weight: 0.5

api:
  host: 0.0.0.0
  port: ${PORT}

persistence:
  database: ${SHOWCASE_DB}
  enabled: true

logging:
  level: INFO
  format: text
CFG

  print_ok "Fresh showcase config prepared: $SHOWCASE_CONFIG"
}

start_server() {
  check_port_free
  prepare_showcase_config

  print_step "Starting PULSAR backend for showcase"
  cd "$PROJECT_DIR"
  "$PYTHON_BIN" -m pulsar.cli server --config "$SHOWCASE_CONFIG" > "$PROJECT_DIR/.showcase_server.log" 2>&1 &
  SERVER_PID=$!
  SERVER_STARTED_BY_SCRIPT="true"

  print_info "Waiting for readiness"
  local retries=50
  local attempt=1
  while [[ $attempt -le $retries ]]; do
    if curl -fsS "${BASE_URL}/readyz" >/dev/null 2>&1; then
      print_ok "Backend is ready"
      return
    fi
    sleep 1
    attempt=$((attempt + 1))
  done

  print_error "Server did not become ready. Last logs:"
  tail -n 40 "$PROJECT_DIR/.showcase_server.log" || true
  exit 1
}

server_ready() {
  curl -fsS "${BASE_URL}/readyz" >/dev/null 2>&1
}

ensure_server() {
  if server_ready; then
    print_ok "Detected already running backend at ${BASE_URL} (reusing it)"
    return
  fi
  start_server
}

configure_showcase_quotas() {
  print_step "Configuring non-restrictive showcase quotas"
  local teams=("team-alpha" "team-beta" "team-gamma")
  local weights=("1.0" "1.0" "0.5")
  local i=0
  local failed=0

  for team in "${teams[@]}"; do
    local weight="${weights[$i]}"
    print_info "Setting quota for $team: max_gpus=16, max_jobs=20, weight=$weight"
    response=$(curl -fS -X PUT "${BASE_URL}/api/v1/quotas/${team}" \
      -H "Content-Type: application/json" \
      -d "{\"max_gpus\":16,\"max_jobs\":20,\"weight\":${weight}}" 2>&1)

    if [[ $? -ne 0 ]]; then
      print_error "Failed to set quota for $team: $response"
      failed=$((failed + 1))
    else
      print_ok "Quota configured for $team"
    fi
    i=$((i + 1))
  done

  if [[ $failed -gt 0 ]]; then
    print_warn "$failed quotas failed to configure — jobs may be rejected!"
  fi

  print_info "Verifying quotas were applied..."
  sleep 1
  response=$(curl -fS "${BASE_URL}/api/v1/quotas" 2>&1)
  if [[ $? -eq 0 ]]; then
    print_ok "Quotas endpoint accessible"
  else
    print_warn "Could not verify quotas: $response"
  fi
  print_ok "Showcase quota configuration complete"
}

cleanup() {
  if [[ "$SERVER_STARTED_BY_SCRIPT" == "true" ]] && [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}

show_intro() {
  print_header "Live Website Showcase Flow"
  cat <<TEXT
1) Web app starts and dashboard opens
2) Script injects backend jobs in timed waves
3) Judges watch queue/running/fairness/metrics update live on website
TEXT

  if [[ "$TORCH_AVAILABLE" == "false" ]]; then
    print_warn "torch is not installed (DNS blocked pypi.org earlier)."
    print_info "Jobs may transition quickly to FAILED, but backend processing and dashboard updates are still visible."
  else
    print_ok "torch detected: real worker execution path available"
  fi

  wait_for_enter
}

submit_job() {
  local user="$1"
  local gpus="$2"
  local priority="$3"
  local workload="$4"
  local framework="$5"

  run_pulsar_cli submit --user "$user" --gpus "$gpus" --priority "$priority" --type "$workload" --framework "$framework"
}

inject_wave_one() {
  print_header "Wave 1: Baseline Workload"
  print_step "Submitting jobs - watch Running Jobs and Queue sections on dashboard"

  submit_job team-alpha 2 NORMAL Training PyTorch
  sleep 2
  submit_job team-beta 2 NORMAL Inference Triton
  sleep 2
  submit_job team-gamma 1 NORMAL Training JAX

  print_ok "Wave 1 submitted"
  print_info "Watching live updates for 8 seconds"
  sleep 8
}

inject_wave_two() {
  print_header "Wave 2: Queue Pressure"
  print_step "Submitting heavier requests to stress fairness + queue behavior"

  submit_job team-alpha 2 HIGH FineTuning PyTorch
  sleep 2
  submit_job team-beta 2 HIGH Training TensorFlow
  sleep 2
  submit_job team-gamma 1 NORMAL Inference Triton

  print_ok "Wave 2 submitted"
  print_info "Watching live updates for 10 seconds"
  sleep 10
}

inject_wave_three() {
  print_header "Wave 3: Priority Spike"
  print_step "Submitting CRITICAL job so judges can see priority impact"

  submit_job team-beta 2 CRITICAL Training PyTorch

  print_ok "Critical workload submitted"
  print_info "Watching live updates for 12 seconds"
  sleep 12
}

show_terminal_snapshot() {
  print_header "Terminal Snapshot (for narration)"

  print_step "Jobs"
  run_pulsar_cli jobs
  echo

  print_step "Cluster"
  run_pulsar_cli cluster
  echo

  print_step "Fairness"
  run_pulsar_cli fairness
  echo

  print_step "Metrics sample"
  curl -fsS "${BASE_URL}/api/v1/metrics" | sed -n '1,20p'
}

show_judge_notes() {
  print_header "Judge Narrative"
  cat <<TEXT
- Data was injected into backend in staged waves.
- Live dashboard reflected backend processing in near real-time.
- Queue depth, running jobs, fairness, and metrics all changed live.
- This is a real control-plane flow, not static UI playback.
TEXT

  print_info "Useful links"
  echo "Dashboard: ${BASE_URL}/"
  echo "API docs:  ${BASE_URL}/docs"
  echo "Health:    ${BASE_URL}/healthz"
  echo "Ready:     ${BASE_URL}/readyz"
  echo "Metrics:   ${BASE_URL}/api/v1/metrics"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --auto)
        AUTO_MODE="true"
        ;;
      -h|--help)
        cat <<USAGE
Usage: $0 [--auto]

Options:
  --auto   Run with minimal pauses
USAGE
        exit 0
        ;;
      *)
        print_error "Unknown argument: $1"
        exit 1
        ;;
    esac
    shift
  done
}

main() {
  parse_args "$@"
  trap cleanup EXIT

  resolve_port
  setup_environment
  detect_torch
  show_intro
  ensure_server
  configure_showcase_quotas
  open_dashboard

  inject_wave_one
  wait_for_enter
  inject_wave_two
  wait_for_enter
  inject_wave_three
  wait_for_enter

  show_terminal_snapshot
  show_judge_notes

  print_ok "Showcase completed"
  print_info "Server log: $PROJECT_DIR/.showcase_server.log"
}

main "$@"
