#!/usr/bin/env bash

# PULSAR Web App Runner
# Starts the backend API + website dashboard.
#
# Usage:
#   ./run_project.sh          # Start web app (default)
#   ./run_project.sh start    # Start web app
#   ./run_project.sh setup    # Only prepare environment

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_DIR/venv"
PYTHON_BIN="$VENV_DIR/bin/python"
PORT="${PULSAR_PORT:-8080}"
PORT_FILE="$PROJECT_DIR/.pulsar_webapp_port"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

print_header() {
  echo -e "\n${BLUE}==============================================================${NC}"
  echo -e "${BLUE}$1${NC}"
  echo -e "${BLUE}==============================================================${NC}\n"
}

print_ok() { echo -e "${GREEN}[OK]${NC} $1"; }
print_info() { echo -e "${YELLOW}[INFO]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    print_error "Missing required command: $1"
    exit 1
  fi
}

setup_env() {
  require_cmd python3

  if [[ ! -x "$PYTHON_BIN" ]]; then
    print_info "Creating virtual environment at $VENV_DIR"
    python3 -m venv "$VENV_DIR"
  fi

  print_info "Installing/updating dependencies"
  "$PYTHON_BIN" -m pip install -q --upgrade pip setuptools wheel
  "$PYTHON_BIN" -m pip install -q -e "$PROJECT_DIR"

  print_ok "Environment is ready"
}

check_port_free() {
  if command -v lsof >/dev/null 2>&1; then
    if lsof -iTCP:"$PORT" -sTCP:LISTEN -Pn >/dev/null 2>&1; then
      print_error "Port $PORT is already in use. Stop it or set PULSAR_PORT to a free port."
      exit 1
    fi
  fi
}

start_webapp() {
  setup_env
  check_port_free

  # Record the active webapp port so showcase_judges.sh can reuse it.
  echo "$PORT" > "$PORT_FILE"

  print_header "Starting PULSAR Web App"
  echo "Dashboard:  http://localhost:${PORT}/"
  echo "API Docs:   http://localhost:${PORT}/docs"
  echo "Health:     http://localhost:${PORT}/healthz"
  echo "Readiness:  http://localhost:${PORT}/readyz"
  echo "Metrics:    http://localhost:${PORT}/api/v1/metrics"
  echo "Port file:  $PORT_FILE"
  echo
  echo "Press Ctrl+C to stop."

  cd "$PROJECT_DIR"
  "$PYTHON_BIN" -m pulsar.cli server
}

show_help() {
  cat <<USAGE
Usage: $0 [start|setup|help]

Commands:
  start   Start PULSAR web app (default)
  setup   Prepare venv and install dependencies only
  help    Show this help
USAGE
}

main() {
  local cmd="${1:-start}"
  case "$cmd" in
    start) start_webapp ;;
    setup) setup_env ;;
    help|-h|--help) show_help ;;
    *)
      print_error "Unknown command: $cmd"
      show_help
      exit 1
      ;;
  esac
}

main "$@"
