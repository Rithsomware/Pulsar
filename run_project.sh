#!/bin/bash

##############################################################################
# PULSAR Project Runner
#
# Runs the PULSAR GPU Queue & Fairness Control Plane locally
#
# Usage:
#   ./run_project.sh                 # Start API server
#   ./run_project.sh demo            # Run demo
#   ./run_project.sh launcher        # Run job launcher example
##############################################################################

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_DIR/venv"
SRC_DIR="$PROJECT_DIR/src"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print section headers
print_header() {
    echo -e "\n${BLUE}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}\n"
}

# Function to print success messages
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

# Function to print info messages
print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

print_header "PULSAR — GPU Queue & Fairness Control Plane"

# Check if virtual environment exists
if [ ! -d "$VENV_DIR" ]; then
    print_info "Creating Python virtual environment..."
    python3 -m venv "$VENV_DIR"
    print_success "Virtual environment created"
fi

# Activate virtual environment
print_info "Activating virtual environment..."
source "$VENV_DIR/bin/activate"
print_success "Virtual environment activated"

# Install dependencies
print_info "Installing dependencies..."
pip install -q -e "$SRC_DIR"
print_success "Dependencies installed"

# Determine which command to run
COMMAND="${1:-server}"

case "$COMMAND" in
    server)
        print_header "Starting PULSAR API Server"
        echo "Available endpoints:"
        echo "  • Dashboard:     http://localhost:8080/"
        echo "  • API Docs:      http://localhost:8080/docs"
        echo "  • Health Check:  http://localhost:8080/health"
        echo ""
        echo "Quick CLI commands (in another terminal):"
        echo "  • pulsar cluster              # Show cluster status"
        echo "  • pulsar submit --user alice --gpus 2  # Submit job"
        echo "  • pulsar jobs                 # List jobs"
        echo "  • pulsar fairness             # Show fairness metrics"
        echo ""
        print_info "Press Ctrl+C to stop the server"
        echo ""
        cd "$SRC_DIR"
        python -m pulsar.cli server
        ;;

    demo)
        print_header "Running PULSAR Demo"
        echo "This demo shows the full scheduling pipeline:"
        echo "  1. Config (16 GPUs, fair-share policy)"
        echo "  2. Submit jobs from multiple teams"
        echo "  3. Fair-share scheduling"
        echo "  4. Preemption of low-priority jobs"
        echo "  5. Rescheduling and metrics"
        echo ""
        cd "$SRC_DIR"
        python -m pulsar.demo
        ;;

    launcher)
        print_header "Running Job Launcher Example"
        echo "This example launches real GPU processes visible in nvidia-smi"
        echo ""
        echo "In another terminal, run:"
        echo "  watch -n 1 nvidia-smi"
        echo ""
        echo "You will see real GPU processes with PIDs and VRAM usage."
        echo ""
        cd "$SRC_DIR"
        python -m pulsar.example_launch_jobs
        ;;

    test)
        print_header "Running Tests"
        cd "$PROJECT_DIR"
        pytest tests/ -v
        ;;

    *)
        echo "Usage: $0 {server|demo|launcher|test}"
        echo ""
        echo "Commands:"
        echo "  server     Start the PULSAR API server (default)"
        echo "  demo       Run the scheduling pipeline demo"
        echo "  launcher   Launch real GPU jobs (requires GPU)"
        echo "  test       Run test suite"
        exit 1
        ;;
esac
