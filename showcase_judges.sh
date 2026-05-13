#!/bin/bash

##############################################################################
# PULSAR Judges Showcase
#
# Interactive demonstration of PULSAR's key features:
#  • Real GPU scheduling
#  • Fair-share allocation
#  • Per-team quotas
#  • Preemption
#  • Live monitoring dashboard
#
# Usage:
#   ./showcase_judges.sh
##############################################################################

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_DIR/venv"
SRC_DIR="$PROJECT_DIR/src"

# Colors for output
BOLD='\033[1m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print section headers
print_header() {
    echo -e "\n${CYAN}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║${NC} ${BOLD}$1${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════════════╝${NC}\n"
}

print_subheader() {
    echo -e "\n${BLUE}▶ $1${NC}\n"
}

# Function to print success messages
print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

# Function to print info messages
print_info() {
    echo -e "${YELLOW}ℹ${NC} $1"
}

# Function to pause and wait for user input
pause_for_input() {
    echo ""
    echo -e "${YELLOW}Press Enter to continue...${NC}"
    read -r
}

# Setup virtual environment
setup_environment() {
    if [ ! -d "$VENV_DIR" ]; then
        print_info "Creating Python virtual environment..."
        python3 -m venv "$VENV_DIR"
    fi

    source "$VENV_DIR/bin/activate"
    pip install -q -e "$SRC_DIR" 2>/dev/null
}

# Demo 1: Show project overview
demo_overview() {
    print_header "PULSAR — GPU Queue & Fairness Control Plane"

    cat << 'EOF'
PULSAR is a GPU resource management system that:

  📦  QUEUES GPU workloads with priority ordering
  ⚖️  ENFORCES fair-share allocation across teams
  🎯  SETS per-team quotas and resource limits
  ⏱️  PERFORMS intelligent job preemption
  🔴  SPAWNS REAL GPU processes (visible in nvidia-smi)
  📊  PROVIDES real-time monitoring & dashboards
  🚀  WORKS standalone or in Kubernetes clusters

KEY PRINCIPLE: Each scheduled job is a REAL OS process consuming actual GPU
memory — not a simulation. You can see jobs in nvidia-smi during execution.

EOF

    print_success "Features Overview"
    pause_for_input
}

# Demo 2: Run scheduling simulation
demo_scheduling() {
    print_header "DEMO 1: Fair-Share Scheduling Pipeline"

    cat << 'EOF'
Watch as PULSAR:
  1️⃣  Initializes a 16-GPU cluster
  2️⃣  Sets quotas for 3 teams (alpha, beta, gamma)
  3️⃣  Submits 8 jobs from different teams
  4️⃣  Schedules jobs fairly based on past usage
  5️⃣  Preempts low-priority jobs for critical work
  6️⃣  Tracks fairness index and per-team metrics

EOF

    print_info "Starting scheduling demo..."
    pause_for_input

    cd "$SRC_DIR"
    python -m pulsar.demo

    pause_for_input
}

# Demo 3: Show real-time API and metrics
demo_api_metrics() {
    print_header "DEMO 2: Real-Time API & Metrics"

    cat << 'EOF'
Starting PULSAR API server on http://localhost:8080

Available endpoints:
  🖥️  Dashboard:        http://localhost:8080/
  📖 API Docs:         http://localhost:8080/docs (Swagger UI)
  ❤️  Health Check:     http://localhost:8080/health
  📊 Metrics:           http://localhost:8080/metrics (Prometheus)

OPEN IN YOUR BROWSER:
  http://localhost:8080/

The dashboard shows:
  ✓ Cluster utilization & GPU availability
  ✓ Active jobs and their status
  ✓ Per-team resource usage
  ✓ Fairness index (Jain's fairness)
  ✓ Queue depth and job completion rates

EOF

    print_info "Server will run for 90 seconds to allow you to explore the dashboard"
    pause_for_input

    print_info "Starting API server..."
    cd "$SRC_DIR"

    # Start server in background
    timeout 90 python -m pulsar.cli server 2>/dev/null || true

    echo ""
    pause_for_input
}

# Demo 4: Show CLI capabilities
demo_cli() {
    print_header "DEMO 3: Command-Line Interface"

    cat << 'EOF'
PULSAR provides a full-featured CLI for managing GPU jobs:

Command Syntax & Examples:

  pulsar server                          Start the API server
  pulsar submit --user TEAM --gpus N     Submit a job to queue
  pulsar jobs [--user TEAM]              List all jobs
  pulsar status <job-id>                 Get job details
  pulsar cancel <job-id>                 Cancel a job
  pulsar cluster                         Show cluster status
  pulsar fairness                        Show fairness metrics
  pulsar quotas                          List per-team quotas

PRACTICAL WORKFLOW:
  1. Start server:        pulsar server
  2. In another terminal:
     - Submit jobs:       pulsar submit --user alice --gpus 2
     - Check status:      pulsar jobs
     - View fairness:     pulsar fairness
     - Check cluster:     pulsar cluster

EOF

    print_success "CLI Overview"
    pause_for_input
}

# Demo 5: Show architecture
demo_architecture() {
    print_header "DEMO 4: System Architecture"

    cat << 'EOF'
PULSAR Architecture:

    ┌─────────────────────────────────────────────────────┐
    │                 Job Submission                       │
    │         (CLI / API / Dashboard / K8s API)            │
    └──────────────────┬──────────────────────────────────┘
                       │
    ┌──────────────────▼──────────────────────────────────┐
    │              QUEUE MANAGER                           │
    │  Stores jobs, enforces per-team quotas, priorities   │
    └──────────────────┬──────────────────────────────────┘
                       │
    ┌──────────────────▼──────────────────────────────────┐
    │              SCHEDULER (Pluggable)                   │
    │  • Fair-Share    — Balance team usage                │
    │  • FIFO          — First-come, first-served         │
    │  • Priority      — Custom job priority              │
    │  • Backfill      — Fill unused slots intelligently   │
    └──────────────────┬──────────────────────────────────┘
                       │
    ┌──────────────────▼──────────────────────────────────┐
    │           ADMISSION & PREEMPTION                     │
    │  Check quotas, evict low-priority jobs if needed     │
    └──────────────────┬──────────────────────────────────┘
                       │
    ┌──────────────────▼──────────────────────────────────┐
    │              EXECUTOR                               │
    │  Spawn real subprocess → visible in nvidia-smi       │
    │  Track PID → GPU mapping, memory usage               │
    └──────────────────┬──────────────────────────────────┘
                       │
    ┌──────────────────▼──────────────────────────────────┐
    │         MONITORING & METRICS                         │
    │  • Prometheus metrics (/metrics)                     │
    │  • Dashboard (web UI)                                │
    │  • Fairness tracking (Jain's index)                  │
    │  • Job history (persistent SQLite)                   │
    └──────────────────────────────────────────────────────┘

KEY STRENGTH: Real execution with PID mapping & OS-level process tracking
             (Not a simulator — actual GPU memory consumption)

EOF

    print_success "Architecture Overview"
    pause_for_input
}

# Demo 6: Key differentiators
demo_differentiators() {
    print_header "DEMO 5: Why PULSAR is Different"

    cat << 'EOF'
Comparison with other solutions:

                    PULSAR      Kubernetes    Slurm      Custom
─────────────────────────────────────────────────────────────────────
Real GPU Execution   ✅ YES      ❌ Pods       ✅ YES      Varies
Fair-Share Schedule  ✅ YES      ❌ No         ✅ YES      Complex
Per-Team Quotas      ✅ YES      ✅ Yes        ✅ YES      ✅ Yes
Smart Preemption     ✅ YES      ⚠️  Limited   ✅ YES      Complex
PID Tracking         ✅ YES      ❌ No         ✅ YES      Hard
Lightweight Setup    ✅ YES      ❌ Heavy      ⚠️  Medium  ✅ Yes
Standalone Mode      ✅ YES      ❌ No         ❌ No       ✅ Yes
K8s Integration      ✅ YES      ✅ Native     ✅ YES      Varies
─────────────────────────────────────────────────────────────────────

🎯 PULSAR's UNIQUE ADVANTAGE:
   Combines simplicity of standalone GPU scheduling with
   the power of Kubernetes integration — choose your deployment!

EOF

    print_success "Key Differentiators"
    pause_for_input
}

# Main showcase flow
main() {
    print_info "Setting up environment..."
    setup_environment
    print_success "Environment ready"

    # Run demos in sequence
    demo_overview
    demo_scheduling
    demo_api_metrics
    demo_cli
    demo_architecture
    demo_differentiators

    # Final summary
    print_header "Showcase Complete!"

    cat << 'EOF'
To continue exploring PULSAR:

START THE SERVER:
  cd /home/rithsomware/Downloads/Pulsar
  source venv/bin/activate
  pulsar server

THEN IN ANOTHER TERMINAL:
  pulsar submit --user alice --gpus 2
  pulsar jobs
  pulsar cluster
  pulsar fairness

DOCUMENTATION:
  README.md              — Full feature documentation
  docs/                  — Architecture diagrams
  src/pulsar/            — Source code with docstrings
  tests/                 — Test examples

EOF

    print_success "Thank you for exploring PULSAR!"
}

# Run the showcase
main
