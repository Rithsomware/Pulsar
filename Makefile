# PULSAR - GPU Queue & Fairness Control Plane
# Built on KGWE (Kubernetes GPU Workload Enhancer)
# Makefile for building, testing, and deploying

# Version and build info
VERSION ?= 1.0.0
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE := $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
LDFLAGS := -X main.Version=$(VERSION) -X main.GitCommit=$(GIT_COMMIT) -X main.BuildDate=$(BUILD_DATE)

# Go parameters
GOCMD := go
GOBUILD := $(GOCMD) build
GOTEST := $(GOCMD) test
GOVET := $(GOCMD) vet
GOMOD := $(GOCMD) mod
GOFMT := gofmt

# Docker parameters
DOCKER := docker
REGISTRY ?= nvidia
IMAGE_PREFIX := $(REGISTRY)/kgwe

# Kubernetes parameters
KUBECTL := kubectl
HELM := helm
NAMESPACE ?= kgwe-system

# Directories
SRC_DIR := ./src
BIN_DIR := ./bin
DEPLOY_DIR := ./deploy

# Components
COMPONENTS := controller scheduler discovery optimizer mig-controller cost-engine exporter agent

.PHONY: all build test clean deploy docker help

## Default target
all: build

## Build all Go components
build: $(COMPONENTS)

controller:
	@echo "Building controller..."
	$(GOBUILD) -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/kgwe-controller ./cmd/controller

scheduler:
	@echo "Building scheduler..."
	$(GOBUILD) -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/kgwe-scheduler ./cmd/scheduler

discovery:
	@echo "Building discovery service..."
	$(GOBUILD) -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/kgwe-discovery ./cmd/discovery

mig-controller:
	@echo "Building MIG controller..."
	$(GOBUILD) -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/kgwe-mig-controller ./cmd/mig-controller

cost-engine:
	@echo "Building cost engine..."
	$(GOBUILD) -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/kgwe-cost-engine ./cmd/cost-engine

exporter:
	@echo "Building Prometheus exporter..."
	$(GOBUILD) -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/kgwe-exporter ./cmd/exporter

agent:
	@echo "Building node agent..."
	$(GOBUILD) -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/kgwe-agent ./cmd/agent

optimizer:
	@echo "Building optimizer (Python)..."
	@echo "Optimizer is a Python component - see src/optimizer/"

## Run all tests
test: test-go test-python

test-go:
	@echo "Running Go tests..."
	$(GOTEST) -v -race -coverprofile=coverage.out ./...

test-python:
	@echo "Running Python tests..."
	python -m pytest tests/ -v

## Run linting
lint: lint-go lint-python

lint-go:
	@echo "Linting Go code..."
	$(GOVET) ./...
	$(GOFMT) -d -s .
	golangci-lint run

lint-python:
	@echo "Linting Python code..."
	cd src/optimizer && ruff check . && mypy .

## Format code
fmt:
	@echo "Formatting Go code..."
	$(GOFMT) -w .
	@echo "Formatting Python code..."
	cd src/optimizer && ruff format .

## Generate code (CRDs, deepcopy, etc.)
generate:
	@echo "Generating code..."
	controller-gen object:headerFile="hack/boilerplate.go.txt" paths="./..."
	controller-gen crd:trivialVersions=true rbac:roleName=kgwe-controller webhook paths="./..." output:crd:artifacts:config=deploy/helm/kgwe/crds

## Build Docker images
docker: docker-build docker-push

docker-build:
	@echo "Building Docker images..."
	@for component in $(COMPONENTS); do \
		echo "Building $(IMAGE_PREFIX)-$$component:$(VERSION)..."; \
		$(DOCKER) build -t $(IMAGE_PREFIX)-$$component:$(VERSION) \
			--build-arg VERSION=$(VERSION) \
			--build-arg GIT_COMMIT=$(GIT_COMMIT) \
			-f docker/Dockerfile.$$component .; \
	done

docker-push:
	@echo "Pushing Docker images..."
	@for component in $(COMPONENTS); do \
		echo "Pushing $(IMAGE_PREFIX)-$$component:$(VERSION)..."; \
		$(DOCKER) push $(IMAGE_PREFIX)-$$component:$(VERSION); \
	done

## Deploy to Kubernetes
deploy: deploy-crds deploy-helm

deploy-crds:
	@echo "Installing CRDs..."
	$(KUBECTL) apply -f $(DEPLOY_DIR)/helm/kgwe/crds/

deploy-helm:
	@echo "Deploying KGWE via Helm..."
	$(HELM) upgrade --install kgwe $(DEPLOY_DIR)/helm/kgwe \
		--namespace $(NAMESPACE) \
		--create-namespace \
		--set image.tag=$(VERSION)

deploy-monitoring:
	@echo "Deploying monitoring stack..."
	$(HELM) upgrade --install kgwe $(DEPLOY_DIR)/helm/kgwe \
		--namespace $(NAMESPACE) \
		--set prometheus.enabled=true \
		--set grafana.enabled=true

## Uninstall from Kubernetes
uninstall:
	@echo "Uninstalling KGWE..."
	$(HELM) uninstall kgwe --namespace $(NAMESPACE) || true
	$(KUBECTL) delete -f $(DEPLOY_DIR)/helm/kgwe/crds/ || true

## Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf $(BIN_DIR)
	rm -f coverage.out
	rm -rf dist/

## Download dependencies
deps:
	@echo "Downloading Go dependencies..."
	$(GOMOD) download
	$(GOMOD) tidy
	@echo "Installing Python dependencies..."
	pip install -r src/optimizer/requirements.txt

## Run locally for development
run-controller:
	$(GOCMD) run ./cmd/controller/main.go --kubeconfig=$(HOME)/.kube/config

run-scheduler:
	$(GOCMD) run ./cmd/scheduler/main.go --kubeconfig=$(HOME)/.kube/config

run-optimizer:
	cd src/optimizer && python -m workload_optimizer

## Integration tests (requires running cluster)
integration-test:
	@echo "Running integration tests..."
	$(GOTEST) -v -tags=integration ./tests/integration/...

## E2E tests (requires GPU cluster)
e2e-test:
	@echo "Running E2E tests..."
	$(GOTEST) -v -tags=e2e ./tests/e2e/...

## Generate documentation
docs:
	@echo "Generating documentation..."
	go doc -all ./... > docs/api-reference.md


## ─── PULSAR Control Plane ─────────────────────────────────

## Install PULSAR dependencies
pulsar-deps:
	@echo "Installing PULSAR dependencies..."
	pip install -r src/pulsar/requirements.txt

## Run PULSAR API server
run-pulsar:
	@echo "Starting PULSAR Control Plane..."
	cd src && python -m pulsar.cli server

## Run PULSAR demo
pulsar-demo:
	@echo "Running PULSAR demo..."
	cd src && python -m pulsar.demo

## Run PULSAR tests
pulsar-test:
	@echo "Running PULSAR tests..."
	python -m pytest tests/test_pulsar.py -v --tb=short

## Build PULSAR Docker image
docker-pulsar:
	@echo "Building PULSAR Docker image..."
	$(DOCKER) build -t $(REGISTRY)/pulsar-control-plane:$(VERSION) \
		-f docker/Dockerfile.pulsar .

## Show help
help:
	@echo "PULSAR — GPU Queue & Fairness Control Plane"
	@echo "Built on KGWE (Kubernetes GPU Workload Enhancer)"
	@echo ""
	@echo "Usage:"
	@echo "  make <target>"
	@echo ""
	@echo "PULSAR Targets:"
	@echo "  pulsar-deps      Install PULSAR Python dependencies"
	@echo "  run-pulsar       Start the PULSAR API server"
	@echo "  pulsar-demo      Run the built-in demo"
	@echo "  pulsar-test      Run PULSAR unit tests"
	@echo "  docker-pulsar    Build PULSAR Docker image"
	@echo ""
	@echo "KGWE Targets:"
	@echo "  all              Build all components (default)"
	@echo "  build            Build all Go components"
	@echo "  test             Run all tests"
	@echo "  lint             Run linters"
	@echo "  fmt              Format code"
	@echo "  generate         Generate CRDs and deepcopy functions"
	@echo "  docker           Build and push Docker images"
	@echo "  deploy           Deploy to Kubernetes"
	@echo "  uninstall        Remove from Kubernetes"
	@echo "  clean            Clean build artifacts"
	@echo "  dep s             Download dependencies"
	@echo ""
	@echo "Variables:"
	@echo "  VERSION          Version tag (default: $(VERSION))"
	@echo "  REGISTRY         Docker registry (default: $(REGISTRY))"
	@echo "  NAMESPACE        Kubernetes namespace (default: $(NAMESPACE))"

