// Package monitoring provides observability components for GPU workload management.
// It includes Prometheus exporters, custom metrics, and integration with DCGM telemetry.
package monitoring

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/nvidia/kgwe/src/discovery"
)

// PrometheusExporter exports KGWE metrics for Prometheus scraping
type PrometheusExporter struct {
	mu sync.RWMutex

	// metrics stores current metric values
	metrics *KGWEMetrics

	// registry for metric registration
	registry MetricRegistry

	// discoveryService for GPU topology data
	discoveryService *discovery.DiscoveryService

	// config holds exporter configuration
	config ExporterConfig

	// stopChan for shutdown
	stopChan chan struct{}
}

// ExporterConfig holds exporter configuration
type ExporterConfig struct {
	// ListenAddress for HTTP server
	ListenAddress string

	// MetricsPath for Prometheus scraping
	MetricsPath string

	// CollectionInterval for metric updates
	CollectionInterval time.Duration

	// EnableDCGMMetrics enables DCGM integration
	EnableDCGMMetrics bool

	// EnableTopologyMetrics enables topology metrics
	EnableTopologyMetrics bool

	// EnableCostMetrics enables cost tracking metrics
	EnableCostMetrics bool
}

// DefaultExporterConfig returns sensible defaults
func DefaultExporterConfig() ExporterConfig {
	return ExporterConfig{
		ListenAddress:         ":9400",
		MetricsPath:           "/metrics",
		CollectionInterval:    15 * time.Second,
		EnableDCGMMetrics:     true,
		EnableTopologyMetrics: true,
		EnableCostMetrics:     true,
	}
}

// MetricRegistry interface for metric registration
type MetricRegistry interface {
	Register(metric Metric) error
	Unregister(metric Metric) bool
}

// Metric interface for exportable metrics
type Metric interface {
	Name() string
	Help() string
	Type() MetricType
	Value() interface{}
}

// MetricType categorizes metrics
type MetricType string

const (
	MetricTypeCounter   MetricType = "counter"
	MetricTypeGauge     MetricType = "gauge"
	MetricTypeHistogram MetricType = "histogram"
	MetricTypeSummary   MetricType = "summary"
)

// KGWEMetrics holds all KGWE-specific metrics
type KGWEMetrics struct {
	// Scheduler metrics
	SchedulingLatencyMs       *HistogramMetric
	SchedulingAttempts        *CounterMetric
	SchedulingSuccesses       *CounterMetric
	SchedulingFailures        *CounterMetric
	TopologyOptimalPlacements *CounterMetric
	PreemptionCount           *CounterMetric

	// GPU metrics
	GPUCount        *GaugeMetric
	GPUUtilization  *GaugeVecMetric
	GPUMemoryUsed   *GaugeVecMetric
	GPUMemoryTotal  *GaugeVecMetric
	GPUTemperature  *GaugeVecMetric
	GPUPowerWatts   *GaugeVecMetric
	GPUHealthStatus *GaugeVecMetric

	// MIG metrics
	MIGInstanceCount       *GaugeVecMetric
	MIGInstanceUtilization *GaugeVecMetric
	MIGAllocations         *CounterMetric
	MIGReleases            *CounterMetric

	// Topology metrics
	NVLinkBandwidth *GaugeVecMetric
	PCIeBandwidth   *GaugeVecMetric
	TopologyScore   *GaugeVecMetric

	// Cost metrics
	GPUCostTotal           *CounterVecMetric
	GPUCostPerHour         *GaugeVecMetric
	BudgetUtilization      *GaugeVecMetric
	CostSavingsRecommended *GaugeMetric

	// Workload metrics
	ActiveWorkloads    *GaugeVecMetric
	WorkloadDuration   *HistogramMetric
	WorkloadQueueDepth *GaugeMetric

	// Queue controller metrics
	QueueDepthByTenant     *GaugeVecMetric
	QueueDepthByGPUClass   *GaugeVecMetric
	WaitTimeHistogram      *HistogramMetric
	AgingBoostsTotal       *CounterMetric
	StarvationEventsTotal  *CounterMetric
	PreemptionSignalsTotal *CounterMetric

	// Fairness metrics
	GPUShareByTenant         *GaugeVecMetric
	DRFDominantShareByTenant *GaugeVecMetric
	PerTenantPreemptions     *CounterVecMetric

	// GPU class visibility metrics
	GPUClassJobsTotal *CounterVecMetric
}

// GaugeMetric is a simple gauge metric
type GaugeMetric struct {
	name   string
	help   string
	value  float64
	labels map[string]string
}

func (m *GaugeMetric) Name() string       { return m.name }
func (m *GaugeMetric) Help() string       { return m.help }
func (m *GaugeMetric) Type() MetricType   { return MetricTypeGauge }
func (m *GaugeMetric) Value() interface{} { return m.value }
func (m *GaugeMetric) Set(v float64)      { m.value = v }

// GaugeVecMetric is a labeled gauge metric
type GaugeVecMetric struct {
	name   string
	help   string
	labels []string
	values map[string]float64
	mu     sync.RWMutex
}

func (m *GaugeVecMetric) Name() string       { return m.name }
func (m *GaugeVecMetric) Help() string       { return m.help }
func (m *GaugeVecMetric) Type() MetricType   { return MetricTypeGauge }
func (m *GaugeVecMetric) Value() interface{} { return m.values }

func (m *GaugeVecMetric) WithLabels(labels map[string]string) *GaugeVecMetric {
	return m
}

func (m *GaugeVecMetric) Set(key string, v float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[key] = v
}

// CounterMetric is a simple counter metric
type CounterMetric struct {
	name  string
	help  string
	value float64
}

func (m *CounterMetric) Name() string       { return m.name }
func (m *CounterMetric) Help() string       { return m.help }
func (m *CounterMetric) Type() MetricType   { return MetricTypeCounter }
func (m *CounterMetric) Value() interface{} { return m.value }
func (m *CounterMetric) Inc()               { m.value++ }
func (m *CounterMetric) Add(v float64)      { m.value += v }

// CounterVecMetric is a labeled counter metric
type CounterVecMetric struct {
	name   string
	help   string
	labels []string
	values map[string]float64
	mu     sync.RWMutex
}

func (m *CounterVecMetric) Name() string       { return m.name }
func (m *CounterVecMetric) Help() string       { return m.help }
func (m *CounterVecMetric) Type() MetricType   { return MetricTypeCounter }
func (m *CounterVecMetric) Value() interface{} { return m.values }

func (m *CounterVecMetric) Inc(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[key]++
}

func (m *CounterVecMetric) Add(key string, v float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[key] += v
}

// HistogramMetric tracks value distributions
type HistogramMetric struct {
	name    string
	help    string
	buckets []float64
	counts  map[float64]int64
	sum     float64
	count   int64
	mu      sync.RWMutex
}

func (m *HistogramMetric) Name() string       { return m.name }
func (m *HistogramMetric) Help() string       { return m.help }
func (m *HistogramMetric) Type() MetricType   { return MetricTypeHistogram }
func (m *HistogramMetric) Value() interface{} { return m.counts }

func (m *HistogramMetric) Observe(v float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sum += v
	m.count++
	for _, bucket := range m.buckets {
		if v <= bucket {
			m.counts[bucket]++
		}
	}
}

// NewPrometheusExporter creates a new exporter
func NewPrometheusExporter(
	discoveryService *discovery.DiscoveryService,
	config ExporterConfig,
) *PrometheusExporter {
	exporter := &PrometheusExporter{
		discoveryService: discoveryService,
		config:           config,
		stopChan:         make(chan struct{}),
	}

	exporter.initMetrics()
	return exporter
}

// initMetrics initializes all metrics
func (e *PrometheusExporter) initMetrics() {
	e.metrics = &KGWEMetrics{
		// Scheduler metrics
		SchedulingLatencyMs: &HistogramMetric{
			name:    "kgwe_scheduling_latency_ms",
			help:    "Histogram of scheduling latency in milliseconds",
			buckets: []float64{10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
			counts:  make(map[float64]int64),
		},
		SchedulingAttempts: &CounterMetric{
			name: "kgwe_scheduling_attempts_total",
			help: "Total number of scheduling attempts",
		},
		SchedulingSuccesses: &CounterMetric{
			name: "kgwe_scheduling_successes_total",
			help: "Total number of successful schedulings",
		},
		SchedulingFailures: &CounterMetric{
			name: "kgwe_scheduling_failures_total",
			help: "Total number of scheduling failures",
		},
		TopologyOptimalPlacements: &CounterMetric{
			name: "kgwe_topology_optimal_placements_total",
			help: "Total number of topology-optimal placements",
		},
		PreemptionCount: &CounterMetric{
			name: "kgwe_preemptions_total",
			help: "Total number of workload preemptions",
		},

		// GPU metrics
		GPUCount: &GaugeMetric{
			name: "kgwe_gpu_count",
			help: "Total number of GPUs in cluster",
		},
		GPUUtilization: &GaugeVecMetric{
			name:   "kgwe_gpu_utilization_percent",
			help:   "GPU SM utilization percentage",
			labels: []string{"gpu_uuid", "node", "model"},
			values: make(map[string]float64),
		},
		GPUMemoryUsed: &GaugeVecMetric{
			name:   "kgwe_gpu_memory_used_bytes",
			help:   "GPU memory used in bytes",
			labels: []string{"gpu_uuid", "node"},
			values: make(map[string]float64),
		},
		GPUMemoryTotal: &GaugeVecMetric{
			name:   "kgwe_gpu_memory_total_bytes",
			help:   "GPU total memory in bytes",
			labels: []string{"gpu_uuid", "node"},
			values: make(map[string]float64),
		},
		GPUTemperature: &GaugeVecMetric{
			name:   "kgwe_gpu_temperature_celsius",
			help:   "GPU temperature in Celsius",
			labels: []string{"gpu_uuid", "node"},
			values: make(map[string]float64),
		},
		GPUPowerWatts: &GaugeVecMetric{
			name:   "kgwe_gpu_power_watts",
			help:   "GPU power consumption in watts",
			labels: []string{"gpu_uuid", "node"},
			values: make(map[string]float64),
		},
		GPUHealthStatus: &GaugeVecMetric{
			name:   "kgwe_gpu_health_status",
			help:   "GPU health status (1=healthy, 0=unhealthy)",
			labels: []string{"gpu_uuid", "node"},
			values: make(map[string]float64),
		},

		// MIG metrics
		MIGInstanceCount: &GaugeVecMetric{
			name:   "kgwe_mig_instance_count",
			help:   "Number of MIG instances per GPU",
			labels: []string{"gpu_uuid", "node", "profile"},
			values: make(map[string]float64),
		},
		MIGInstanceUtilization: &GaugeVecMetric{
			name:   "kgwe_mig_instance_utilization_percent",
			help:   "MIG instance utilization percentage",
			labels: []string{"instance_uuid", "gpu_uuid", "profile"},
			values: make(map[string]float64),
		},
		MIGAllocations: &CounterMetric{
			name: "kgwe_mig_allocations_total",
			help: "Total MIG instance allocations",
		},
		MIGReleases: &CounterMetric{
			name: "kgwe_mig_releases_total",
			help: "Total MIG instance releases",
		},

		// Topology metrics
		NVLinkBandwidth: &GaugeVecMetric{
			name:   "kgwe_nvlink_bandwidth_gbps",
			help:   "NVLink bandwidth between GPUs in GB/s",
			labels: []string{"gpu_uuid_1", "gpu_uuid_2", "node"},
			values: make(map[string]float64),
		},
		PCIeBandwidth: &GaugeVecMetric{
			name:   "kgwe_pcie_bandwidth_gbps",
			help:   "PCIe bandwidth in GB/s",
			labels: []string{"gpu_uuid", "node"},
			values: make(map[string]float64),
		},
		TopologyScore: &GaugeVecMetric{
			name:   "kgwe_topology_score",
			help:   "Node topology quality score (0-100)",
			labels: []string{"node"},
			values: make(map[string]float64),
		},

		// Cost metrics
		GPUCostTotal: &CounterVecMetric{
			name:   "kgwe_gpu_cost_total_dollars",
			help:   "Total GPU cost in dollars",
			labels: []string{"namespace", "team"},
			values: make(map[string]float64),
		},
		GPUCostPerHour: &GaugeVecMetric{
			name:   "kgwe_gpu_cost_per_hour_dollars",
			help:   "Current GPU cost rate per hour in dollars",
			labels: []string{"namespace", "team"},
			values: make(map[string]float64),
		},
		BudgetUtilization: &GaugeVecMetric{
			name:   "kgwe_budget_utilization_percent",
			help:   "Budget utilization percentage",
			labels: []string{"budget_id", "scope"},
			values: make(map[string]float64),
		},
		CostSavingsRecommended: &GaugeMetric{
			name: "kgwe_cost_savings_recommended_dollars",
			help: "Total recommended cost savings in dollars",
		},

		// Workload metrics
		ActiveWorkloads: &GaugeVecMetric{
			name:   "kgwe_active_workloads",
			help:   "Number of active GPU workloads",
			labels: []string{"namespace", "workload_type"},
			values: make(map[string]float64),
		},
		WorkloadDuration: &HistogramMetric{
			name:    "kgwe_workload_duration_seconds",
			help:    "Histogram of workload duration in seconds",
			buckets: []float64{60, 300, 900, 1800, 3600, 7200, 14400, 28800, 86400},
			counts:  make(map[float64]int64),
		},
		WorkloadQueueDepth: &GaugeMetric{
			name: "kgwe_workload_queue_depth",
			help: "Number of workloads waiting to be scheduled",
		},

		// Queue controller metrics
		QueueDepthByTenant: &GaugeVecMetric{
			name:   "kgwe_queue_depth_by_tenant",
			help:   "Current queue depth per tenant",
			labels: []string{"tenant"},
			values: make(map[string]float64),
		},
		QueueDepthByGPUClass: &GaugeVecMetric{
			name:   "kgwe_queue_depth_by_gpu_class",
			help:   "Current queue depth by GPU class",
			labels: []string{"gpu_class"},
			values: make(map[string]float64),
		},
		WaitTimeHistogram: &HistogramMetric{
			name:    "kgwe_wait_time_seconds",
			help:    "Histogram of queue wait times in seconds",
			buckets: []float64{10, 60, 300, 600, 1800, 3600},
			counts:  make(map[float64]int64),
		},
		AgingBoostsTotal: &CounterMetric{
			name: "kgwe_aging_boosts_total",
			help: "Total priority aging boosts applied",
		},
		StarvationEventsTotal: &CounterMetric{
			name: "kgwe_starvation_events_total",
			help: "Total starvation prevention events",
		},
		PreemptionSignalsTotal: &CounterMetric{
			name: "kgwe_preemption_signals_total",
			help: "Total preemption signals emitted",
		},

		// Fairness metrics
		GPUShareByTenant: &GaugeVecMetric{
			name:   "kgwe_gpu_share_by_tenant",
			help:   "Tenant GPU share fraction",
			labels: []string{"tenant"},
			values: make(map[string]float64),
		},
		DRFDominantShareByTenant: &GaugeVecMetric{
			name:   "kgwe_drf_dominant_share_by_tenant",
			help:   "Tenant DRF dominant resource share",
			labels: []string{"tenant"},
			values: make(map[string]float64),
		},
		PerTenantPreemptions: &CounterVecMetric{
			name:   "kgwe_preemptions_by_tenant_total",
			help:   "Preemptions per tenant",
			labels: []string{"tenant"},
			values: make(map[string]float64),
		},

		// GPU class visibility metrics
		GPUClassJobsTotal: &CounterVecMetric{
			name:   "kgwe_gpu_class_jobs_total",
			help:   "Jobs assigned by GPU class",
			labels: []string{"gpu_class"},
			values: make(map[string]float64),
		},
	}
}

// Start begins metric collection and HTTP server
func (e *PrometheusExporter) Start(ctx context.Context) error {
	// Start collection loop
	go e.collectLoop(ctx)

	// Start HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc(e.config.MetricsPath, e.metricsHandler)
	mux.HandleFunc("/health", e.healthHandler)

	server := &http.Server{
		Addr:    e.config.ListenAddress,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	return server.ListenAndServe()
}

// collectLoop periodically collects metrics
func (e *PrometheusExporter) collectLoop(ctx context.Context) {
	ticker := time.NewTicker(e.config.CollectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.collectMetrics()
		case <-ctx.Done():
			return
		case <-e.stopChan:
			return
		}
	}
}

// collectMetrics gathers current metric values
func (e *PrometheusExporter) collectMetrics() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.discoveryService == nil {
		return
	}

	topology := e.discoveryService.GetClusterTopology()
	if topology == nil {
		return
	}

	// Collect GPU metrics
	e.metrics.GPUCount.Set(float64(topology.TotalGPUs))

	for nodeName, node := range topology.Nodes {
		for _, gpu := range node.GPUs {
			key := fmt.Sprintf("%s/%s", nodeName, gpu.UUID)

			e.metrics.GPUUtilization.Set(key, gpu.Utilization.GPUPercent)
			e.metrics.GPUMemoryUsed.Set(key, float64(gpu.Memory.UsedBytes))
			e.metrics.GPUMemoryTotal.Set(key, float64(gpu.Memory.TotalBytes))
			e.metrics.GPUTemperature.Set(key, gpu.Utilization.TemperatureCelsius)
			e.metrics.GPUPowerWatts.Set(key, gpu.Utilization.PowerWatts)

			healthValue := 0.0
			if gpu.Health.Status == discovery.HealthStatusHealthy {
				healthValue = 1.0
			}
			e.metrics.GPUHealthStatus.Set(key, healthValue)

			// PCIe bandwidth
			e.metrics.PCIeBandwidth.Set(key, gpu.Topology.PCIeInfo.BandwidthGBps)

			// NVLink bandwidth
			for peerUUID, nvlink := range gpu.Topology.NVLinkConnections {
				nvlinkKey := fmt.Sprintf("%s/%s/%s", nodeName, gpu.UUID, peerUUID)
				e.metrics.NVLinkBandwidth.Set(nvlinkKey, nvlink.BandwidthGBps)
			}

			// MIG instances
			if gpu.MIGConfig != nil && gpu.MIGConfig.Enabled {
				for _, instance := range gpu.MIGConfig.CurrentInstances {
					migKey := fmt.Sprintf("%s/%s/%s", gpu.UUID, instance.UUID, instance.Profile.Name)
					e.metrics.MIGInstanceCount.Set(key, float64(len(gpu.MIGConfig.CurrentInstances)))
					if instance.InUse {
						e.metrics.MIGInstanceUtilization.Set(migKey, 100.0)
					} else {
						e.metrics.MIGInstanceUtilization.Set(migKey, 0.0)
					}
				}
			}
		}

		// Node topology score
		score := e.calculateTopologyScore(node)
		e.metrics.TopologyScore.Set(nodeName, float64(score))
	}
}

// calculateTopologyScore calculates a node's topology quality
func (e *PrometheusExporter) calculateTopologyScore(node *discovery.NodeTopology) int {
	if len(node.GPUs) == 0 {
		return 0
	}

	score := 50 // Base score

	// NVSwitch bonus
	if node.NVSwitchInfo != nil && node.NVSwitchInfo.Present {
		score += 30
	}

	// NVLink connectivity bonus
	nvlinkCount := 0
	for _, gpu := range node.GPUs {
		nvlinkCount += len(gpu.Topology.NVLinkConnections)
	}
	if nvlinkCount > 0 {
		score += 20
	}

	return min(100, score)
}

// metricsHandler handles Prometheus scrape requests
func (e *PrometheusExporter) metricsHandler(w http.ResponseWriter, r *http.Request) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	// Write scheduler metrics
	e.writeMetric(w, e.metrics.SchedulingLatencyMs)
	e.writeMetric(w, e.metrics.SchedulingAttempts)
	e.writeMetric(w, e.metrics.SchedulingSuccesses)
	e.writeMetric(w, e.metrics.SchedulingFailures)
	e.writeMetric(w, e.metrics.TopologyOptimalPlacements)
	e.writeMetric(w, e.metrics.PreemptionCount)

	// Write GPU metrics
	e.writeMetric(w, e.metrics.GPUCount)
	e.writeVecMetric(w, e.metrics.GPUUtilization)
	e.writeVecMetric(w, e.metrics.GPUMemoryUsed)
	e.writeVecMetric(w, e.metrics.GPUMemoryTotal)
	e.writeVecMetric(w, e.metrics.GPUTemperature)
	e.writeVecMetric(w, e.metrics.GPUPowerWatts)
	e.writeVecMetric(w, e.metrics.GPUHealthStatus)

	// Write MIG metrics
	e.writeVecMetric(w, e.metrics.MIGInstanceCount)
	e.writeVecMetric(w, e.metrics.MIGInstanceUtilization)
	e.writeMetric(w, e.metrics.MIGAllocations)
	e.writeMetric(w, e.metrics.MIGReleases)

	// Write topology metrics
	e.writeVecMetric(w, e.metrics.NVLinkBandwidth)
	e.writeVecMetric(w, e.metrics.PCIeBandwidth)
	e.writeVecMetric(w, e.metrics.TopologyScore)

	// Write cost metrics
	e.writeVecCounterMetric(w, e.metrics.GPUCostTotal)
	e.writeVecMetric(w, e.metrics.GPUCostPerHour)
	e.writeVecMetric(w, e.metrics.BudgetUtilization)
	e.writeMetric(w, e.metrics.CostSavingsRecommended)

	// Write workload metrics
	e.writeVecMetric(w, e.metrics.ActiveWorkloads)
	e.writeMetric(w, e.metrics.WorkloadQueueDepth)

	// Write queue controller metrics
	e.writeVecMetric(w, e.metrics.QueueDepthByTenant)
	e.writeVecMetric(w, e.metrics.QueueDepthByGPUClass)
	e.writeMetric(w, e.metrics.WaitTimeHistogram)
	e.writeMetric(w, e.metrics.AgingBoostsTotal)
	e.writeMetric(w, e.metrics.StarvationEventsTotal)
	e.writeMetric(w, e.metrics.PreemptionSignalsTotal)

	// Write fairness metrics
	e.writeVecMetric(w, e.metrics.GPUShareByTenant)
	e.writeVecMetric(w, e.metrics.DRFDominantShareByTenant)
	e.writeVecCounterMetric(w, e.metrics.PerTenantPreemptions)

	// Write GPU class visibility metrics
	e.writeVecCounterMetric(w, e.metrics.GPUClassJobsTotal)
}

func (e *PrometheusExporter) writeMetric(w http.ResponseWriter, m Metric) {
	fmt.Fprintf(w, "# HELP %s %s\n", m.Name(), m.Help())
	fmt.Fprintf(w, "# TYPE %s %s\n", m.Name(), m.Type())

	switch v := m.Value().(type) {
	case float64:
		fmt.Fprintf(w, "%s %f\n", m.Name(), v)
	case map[float64]int64: // Histogram
		// Write histogram buckets
		if hm, ok := m.(*HistogramMetric); ok {
			for _, bucket := range hm.buckets {
				fmt.Fprintf(w, "%s_bucket{le=\"%f\"} %d\n", m.Name(), bucket, hm.counts[bucket])
			}
			fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", m.Name(), hm.count)
			fmt.Fprintf(w, "%s_sum %f\n", m.Name(), hm.sum)
			fmt.Fprintf(w, "%s_count %d\n", m.Name(), hm.count)
		}
	}
}

func (e *PrometheusExporter) writeVecMetric(w http.ResponseWriter, m *GaugeVecMetric) {
	fmt.Fprintf(w, "# HELP %s %s\n", m.Name(), m.Help())
	fmt.Fprintf(w, "# TYPE %s %s\n", m.Name(), m.Type())

	m.mu.RLock()
	defer m.mu.RUnlock()

	for key, value := range m.values {
		fmt.Fprintf(w, "%s{label=\"%s\"} %f\n", m.Name(), key, value)
	}
}

func (e *PrometheusExporter) writeVecCounterMetric(w http.ResponseWriter, m *CounterVecMetric) {
	fmt.Fprintf(w, "# HELP %s %s\n", m.Name(), m.Help())
	fmt.Fprintf(w, "# TYPE %s %s\n", m.Name(), m.Type())

	m.mu.RLock()
	defer m.mu.RUnlock()

	for key, value := range m.values {
		fmt.Fprintf(w, "%s{label=\"%s\"} %f\n", m.Name(), key, value)
	}
}

// healthHandler handles health check requests
func (e *PrometheusExporter) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// Stop shuts down the exporter
func (e *PrometheusExporter) Stop() {
	close(e.stopChan)
}

// RecordSchedulingLatency records a scheduling latency sample
func (e *PrometheusExporter) RecordSchedulingLatency(latencyMs float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.metrics.SchedulingLatencyMs.Observe(latencyMs)
}

// RecordSchedulingAttempt records a scheduling attempt
func (e *PrometheusExporter) RecordSchedulingAttempt(success bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.metrics.SchedulingAttempts.Inc()
	if success {
		e.metrics.SchedulingSuccesses.Inc()
	} else {
		e.metrics.SchedulingFailures.Inc()
	}
}

// RecordCost records a cost event
func (e *PrometheusExporter) RecordCost(namespace, team string, amount float64, labels map[string]string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	key := fmt.Sprintf("%s/%s", namespace, team)
	e.metrics.GPUCostTotal.Add(key, amount)
}

// RecordUtilization records GPU utilization
func (e *PrometheusExporter) RecordUtilization(gpuUUID string, utilization float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.metrics.GPUUtilization.Set(gpuUUID, utilization)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
