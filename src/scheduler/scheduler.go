// Package scheduler implements topology-aware GPU scheduling for Kubernetes.
package scheduler

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/nvidia/kgwe/src/discovery"
)

// TopologyAwareScheduler implements GPU-aware Kubernetes scheduling
type TopologyAwareScheduler struct {
	mu sync.RWMutex

	// config holds scheduler configuration
	config SchedulerConfig

	// discoveryService provides GPU topology information
	discoveryService *discovery.DiscoveryService

	// optimizer provides workload optimization hints
	optimizer WorkloadOptimizer

	// allocations tracks current GPU allocations
	allocations map[string]*GPUAllocation

	// gangGroups tracks gang scheduling groups
	gangGroups map[string]*GangSchedulingGroup

	// metrics tracks scheduling performance
	metrics *SchedulerMetrics

	// eventChan receives scheduling events
	eventChan chan SchedulingEvent
}

// WorkloadOptimizer interface for workload-based optimization
type WorkloadOptimizer interface {
	// PredictResourceNeeds predicts optimal resources
	PredictResourceNeeds(workload *GPUWorkload) (*ResourcePrediction, error)

	// GetOptimalPlacement returns optimized placement hints
	GetOptimalPlacement(workload *GPUWorkload, topology *discovery.ClusterTopology) (*PlacementHint, error)
}

// ResourcePrediction from the optimizer
type ResourcePrediction struct {
	RecommendedGPUs        int
	RecommendedMemoryGB    int
	ExpectedUtilization    float64
	ExpectedDuration       time.Duration
	Confidence             float64
}

// PlacementHint from the optimizer
type PlacementHint struct {
	PreferredNodes []string
	PreferredGPUs  map[string][]string // node -> gpu uuids
	Score          int
	Reasoning      string
}

// GPUAllocation tracks a GPU allocation
type GPUAllocation struct {
	WorkloadUID  string
	NodeName     string
	GPUUUIDs     []string
	MIGInstances []MIGInstanceAllocation
	AllocatedAt  time.Time
	WorkloadType WorkloadType
}

// SchedulingEvent represents a scheduling event
type SchedulingEvent struct {
	Type        SchedulingEventType
	WorkloadUID string
	Decision    *SchedulingDecision
	Error       error
	Timestamp   time.Time
}

// SchedulingEventType categorizes events
type SchedulingEventType string

const (
	EventTypeScheduled  SchedulingEventType = "Scheduled"
	EventTypeFailed     SchedulingEventType = "Failed"
	EventTypePreempted  SchedulingEventType = "Preempted"
	EventTypeReleased   SchedulingEventType = "Released"
)

// NewTopologyAwareScheduler creates a new scheduler
func NewTopologyAwareScheduler(
	discoveryService *discovery.DiscoveryService,
	optimizer WorkloadOptimizer,
	config SchedulerConfig,
) *TopologyAwareScheduler {
	return &TopologyAwareScheduler{
		config:           config,
		discoveryService: discoveryService,
		optimizer:        optimizer,
		allocations:      make(map[string]*GPUAllocation),
		gangGroups:       make(map[string]*GangSchedulingGroup),
		metrics:          &SchedulerMetrics{},
		eventChan:        make(chan SchedulingEvent, 100),
	}
}

// Schedule performs GPU-aware scheduling for a workload
func (s *TopologyAwareScheduler) Schedule(ctx context.Context, workload *GPUWorkload) (*SchedulingDecision, error) {
	startTime := time.Now()
	defer func() {
		latency := time.Since(startTime).Milliseconds()
		s.updateLatencyMetrics(float64(latency))
	}()

	// Get current cluster topology
	topology := s.discoveryService.GetClusterTopology()
	if topology == nil {
		return nil, fmt.Errorf("cluster topology not available")
	}

	// Get optimization hints if optimizer is available
	var placementHint *PlacementHint
	if s.optimizer != nil {
		hint, err := s.optimizer.GetOptimalPlacement(workload, topology)
		if err == nil {
			placementHint = hint
		}
	}

	// Score all eligible nodes
	nodeScores := s.scoreNodes(topology, workload, placementHint)
	if len(nodeScores) == 0 {
		s.metrics.TotalFailed++
		return nil, fmt.Errorf("no eligible nodes found for workload %s", workload.Name)
	}

	// Sort by score descending
	sort.Slice(nodeScores, func(i, j int) bool {
		return nodeScores[i].TotalScore > nodeScores[j].TotalScore
	})

	// Try to schedule on best node
	for _, nodeScore := range nodeScores {
		decision, err := s.tryScheduleOnNode(ctx, workload, nodeScore)
		if err == nil {
			s.recordAllocation(workload, decision)
			s.metrics.TotalScheduled++
			if nodeScore.TopologyScore >= 80 {
				s.metrics.TopologyOptimalPlacements++
			}

			s.eventChan <- SchedulingEvent{
				Type:        EventTypeScheduled,
				WorkloadUID: workload.UID,
				Decision:    decision,
				Timestamp:   time.Now(),
			}

			return decision, nil
		}
	}

	// If scheduling failed, try preemption if enabled
	if s.config.EnablePreemption && workload.Priority > 0 {
		decision, err := s.scheduleWithPreemption(ctx, workload, topology)
		if err == nil {
			return decision, nil
		}
	}

	s.metrics.TotalFailed++
	return nil, fmt.Errorf("failed to schedule workload %s after trying all nodes", workload.Name)
}

// scoreNodes calculates scores for all eligible nodes
func (s *TopologyAwareScheduler) scoreNodes(
	topology *discovery.ClusterTopology,
	workload *GPUWorkload,
	hint *PlacementHint,
) []NodeScore {
	scores := make([]NodeScore, 0)

	for nodeName, node := range topology.Nodes {
		// Check basic eligibility
		if !s.isNodeEligible(node, workload) {
			continue
		}

		score := s.scoreNode(node, workload, hint)
		if score.TotalScore > 0 {
			score.NodeName = nodeName
			scores = append(scores, score)
		}
	}

	return scores
}

// isNodeEligible checks if node meets basic requirements
func (s *TopologyAwareScheduler) isNodeEligible(node *discovery.NodeTopology, workload *GPUWorkload) bool {
	// Check node selectors
	if len(workload.Constraints.NodeSelector) > 0 {
		// In production, check actual node labels
	}

	// Check GPU count
	availableGPUs := s.getAvailableGPUs(node)
	if len(availableGPUs) < workload.Requirements.GPUCount {
		return false
	}

	// Check architecture requirement
	if workload.Requirements.GPUArchitecture != "" {
		hasArch := false
		for _, gpu := range availableGPUs {
			if gpu.Architecture == workload.Requirements.GPUArchitecture {
				hasArch = true
				break
			}
		}
		if !hasArch {
			return false
		}
	}

	// Check MIG requirements
	if workload.Requirements.MIGRequirements != nil {
		migAvailable := s.getMIGAvailability(node, workload.Requirements.MIGRequirements)
		if migAvailable < workload.Requirements.MIGRequirements.Count {
			return false
		}
	}

	return true
}

// scoreNode calculates comprehensive score for a node
func (s *TopologyAwareScheduler) scoreNode(
	node *discovery.NodeTopology,
	workload *GPUWorkload,
	hint *PlacementHint,
) NodeScore {
	score := NodeScore{
		NodeName: node.NodeName,
		Reasons:  make([]string, 0),
	}

	availableGPUs := s.getAvailableGPUs(node)

	// Calculate topology score (0-100)
	topologyScore, selectedGPUs := s.calculateTopologyScore(node, availableGPUs, workload)
	score.TopologyScore = topologyScore
	score.SelectedGPUs = selectedGPUs

	// Calculate resource score (0-100)
	resourceScore := s.calculateResourceScore(node, availableGPUs, workload)
	score.ResourceScore = resourceScore

	// Calculate balance score (0-100)
	balanceScore := s.calculateBalanceScore(node)
	score.BalanceScore = balanceScore

	// Apply hint bonus if node is preferred
	hintBonus := 0
	if hint != nil {
		for _, preferredNode := range hint.PreferredNodes {
			if preferredNode == node.NodeName {
				hintBonus = 10
				score.Reasons = append(score.Reasons, "ML optimizer preferred this node")
				break
			}
		}
	}

	// Calculate weighted total
	score.TotalScore = (score.TopologyScore*s.config.TopologyWeight +
		score.ResourceScore*s.config.ResourceWeight +
		score.BalanceScore*s.config.BalanceWeight) / 100

	score.TotalScore += hintBonus

	// Add scoring reasons
	if score.TopologyScore >= 80 {
		score.Reasons = append(score.Reasons, "Optimal NVLink connectivity available")
	}
	if score.ResourceScore >= 80 {
		score.Reasons = append(score.Reasons, "Sufficient resources with headroom")
	}
	if score.BalanceScore >= 80 {
		score.Reasons = append(score.Reasons, "Good load distribution")
	}

	return score
}

// calculateTopologyScore scores based on GPU interconnect topology
func (s *TopologyAwareScheduler) calculateTopologyScore(
	node *discovery.NodeTopology,
	availableGPUs []*discovery.GPUDevice,
	workload *GPUWorkload,
) (int, []string) {
	required := workload.Requirements.GPUCount
	if required <= 1 {
		// Single GPU, topology doesn't matter
		if len(availableGPUs) > 0 {
			return 100, []string{availableGPUs[0].UUID}
		}
		return 0, nil
	}

	// Find best GPU combination based on topology preference
	switch workload.Requirements.TopologyPreference {
	case TopologyPreferenceNVLinkRequired, TopologyPreferenceNVLinkOptimal:
		return s.scoreNVLinkTopology(node, availableGPUs, required)
	case TopologyPreferenceSameNUMA:
		return s.scoreNUMATopology(node, availableGPUs, required)
	case TopologyPreferenceSamePCIeSwitch:
		return s.scorePCIeTopology(node, availableGPUs, required)
	default:
		// No preference, just find any available GPUs
		gpus := make([]string, 0, required)
		for i := 0; i < required && i < len(availableGPUs); i++ {
			gpus = append(gpus, availableGPUs[i].UUID)
		}
		return 50, gpus
	}
}

// scoreNVLinkTopology scores based on NVLink connectivity
func (s *TopologyAwareScheduler) scoreNVLinkTopology(
	node *discovery.NodeTopology,
	availableGPUs []*discovery.GPUDevice,
	required int,
) (int, []string) {
	if len(availableGPUs) < required {
		return 0, nil
	}

	// Build adjacency graph for NVLink connections
	nvlinkGraph := make(map[string]map[string]float64)
	for _, gpu := range availableGPUs {
		nvlinkGraph[gpu.UUID] = make(map[string]float64)
		for peerUUID, info := range gpu.Topology.NVLinkConnections {
			if info.IsActive {
				nvlinkGraph[gpu.UUID][peerUUID] = info.BandwidthGBps
			}
		}
	}

	// Find best clique of required size
	bestGroup, totalBandwidth := s.findBestNVLinkGroup(nvlinkGraph, availableGPUs, required)
	if len(bestGroup) < required {
		// Couldn't find fully connected group, fallback
		gpus := make([]string, 0, required)
		for i := 0; i < required && i < len(availableGPUs); i++ {
			gpus = append(gpus, availableGPUs[i].UUID)
		}
		return 30, gpus // Lower score for non-NVLink placement
	}

	// Score based on bandwidth (normalized to H100 NVSwitch: ~900 GB/s)
	maxBandwidth := 900.0 * float64(required*(required-1)/2) // Full mesh
	bandwidthRatio := totalBandwidth / maxBandwidth
	score := int(50 + 50*bandwidthRatio) // 50-100 range

	return score, bestGroup
}

// findBestNVLinkGroup finds the best connected GPU group
func (s *TopologyAwareScheduler) findBestNVLinkGroup(
	graph map[string]map[string]float64,
	availableGPUs []*discovery.GPUDevice,
	size int,
) ([]string, float64) {
	if size <= 1 && len(availableGPUs) > 0 {
		return []string{availableGPUs[0].UUID}, 0
	}

	bestGroup := make([]string, 0)
	bestBandwidth := 0.0

	// Greedy approach: start from each GPU and expand
	for _, startGPU := range availableGPUs {
		group := []string{startGPU.UUID}
		totalBandwidth := 0.0

		// Candidates sorted by aggregate bandwidth to existing group
		for len(group) < size {
			bestCandidate := ""
			bestCandidateBW := 0.0

			for _, candidate := range availableGPUs {
				if contains(group, candidate.UUID) {
					continue
				}

				// Calculate aggregate bandwidth to group
				aggBW := 0.0
				fullyConnected := true
				for _, member := range group {
					if bw, ok := graph[candidate.UUID][member]; ok {
						aggBW += bw
					} else {
						fullyConnected = false
					}
				}

				if fullyConnected && aggBW > bestCandidateBW {
					bestCandidate = candidate.UUID
					bestCandidateBW = aggBW
				}
			}

			if bestCandidate == "" {
				break // Can't extend further
			}

			group = append(group, bestCandidate)
			totalBandwidth += bestCandidateBW
		}

		if len(group) >= size && totalBandwidth > bestBandwidth {
			bestGroup = group[:size]
			bestBandwidth = totalBandwidth
		}
	}

	return bestGroup, bestBandwidth
}

// scoreNUMATopology scores based on NUMA locality
func (s *TopologyAwareScheduler) scoreNUMATopology(
	node *discovery.NodeTopology,
	availableGPUs []*discovery.GPUDevice,
	required int,
) (int, []string) {
	// Group GPUs by NUMA node
	numaGroups := make(map[int][]*discovery.GPUDevice)
	for _, gpu := range availableGPUs {
		numaGroups[gpu.Topology.NUMANode] = append(numaGroups[gpu.Topology.NUMANode], gpu)
	}

	// Find NUMA node with enough GPUs
	for _, gpus := range numaGroups {
		if len(gpus) >= required {
			selected := make([]string, required)
			for i := 0; i < required; i++ {
				selected[i] = gpus[i].UUID
			}
			return 90, selected // High score for same-NUMA placement
		}
	}

	// Fallback: spread across NUMA nodes
	selected := make([]string, 0, required)
	for _, gpus := range numaGroups {
		for _, gpu := range gpus {
			selected = append(selected, gpu.UUID)
			if len(selected) >= required {
				return 50, selected
			}
		}
	}

	return 0, nil
}

// scorePCIeTopology scores based on PCIe switch locality
func (s *TopologyAwareScheduler) scorePCIeTopology(
	node *discovery.NodeTopology,
	availableGPUs []*discovery.GPUDevice,
	required int,
) (int, []string) {
	// Group by PCIe switch
	switchGroups := make(map[string][]*discovery.GPUDevice)
	for _, gpu := range availableGPUs {
		switchID := gpu.Topology.PCIeInfo.SwitchID
		if switchID == "" {
			switchID = "default"
		}
		switchGroups[switchID] = append(switchGroups[switchID], gpu)
	}

	// Find switch with enough GPUs
	for _, gpus := range switchGroups {
		if len(gpus) >= required {
			selected := make([]string, required)
			for i := 0; i < required; i++ {
				selected[i] = gpus[i].UUID
			}
			return 80, selected
		}
	}

	// Fallback
	selected := make([]string, 0, required)
	for _, gpus := range switchGroups {
		for _, gpu := range gpus {
			selected = append(selected, gpu.UUID)
			if len(selected) >= required {
				return 40, selected
			}
		}
	}

	return 0, nil
}

// calculateResourceScore scores based on resource availability
func (s *TopologyAwareScheduler) calculateResourceScore(
	node *discovery.NodeTopology,
	availableGPUs []*discovery.GPUDevice,
	workload *GPUWorkload,
) int {
	if len(availableGPUs) == 0 {
		return 0
	}

	score := 50 // Base score

	// Check memory headroom
	totalFreeMemory := uint64(0)
	for _, gpu := range availableGPUs {
		totalFreeMemory += gpu.Memory.FreeBytes
	}
	requiredMemory := uint64(workload.Requirements.MinMemoryGB) * 1024 * 1024 * 1024 * uint64(workload.Requirements.GPUCount)
	if totalFreeMemory >= requiredMemory*2 {
		score += 25 // Good headroom
	} else if totalFreeMemory >= requiredMemory {
		score += 10 // Minimal headroom
	}

	// Check compute capacity
	avgUtilization := 0.0
	for _, gpu := range availableGPUs {
		avgUtilization += gpu.Utilization.GPUPercent
	}
	avgUtilization /= float64(len(availableGPUs))

	if avgUtilization < 30 {
		score += 25 // Low utilization bonus
	} else if avgUtilization < 60 {
		score += 10
	}

	return minInt(100, score)
}

// calculateBalanceScore scores based on cluster balance
func (s *TopologyAwareScheduler) calculateBalanceScore(node *discovery.NodeTopology) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Count allocations on this node
	nodeAllocations := 0
	for _, alloc := range s.allocations {
		if alloc.NodeName == node.NodeName {
			nodeAllocations++
		}
	}

	// Prefer less loaded nodes
	totalGPUs := len(node.GPUs)
	if totalGPUs == 0 {
		return 0
	}

	usageRatio := float64(nodeAllocations) / float64(totalGPUs)
	score := int(100 * (1 - usageRatio))

	return maxInt(0, score)
}

// getAvailableGPUs returns GPUs available for allocation
func (s *TopologyAwareScheduler) getAvailableGPUs(node *discovery.NodeTopology) []*discovery.GPUDevice {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Get allocated GPU UUIDs on this node
	allocatedGPUs := make(map[string]bool)
	for _, alloc := range s.allocations {
		if alloc.NodeName == node.NodeName {
			for _, uuid := range alloc.GPUUUIDs {
				allocatedGPUs[uuid] = true
			}
		}
	}

	available := make([]*discovery.GPUDevice, 0)
	for _, gpu := range node.GPUs {
		if !allocatedGPUs[gpu.UUID] && gpu.Health.Status == discovery.HealthStatusHealthy {
			available = append(available, gpu)
		}
	}

	return available
}

// getMIGAvailability returns count of available MIG instances
func (s *TopologyAwareScheduler) getMIGAvailability(
	node *discovery.NodeTopology,
	req *MIGRequirements,
) int {
	count := 0
	for _, gpu := range node.GPUs {
		if gpu.MIGConfig == nil || !gpu.MIGConfig.Enabled {
			continue
		}
		for _, instance := range gpu.MIGConfig.CurrentInstances {
			if !instance.InUse && instance.Profile.Name == req.Profile {
				count++
			}
		}
	}
	return count
}

// tryScheduleOnNode attempts to schedule workload on a specific node
func (s *TopologyAwareScheduler) tryScheduleOnNode(
	ctx context.Context,
	workload *GPUWorkload,
	nodeScore NodeScore,
) (*SchedulingDecision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Verify GPUs are still available
	for _, uuid := range nodeScore.SelectedGPUs {
		for _, alloc := range s.allocations {
			if contains(alloc.GPUUUIDs, uuid) {
				return nil, fmt.Errorf("GPU %s no longer available", uuid)
			}
		}
	}

	decision := &SchedulingDecision{
		WorkloadUID:        workload.UID,
		SelectedNode:       nodeScore.NodeName,
		SelectedGPUs:       nodeScore.SelectedGPUs,
		Score:              nodeScore.TotalScore,
		Reasons:            nodeScore.Reasons,
		EstimatedBandwidth: s.estimateBandwidth(nodeScore),
		Timestamp:          time.Now(),
	}

	return decision, nil
}

// estimateBandwidth estimates inter-GPU bandwidth for the placement
func (s *TopologyAwareScheduler) estimateBandwidth(score NodeScore) float64 {
	if len(score.SelectedGPUs) <= 1 {
		return 0
	}

	// Get topology from discovery service
	nodeTopology, ok := s.discoveryService.GetNodeTopology(score.NodeName)
	if !ok {
		return 0
	}

	totalBandwidth := 0.0
	pairs := 0

	for i, uuid1 := range score.SelectedGPUs {
		for j, uuid2 := range score.SelectedGPUs {
			if i >= j {
				continue
			}
			for _, gpu := range nodeTopology.GPUs {
				if gpu.UUID == uuid1 {
					if nvlink, ok := gpu.Topology.NVLinkConnections[uuid2]; ok {
						totalBandwidth += nvlink.BandwidthGBps
					} else {
						totalBandwidth += gpu.Topology.PCIeInfo.BandwidthGBps
					}
					pairs++
				}
			}
		}
	}

	if pairs == 0 {
		return 0
	}
	return totalBandwidth / float64(pairs)
}

// recordAllocation records a GPU allocation
func (s *TopologyAwareScheduler) recordAllocation(workload *GPUWorkload, decision *SchedulingDecision) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.allocations[workload.UID] = &GPUAllocation{
		WorkloadUID:  workload.UID,
		NodeName:     decision.SelectedNode,
		GPUUUIDs:     decision.SelectedGPUs,
		MIGInstances: decision.MIGInstances,
		AllocatedAt:  time.Now(),
		WorkloadType: workload.WorkloadSpec.Type,
	}
}

// ReleaseAllocation releases GPU allocation for a workload
func (s *TopologyAwareScheduler) ReleaseAllocation(workloadUID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.allocations[workloadUID]; !ok {
		return fmt.Errorf("allocation not found for workload %s", workloadUID)
	}

	delete(s.allocations, workloadUID)

	s.eventChan <- SchedulingEvent{
		Type:        EventTypeReleased,
		WorkloadUID: workloadUID,
		Timestamp:   time.Now(),
	}

	return nil
}

// scheduleWithPreemption attempts scheduling via preemption
func (s *TopologyAwareScheduler) scheduleWithPreemption(
	ctx context.Context,
	workload *GPUWorkload,
	topology *discovery.ClusterTopology,
) (*SchedulingDecision, error) {
	candidates := s.findPreemptionCandidates(workload, topology)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no preemption candidates found")
	}

	// Sort by preemption cost (lower is better)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].PreemptionCost < candidates[j].PreemptionCost
	})

	// Try preempting lowest cost candidate
	candidate := candidates[0]

	// Release the candidate's allocation
	s.ReleaseAllocation(candidate.Pod.Name) // Using name as UID for simplicity

	s.metrics.PreemptionCount++
	s.eventChan <- SchedulingEvent{
		Type:        EventTypePreempted,
		WorkloadUID: candidate.Pod.Name,
		Timestamp:   time.Now(),
	}

	// Retry scheduling
	return s.Schedule(ctx, workload)
}

// findPreemptionCandidates finds workloads that can be preempted
func (s *TopologyAwareScheduler) findPreemptionCandidates(
	workload *GPUWorkload,
	topology *discovery.ClusterTopology,
) []PreemptionCandidate {
	s.mu.RLock()
	defer s.mu.RUnlock()

	candidates := make([]PreemptionCandidate, 0)

	for uid, alloc := range s.allocations {
		// Can't preempt higher priority workloads
		// In production, get priority from workload spec
		if alloc.WorkloadType == WorkloadTypeTraining {
			// Training workloads have implicit lower preemption cost
			cost := float64(time.Since(alloc.AllocatedAt).Minutes())

			candidates = append(candidates, PreemptionCandidate{
				Pod:            PodReference{Name: uid},
				NodeName:       alloc.NodeName,
				GPUs:           alloc.GPUUUIDs,
				PreemptionCost: cost,
				Age:            time.Since(alloc.AllocatedAt),
			})
		}
	}

	return candidates
}

// GetMetrics returns scheduler metrics
func (s *TopologyAwareScheduler) GetMetrics() SchedulerMetrics {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return *s.metrics
}

// Events returns the event channel
func (s *TopologyAwareScheduler) Events() <-chan SchedulingEvent {
	return s.eventChan
}

// updateLatencyMetrics updates latency tracking
func (s *TopologyAwareScheduler) updateLatencyMetrics(latencyMs float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Simple moving average
	total := float64(s.metrics.TotalScheduled + s.metrics.TotalFailed)
	if total == 0 {
		s.metrics.AverageLatencyMs = latencyMs
	} else {
		s.metrics.AverageLatencyMs = (s.metrics.AverageLatencyMs*total + latencyMs) / (total + 1)
	}

	// Track P99 (simplified)
	s.metrics.P99LatencyMs = math.Max(s.metrics.P99LatencyMs, latencyMs)
}

// Helper functions
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
