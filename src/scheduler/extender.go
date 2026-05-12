// Package scheduler implements the Kubernetes scheduler extender for GPU workload management.
// It provides HTTP endpoints for filter, prioritize, and preempt operations,
// alongside pod annotation injection for GPU class preferences.
package scheduler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulerapi "k8s.io/kube-scheduler/extender/v1"
)

// SchedulerExtender implements the Kubernetes scheduler extender API.
type SchedulerExtender struct {
	config           SchedulerConfig
	discoveryService DiscoveryService
	metrics          *SchedulerMetrics
}

// DiscoveryService interface for GPU topology queries.
type DiscoveryService interface {
	GetClusterTopology() *ClusterTopology
	GetNodeGPUCount(nodeName string) int
	GetNodeGPUMemoryGB(nodeName string) int
	GetNodeGPUClass(nodeName string) string
}

// ClusterTopology is a lightweight topology view for the extender.
type ClusterTopology struct {
	Nodes map[string]*NodeTopologyInfo
}

// NodeTopologyInfo holds per-node GPU info.
type NodeTopologyInfo struct {
	GPUCount    int
	GPUMemoryGB int
	GPUClass    string
	GPUModels   []string
	HealthyGPUs int
}

// NewSchedulerExtender creates a new extender instance.
func NewSchedulerExtender(config SchedulerConfig, ds DiscoveryService) *SchedulerExtender {
	return &SchedulerExtender{
		config:           config,
		discoveryService: ds,
		metrics:          &SchedulerMetrics{},
	}
}

// RegisterHandlers registers extender HTTP endpoints.
func (e *SchedulerExtender) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/filter", e.filterHandler)
	mux.HandleFunc("/prioritize", e.prioritizeHandler)
	mux.HandleFunc("/preempt", e.preemptHandler)
	mux.HandleFunc("/bind", e.bindHandler)
	mux.HandleFunc("/healthz", e.healthzHandler)
}

// filterHandler handles the scheduler extender filter request.
func (e *SchedulerExtender) filterHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var args schedulerapi.ExtenderArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	filtered := e.filterNodes(args.Pod, args.Nodes.Items)
	result := schedulerapi.ExtenderFilterResult{
		Nodes: &v1.NodeList{Items: filtered},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)

	e.metrics.TotalScheduled++
	e.metrics.AverageLatencyMs = float64(time.Since(start).Milliseconds())
}

// filterNodes removes nodes that cannot satisfy GPU requirements.
func (e *SchedulerExtender) filterNodes(pod *v1.Pod, nodes []v1.Node) []v1.Node {
	gpuReq := extractGPURequest(pod)
	gpuMemReq := extractGPUMemoryRequest(pod)
	preferredClass := extractPreferredGPUClass(pod)

	var filtered []v1.Node
	for _, node := range nodes {
		nodeName := node.Name
		nodeGPUCount := e.discoveryService.GetNodeGPUCount(nodeName)
		nodeGPUMem := e.discoveryService.GetNodeGPUMemoryGB(nodeName)
		nodeClass := e.discoveryService.GetNodeGPUClass(nodeName)

		if nodeGPUCount < gpuReq {
			continue
		}
		if gpuMemReq > 0 && nodeGPUMem < gpuMemReq {
			continue
		}
		// GPU class matching: if node has no matching class, skip unless fallback allowed
		if preferredClass != "" && preferredClass != "any" {
			if nodeClass != "" && nodeClass != preferredClass {
				// Check if fallback is allowed via annotation
				if pod.Annotations["kgwe.nvidia.io/gpu-fallback-allowed"] != "true" {
					continue
				}
			}
		}
		filtered = append(filtered, node)
	}
	return filtered
}

// prioritizeHandler handles the scheduler extender prioritize request.
func (e *SchedulerExtender) prioritizeHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var args schedulerapi.ExtenderArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	scores := e.prioritizeNodes(args.Pod, args.Nodes.Items)
	result := schedulerapi.HostPriorityList(scores)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)

	e.metrics.AverageLatencyMs = float64(time.Since(start).Milliseconds())
}

// prioritizeNodes scores nodes based on GPU topology and class preference.
func (e *SchedulerExtender) prioritizeNodes(pod *v1.Pod, nodes []v1.Node) schedulerapi.HostPriorityList {
	preferredClass := extractPreferredGPUClass(pod)
	var scores schedulerapi.HostPriorityList

	for _, node := range nodes {
		nodeName := node.Name
		score := 50 // base score

		nodeGPUCount := e.discoveryService.GetNodeGPUCount(nodeName)
		nodeClass := e.discoveryService.GetNodeGPUClass(nodeName)

		// Prefer nodes with more available GPUs
		score += min(nodeGPUCount*5, 30)

		// GPU class preference bonus
		if preferredClass != "" && preferredClass != "any" {
			if nodeClass == preferredClass {
				score += 20
			} else if nodeClass != "" {
				score -= 10
			}
		}

		scores = append(scores, schedulerapi.HostPriority{
			Host:  nodeName,
			Score: int64(minInt(score, 100)),
		})
	}

	sort.Slice(scores, func(i, j int) bool {
		return scores[i].Score > scores[j].Score
	})
	return scores
}

// preemptHandler handles preemption requests from the scheduler.
func (e *SchedulerExtender) preemptHandler(w http.ResponseWriter, r *http.Request) {
	var args schedulerapi.ExtenderPreemptionArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result := e.findPreemptionVictims(&args)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// findPreemptionVictims identifies pods to preempt on a node.
func (e *SchedulerExtender) findPreemptionVictims(
	args *schedulerapi.ExtenderPreemptionArgs,
) schedulerapi.ExtenderPreemptionResult {
	gpuReq := extractGPURequest(args.Pod)
	result := schedulerapi.ExtenderPreemptionResult{
		NodeNameToMetaVictims: make(map[string]*schedulerapi.MetaVictims),
	}
	if gpuReq == 0 {
		return result
	}

	for nodeName, victimsInfo := range args.NodeNameToVictims {
		var preemptionVictims []*v1.Pod
		var freedGPUs int

		potentialVictims := victimsInfo.Pods

		// Sort by priority ascending (lowest priority first)
		sort.Slice(potentialVictims, func(i, j int) bool {
			pi := getPodPriority(potentialVictims[i])
			pj := getPodPriority(potentialVictims[j])
			if pi == pj {
				return potentialVictims[i].CreationTimestamp.Before(&potentialVictims[j].CreationTimestamp)
			}
			return pi < pj
		})

		for _, victim := range potentialVictims {
			if freedGPUs >= gpuReq {
				break
			}
			if isPreemptible(victim) && getPodPriority(victim) < getPodPriority(args.Pod) {
				vGPU := extractGPURequest(victim)
				if vGPU > 0 {
					preemptionVictims = append(preemptionVictims, victim)
					freedGPUs += vGPU
				}
			}
		}

		if freedGPUs >= gpuReq {
			var metaPods []*schedulerapi.MetaPod
			for _, v := range preemptionVictims {
				metaPods = append(metaPods, &schedulerapi.MetaPod{UID: string(v.UID)})
			}
			result.NodeNameToMetaVictims[nodeName] = &schedulerapi.MetaVictims{
				Pods: metaPods,
			}
		}
	}

	return result
}

// bindHandler handles the bind request (optional, can delegate to default scheduler).
func (e *SchedulerExtender) bindHandler(w http.ResponseWriter, r *http.Request) {
	var args schedulerapi.ExtenderBindingArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Inject GPU annotations into the pod before binding
	if err := e.injectAnnotations(args.PodName, args.PodNamespace, args.Node); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	result := schedulerapi.ExtenderBindingResult{Error: ""}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// injectAnnotations adds GPU class and fallback annotations to pods.
func (e *SchedulerExtender) injectAnnotations(podName, podNamespace, nodeName string) error {
	// In a real implementation, this would use a Kubernetes client to patch the pod.
	// Here we log the intent.
	nodeClass := e.discoveryService.GetNodeGPUClass(nodeName)
	annotations := map[string]string{
		"kgwe.nvidia.io/assigned-node":      nodeName,
		"kgwe.nvidia.io/assigned-gpu-class": nodeClass,
		"kgwe.nvidia.io/scheduled-at":       time.Now().Format(time.RFC3339),
	}

	if nodeClass == "igpu" {
		annotations["kgwe.nvidia.io/fallback-applied"] = "true"
		annotations["kgwe.nvidia.io/fallback-reason"] = "node-capacity"
	}

	_ = annotations // used in real K8s client patch
	return nil
}

// healthzHandler health check endpoint.
func (e *SchedulerExtender) healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// ─── Helper functions ──────────────────────────────────────────────

func extractGPURequest(pod *v1.Pod) int {
	var total int
	for _, c := range pod.Spec.Containers {
		if val, ok := c.Resources.Limits["nvidia.com/gpu"]; ok {
			total += int(val.Value())
		}
		if val, ok := c.Resources.Limits["amd.com/gpu"]; ok {
			total += int(val.Value())
		}
	}
	return total
}

func extractGPUMemoryRequest(pod *v1.Pod) int {
	if val, ok := pod.Annotations["kgwe.nvidia.io/gpu-memory-gb"]; ok {
		var mem int
		fmt.Sscanf(val, "%d", &mem)
		return mem
	}
	return 0
}

func extractPreferredGPUClass(pod *v1.Pod) string {
	if val, ok := pod.Annotations["kgwe.nvidia.io/preferred-gpu-class"]; ok {
		return strings.ToLower(val)
	}
	return "dgpu"
}

func isPreemptible(pod *v1.Pod) bool {
	if val, ok := pod.Annotations["kgwe.nvidia.io/preemptible"]; ok {
		return val == "true"
	}
	return true
}

func getPodPriority(pod *v1.Pod) int32 {
	if pod.Spec.Priority != nil {
		return *pod.Spec.Priority
	}
	return 0
}


