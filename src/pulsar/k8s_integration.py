"""
PULSAR Kubernetes Integration

Provides CRD definitions and operator hooks for deploying PULSAR
as a Kubernetes-native controller. Works in standalone mode when
K8s is unavailable.
"""

import logging
import json
from typing import Dict, Optional

logger = logging.getLogger("pulsar.k8s")

# CRD definition for GPUWorkload custom resource
GPU_WORKLOAD_CRD = {
    "apiVersion": "apiextensions.k8s.io/v1",
    "kind": "CustomResourceDefinition",
    "metadata": {
        "name": "gpuworkloads.pulsar.io",
    },
    "spec": {
        "group": "pulsar.io",
        "versions": [{
            "name": "v1alpha1",
            "served": True,
            "storage": True,
            "schema": {
                "openAPIV3Schema": {
                    "type": "object",
                    "properties": {
                        "spec": {
                            "type": "object",
                            "required": ["user", "gpuRequired"],
                            "properties": {
                                "user": {"type": "string"},
                                "gpuRequired": {"type": "integer", "minimum": 1},
                                "gpuMemoryGB": {"type": "integer", "default": 0},
                                "priority": {
                                    "type": "string",
                                    "enum": ["LOW", "NORMAL", "HIGH", "CRITICAL"],
                                    "default": "NORMAL",
                                },
                                "preemptible": {"type": "boolean", "default": True},
                                "workloadType": {"type": "string", "default": "Training"},
                                "framework": {"type": "string", "default": "PyTorch"},
                                "estimatedDurationMinutes": {"type": "number", "default": 60},
                                "preferredGpuClass": {
                                    "type": "string",
                                    "enum": ["dgpu", "igpu"],
                                    "default": "dgpu",
                                },
                                "image": {"type": "string"},
                                "command": {"type": "array", "items": {"type": "string"}},
                                "env": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "name": {"type": "string"},
                                            "value": {"type": "string"},
                                        },
                                    },
                                },
                            },
                        },
                        "status": {
                            "type": "object",
                            "properties": {
                                "state": {"type": "string"},
                                "jobId": {"type": "string"},
                                "assignedNode": {"type": "string"},
                                "assignedGpuClass": {"type": "string"},
                                "fallbackApplied": {"type": "boolean"},
                                "fallbackReason": {"type": "string"},
                                "startedAt": {"type": "string"},
                                "completedAt": {"type": "string"},
                                "message": {"type": "string"},
                            },
                        },
                    },
                },
            },
            "subresources": {"status": {}},
            "additionalPrinterColumns": [
                {"name": "User", "type": "string", "jsonPath": ".spec.user"},
                {"name": "GPUs", "type": "integer", "jsonPath": ".spec.gpuRequired"},
                {"name": "Priority", "type": "string", "jsonPath": ".spec.priority"},
                {"name": "Status", "type": "string", "jsonPath": ".status.state"},
                {"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp"},
            ],
        }],
        "scope": "Namespaced",
        "names": {
            "plural": "gpuworkloads",
            "singular": "gpuworkload",
            "kind": "GPUWorkload",
            "shortNames": ["gw"],
        },
    },
}

# Example GPUWorkload CR
EXAMPLE_CR = {
    "apiVersion": "pulsar.io/v1alpha1",
    "kind": "GPUWorkload",
    "metadata": {"name": "training-job-1", "namespace": "ml-team-alpha"},
    "spec": {
        "user": "ml-team-alpha",
        "gpuRequired": 4,
        "gpuMemoryGB": 80,
        "priority": "HIGH",
        "preemptible": False,
        "workloadType": "Training",
        "framework": "PyTorch",
        "estimatedDurationMinutes": 120,
        "preferredGpuClass": "dgpu",
        "image": "pytorch/pytorch:2.2.0-cuda12.1-cudnn8-runtime",
        "command": ["python", "train.py", "--model=gpt2-xl"],
        "env": [
            {"name": "MASTER_ADDR", "value": "localhost"},
            {"name": "NCCL_DEBUG", "value": "INFO"},
        ],
    },
}


def export_crd_yaml() -> str:
    """Export the GPUWorkload CRD as YAML."""
    import yaml
    return yaml.dump(GPU_WORKLOAD_CRD, default_flow_style=False)


def export_example_cr() -> str:
    """Export an example GPUWorkload CR as YAML."""
    import yaml
    return yaml.dump(EXAMPLE_CR, default_flow_style=False)


class K8sIntegration:
    """
    Kubernetes integration for PULSAR.

    In production, watches for GPUWorkload CRs and auto-submits them
    to the PULSAR control plane. Falls back to standalone mode when
    K8s is unavailable.
    """

    def __init__(self, control_plane=None):
        self._cp = control_plane
        self._k8s_available = False
        self._try_init_k8s()

    def _try_init_k8s(self):
        try:
            from kubernetes import client, config as k8s_config
            try:
                k8s_config.load_incluster_config()
            except k8s_config.ConfigException:
                k8s_config.load_kube_config()
            self._k8s_client = client.CustomObjectsApi()
            self._core_v1_api = client.CoreV1Api()
            self._k8s_available = True
            logger.info("Kubernetes integration enabled")
        except (ImportError, Exception) as e:
            logger.info("Running in standalone mode (no K8s): %s", e)
            self._k8s_available = False

    @property
    def is_k8s_available(self) -> bool:
        return self._k8s_available

    def cr_to_job_spec(self, cr: dict) -> dict:
        """Convert a GPUWorkload CR to PULSAR job submission format."""
        spec = cr.get("spec", {})
        return {
            "user": spec.get("user", "unknown"),
            "gpu_required": spec.get("gpuRequired", 1),
            "gpu_memory_gb": spec.get("gpuMemoryGB", 0),
            "priority": spec.get("priority", "NORMAL"),
            "preemptible": spec.get("preemptible", True),
            "workload_type": spec.get("workloadType", "Training"),
            "framework": spec.get("framework", "PyTorch"),
            "estimated_duration_minutes": spec.get("estimatedDurationMinutes", 60),
            "preferred_gpu_class": spec.get("preferredGpuClass", "dgpu"),
        }

    def patch_pod_annotations(self, pod_name: str, namespace: str, annotations: dict) -> bool:
        """Patch pod with given annotations."""
        if not self.is_k8s_available:
            logger.warning("K8s not available, skipping pod patch for %s", pod_name)
            return False
            
        try:
            body = {"metadata": {"annotations": annotations}}
            self._core_v1_api.patch_namespaced_pod(
                name=pod_name, namespace=namespace, body=body
            )
            logger.info("Successfully patched annotations for pod %s/%s", namespace, pod_name)
            return True
        except Exception as e:
            logger.error("Failed to patch pod %s/%s: %s", namespace, pod_name, e)
            return False
