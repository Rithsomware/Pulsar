"""
PULSAR Configuration

Loads cluster configuration from YAML files. Defines all configurable
parameters for the PULSAR control plane.
"""

import os
import yaml
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger("pulsar.config")

DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "pulsar.yaml")


@dataclass
class PreemptionConfig:
    enabled: bool = True
    grace_period_seconds: int = 30
    max_preemptions_per_cycle: int = 3


@dataclass
class FallbackConfig:
    preferred_gpu_class: str = "dgpu"
    fallback_gpu_class: str = "igpu"
    max_dgpu_wait_seconds: float = 0.0


@dataclass
class QueueControllerConfig:
    aging_enabled: bool = True
    aging_boost_interval_seconds: float = 60.0
    starvation_threshold_seconds: float = 300.0
    max_queue_depth_per_tenant: int = 50


@dataclass
class SchedulingConfig:
    policy: str = "fair_share"  # fifo | fair_share | priority | backfill | drf
    scheduling_interval_seconds: float = 2.0
    preemption: PreemptionConfig = field(default_factory=PreemptionConfig)
    fallback: FallbackConfig = field(default_factory=FallbackConfig)
    queue_controller: QueueControllerConfig = field(default_factory=QueueControllerConfig)


@dataclass
class QuotaConfig:
    max_gpus: int = 8
    max_jobs: int = 10
    weight: float = 1.0  # for weighted fair sharing


@dataclass
class APIConfig:
    host: str = "0.0.0.0"
    port: int = 8080


@dataclass
class PersistenceConfig:
    database: str = "pulsar.db"
    enabled: bool = True


@dataclass
class LoggingConfig:
    level: str = "INFO"
    format: str = "text"  # text | json


@dataclass
class ClusterConfig:
    total_gpus: int = 16
    gpu_memory_gb: int = 80
    gpu_memory_mb: int = 0  # if set, takes precedence over gpu_memory_gb
    gpu_resource_name: str = "nvidia.com/gpu"  # nvidia.com/gpu | amd.com/gpu | custom
    dgpu_resource_name: str = "nvidia.com/gpu"
    igpu_resource_name: str = "gpu.intel.com/i915"
    nodes: List[str] = field(default_factory=lambda: ["gpu-node-1", "gpu-node-2"])


@dataclass
class PulsarConfig:
    """Root configuration for the PULSAR control plane."""
    cluster: ClusterConfig = field(default_factory=ClusterConfig)
    scheduling: SchedulingConfig = field(default_factory=SchedulingConfig)
    quotas: Dict[str, QuotaConfig] = field(default_factory=dict)
    api: APIConfig = field(default_factory=APIConfig)
    persistence: PersistenceConfig = field(default_factory=PersistenceConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    @classmethod
    def from_yaml(cls, path: str) -> "PulsarConfig":
        """Load configuration from a YAML file."""
        with open(path, "r") as f:
            raw = yaml.safe_load(f) or {}

        config = cls()

        # Cluster
        if "cluster" in raw:
            c = raw["cluster"]
            config.cluster = ClusterConfig(
                total_gpus=c.get("total_gpus", 16),
                gpu_memory_gb=c.get("gpu_memory_gb", 80),
                gpu_resource_name=c.get("gpu_resource_name", "nvidia.com/gpu"),
                dgpu_resource_name=c.get("dgpu_resource_name", c.get("gpu_resource_name", "nvidia.com/gpu")),
                igpu_resource_name=c.get("igpu_resource_name", "gpu.intel.com/i915"),
                nodes=c.get("nodes", ["gpu-node-1", "gpu-node-2"]),
            )

        # Scheduling
        if "scheduling" in raw:
            s = raw["scheduling"]
            preemption = PreemptionConfig()
            if "preemption" in s:
                p = s["preemption"]
                preemption = PreemptionConfig(
                    enabled=p.get("enabled", True),
                    grace_period_seconds=p.get("grace_period_seconds", 30),
                    max_preemptions_per_cycle=p.get("max_preemptions_per_cycle", 3),
                )
            fallback = FallbackConfig()
            if "fallback" in s:
                f = s["fallback"]
                fallback = FallbackConfig(
                    preferred_gpu_class=f.get("preferred_gpu_class", "dgpu"),
                    fallback_gpu_class=f.get("fallback_gpu_class", "igpu"),
                    max_dgpu_wait_seconds=f.get("max_dgpu_wait_seconds", 0.0),
                )
            queue_ctrl = QueueControllerConfig()
            if "queue_controller" in s:
                qc = s["queue_controller"]
                queue_ctrl = QueueControllerConfig(
                    aging_enabled=qc.get("aging_enabled", True),
                    aging_boost_interval_seconds=qc.get("aging_boost_interval_seconds", 60.0),
                    starvation_threshold_seconds=qc.get("starvation_threshold_seconds", 300.0),
                    max_queue_depth_per_tenant=qc.get("max_queue_depth_per_tenant", 50),
                )
            config.scheduling = SchedulingConfig(
                policy=s.get("policy", "fair_share"),
                scheduling_interval_seconds=s.get("scheduling_interval_seconds", 2.0),
                preemption=preemption,
                fallback=fallback,
                queue_controller=queue_ctrl,
            )

        # Quotas
        if "quotas" in raw:
            for user, q in raw["quotas"].items():
                config.quotas[user] = QuotaConfig(
                    max_gpus=q.get("max_gpus", 8),
                    max_jobs=q.get("max_jobs", 10),
                    weight=q.get("weight", 1.0),
                )

        # API
        if "api" in raw:
            a = raw["api"]
            config.api = APIConfig(
                host=a.get("host", "0.0.0.0"),
                port=a.get("port", 8080),
            )

        # Persistence
        if "persistence" in raw:
            pe = raw["persistence"]
            config.persistence = PersistenceConfig(
                database=pe.get("database", "pulsar.db"),
                enabled=pe.get("enabled", True),
            )

        # Logging
        if "logging" in raw:
            lo = raw["logging"]
            config.logging = LoggingConfig(
                level=lo.get("level", "INFO"),
                format=lo.get("format", "text"),
            )

        logger.info("Loaded PULSAR config from %s", path)
        return config

    @classmethod
    def load(cls, path: Optional[str] = None) -> "PulsarConfig":
        """Load config from path, env var, or default."""
        if path is None:
            path = os.environ.get("PULSAR_CONFIG", DEFAULT_CONFIG_PATH)
        if os.path.exists(path):
            return cls.from_yaml(path)
        logger.warning("Config file %s not found, using defaults", path)
        return cls()

    def setup_logging(self):
        """Configure Python logging from config."""
        level = getattr(logging, self.logging.level.upper(), logging.INFO)
        fmt = (
            '{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}'
            if self.logging.format == "json"
            else "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        )
        logging.basicConfig(level=level, format=fmt, force=True)
