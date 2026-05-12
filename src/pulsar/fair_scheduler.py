"""
PULSAR Fair Scheduler

Production fairness-aware GPU scheduling with multiple algorithms:
- Inverse proportional fairness (DRF-inspired)
- Weighted fair sharing
- Jain's fairness index computation
"""

import logging
import math
import threading
from collections import defaultdict
from typing import Dict, List, Tuple, Optional

from pulsar.pulsar_types import GPUJob, PulsarEvent

logger = logging.getLogger("pulsar.fairness")


class FairScheduler:
    """
    Fairness-aware scheduler for multi-tenant GPU clusters.

    Supports weighted fair sharing where each user/team has a configurable
    weight that determines their fair share of cluster resources.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._usage: Dict[str, float] = defaultdict(float)
        self._active_allocations: Dict[str, int] = defaultdict(int)
        self._allocation_count: Dict[str, int] = defaultdict(int)
        self._weights: Dict[str, float] = defaultdict(lambda: 1.0)
        self._events: List[PulsarEvent] = []

        # DRF multi-dimensional resource tracking
        self._total_resources: Dict[str, float] = {"gpu_count": 0.0, "gpu_memory_gb": 0.0}
        self._resource_usage: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self._resource_weights: Dict[str, float] = {"gpu_count": 1.0, "gpu_memory_gb": 1.0}

    def set_weight(self, user: str, weight: float):
        """Set the fair-share weight for a user. Higher weight = larger share."""
        with self._lock:
            self._weights[user] = max(0.1, weight)
            logger.info("Fair-share weight for %s set to %.2f", user, weight)

    def update_usage(self, user: str, gpu_count: int) -> PulsarEvent:
        """Record a GPU allocation for a user."""
        with self._lock:
            self._usage[user] += gpu_count
            self._active_allocations[user] += gpu_count
            self._allocation_count[user] += 1
            priority = self._compute_priority_unlocked(user)

            event = PulsarEvent(
                event_type="FAIRNESS",
                message=f"Allocated {gpu_count} GPUs to {user}",
                user=user,
                metadata={
                    "cumulative_usage": self._usage[user],
                    "active_gpus": self._active_allocations[user],
                    "priority": f"{priority:.4f}",
                },
            )
            self._events.append(event)
            logger.info(event.format())
            return event

    def release_usage(self, user: str, gpu_count: int) -> PulsarEvent:
        """Record GPU deallocation. Cumulative usage is NOT reduced."""
        with self._lock:
            self._active_allocations[user] = max(0, self._active_allocations[user] - gpu_count)
            event = PulsarEvent(
                event_type="FAIRNESS",
                message=f"Released {gpu_count} GPUs from {user}",
                user=user,
                metadata={
                    "active_gpus": self._active_allocations[user],
                    "cumulative_usage": self._usage[user],
                },
            )
            self._events.append(event)
            logger.info(event.format())
            return event

    def set_total_resources(self, gpu_count: float, gpu_memory_gb: float):
        """Set total cluster resources for DRF calculations."""
        with self._lock:
            self._total_resources["gpu_count"] = max(1.0, gpu_count)
            self._total_resources["gpu_memory_gb"] = max(1.0, gpu_memory_gb)
            logger.info("Total resources set: gpu_count=%.0f, gpu_memory_gb=%.0f",
                        gpu_count, gpu_memory_gb)

    def update_resource_usage(self, user: str, gpu_count: int, gpu_memory_gb: int = 0):
        """Record multi-dimensional resource allocation for DRF."""
        with self._lock:
            self._resource_usage[user]["gpu_count"] += float(gpu_count)
            self._resource_usage[user]["gpu_memory_gb"] += float(gpu_memory_gb)

    def release_resource_usage(self, user: str, gpu_count: int, gpu_memory_gb: int = 0):
        """Release multi-dimensional resource allocation for DRF."""
        with self._lock:
            self._resource_usage[user]["gpu_count"] = max(
                0.0, self._resource_usage[user]["gpu_count"] - float(gpu_count)
            )
            self._resource_usage[user]["gpu_memory_gb"] = max(
                0.0, self._resource_usage[user]["gpu_memory_gb"] - float(gpu_memory_gb)
            )

    def compute_dominant_share(self, user: str) -> Tuple[str, float]:
        """
        Compute the dominant resource share for a user.
        Returns (resource_name, share_fraction).
        """
        with self._lock:
            user_usage = self._resource_usage.get(user, {})
            weight = self._weights.get(user, 1.0)
            shares = {}
            for resource, total in self._total_resources.items():
                if total > 0:
                    # weighted fair share: user's share = usage / (total * weight)
                    usage = user_usage.get(resource, 0.0)
                    shares[resource] = usage / total / weight if weight > 0 else 0.0
            if not shares:
                return ("gpu_count", 0.0)
            dominant = max(shares, key=shares.get)
            return (dominant, shares[dominant])

    def compute_drf_shares(self) -> Dict[str, Dict[str, float]]:
        """
        Compute DRF shares for all users.
        Returns user -> {resource: share_fraction}.
        """
        with self._lock:
            all_users = set(self._resource_usage.keys()) | set(self._active_allocations.keys())
            result = {}
            for user in all_users:
                weight = self._weights.get(user, 1.0)
                user_usage = self._resource_usage.get(user, {})
                shares = {}
                for resource, total in self._total_resources.items():
                    if total > 0:
                        usage = user_usage.get(resource, 0.0)
                        shares[resource] = usage / total / weight if weight > 0 else 0.0
                dominant, dominant_share = self.compute_dominant_share(user)
                result[user] = {
                    "shares": shares,
                    "dominant_resource": dominant,
                    "dominant_share": dominant_share,
                }
            return result

    def get_drf_priority(self, user: str) -> float:
        """
        DRF priority: lower dominant share = higher priority.
        Returns inverse of dominant share so higher = more deserving.
        """
        _, dominant_share = self.compute_dominant_share(user)
        # Add small epsilon to avoid division by zero
        return 1.0 / (1.0 + dominant_share)

    def get_priority(self, user: str) -> float:
        """
        Compute weighted fairness priority.

        priority = weight / (1 + cumulative_gpu_usage)
        Higher weight and lower usage both increase priority.
        """
        with self._lock:
            return self._compute_priority_unlocked(user)

    def _compute_priority_unlocked(self, user: str) -> float:
        weight = self._weights.get(user, 1.0)
        usage = self._usage.get(user, 0)
        return weight / (1.0 + usage)

    def get_all_priorities(self) -> Dict[str, float]:
        with self._lock:
            all_users = set(self._usage.keys()) | set(self._active_allocations.keys())
            return {u: self._compute_priority_unlocked(u) for u in all_users}

    def select_user(self, candidates: List[str]) -> Tuple[str, float]:
        """Select the most deserving user from candidates."""
        if not candidates:
            raise ValueError("No candidates provided")
        with self._lock:
            scores = [(u, self._compute_priority_unlocked(u)) for u in candidates]
        scores.sort(key=lambda x: x[1], reverse=True)
        selected, score = scores[0]
        logger.info("[PULSAR] [FAIRNESS] Selected %s (score=%.4f) from %d candidates",
                     selected, score, len(candidates))
        return selected, score

    def compute_jains_fairness_index(self) -> float:
        """
        Compute Jain's Fairness Index across all users.

        J(x) = (sum(xi))^2 / (n * sum(xi^2))
        Returns value between 0 (unfair) and 1 (perfectly fair).
        """
        with self._lock:
            if not self._usage:
                return 1.0
            values = list(self._usage.values())
            n = len(values)
            if n == 0:
                return 1.0
            sum_x = sum(values)
            sum_x2 = sum(x * x for x in values)
            if sum_x2 == 0:
                return 1.0
            return (sum_x ** 2) / (n * sum_x2)

    def get_fair_share(self, total_gpus: int) -> Dict[str, float]:
        """Compute each user's fair share of GPUs based on weights."""
        with self._lock:
            total_weight = sum(self._weights.get(u, 1.0) for u in self._usage.keys())
            if total_weight == 0:
                return {}
            return {
                u: (self._weights.get(u, 1.0) / total_weight) * total_gpus
                for u in self._usage.keys()
            }

    def get_fair_share_violation(self, total_gpus: int) -> Dict[str, float]:
        """Compute deviation from fair share (positive = over-consuming)."""
        fair_shares = self.get_fair_share(total_gpus)
        with self._lock:
            return {
                u: self._active_allocations.get(u, 0) - fair_shares.get(u, 0)
                for u in fair_shares
            }

    def get_fairness_report(self) -> Dict[str, dict]:
        with self._lock:
            report = {}
            total_usage = sum(self._usage.values()) or 1
            drf_shares = self.compute_drf_shares()
            for user in set(self._usage.keys()) | set(self._active_allocations.keys()):
                usage = self._usage.get(user, 0)
                drf = drf_shares.get(user, {})
                report[user] = {
                    "cumulative_gpu_usage": usage,
                    "active_gpus": self._active_allocations.get(user, 0),
                    "allocation_count": self._allocation_count.get(user, 0),
                    "weight": self._weights.get(user, 1.0),
                    "fairness_priority": self._compute_priority_unlocked(user),
                    "drf_priority": self.get_drf_priority(user),
                    "usage_share": usage / total_usage,
                    "drf_dominant_resource": drf.get("dominant_resource", "unknown"),
                    "drf_dominant_share": round(drf.get("dominant_share", 0.0), 6),
                    "drf_gpu_count_share": round(drf.get("shares", {}).get("gpu_count", 0.0), 6),
                    "drf_gpu_memory_share": round(drf.get("shares", {}).get("gpu_memory_gb", 0.0), 6),
                }
            return report

    @property
    def events(self) -> List[PulsarEvent]:
        return self._events
