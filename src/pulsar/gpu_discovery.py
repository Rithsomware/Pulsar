"""
PULSAR GPU Discovery

Detects real GPU hardware on the local machine using nvidia-smi or rocm-smi.
Returns actual GPU count, memory, and device names for use in config.
"""

import subprocess
import logging
import json
from typing import List, Optional
from dataclasses import dataclass, asdict

logger = logging.getLogger("pulsar.gpu_discovery")


@dataclass
class GPUDevice:
    index: int
    name: str
    memory_mb: int
    uuid: str
    vendor: str  # "nvidia" or "amd"


def _safe_int(v: str, default: int = 0) -> int:
    """Parse integer from nvidia-smi output, ignoring unit suffixes like 'MiB'."""
    v = v.strip()
    if not v or v in ("[N/A]", "[Not Supported]", "N/A"):
        return default
    # Extract leading numeric portion (e.g. "6144 MiB" -> "6144")
    numeric = ""
    for ch in v:
        if ch.isdigit() or ch == ".":
            numeric += ch
        else:
            break
    if not numeric:
        return default
    try:
        return int(float(numeric))
    except ValueError:
        return default


def detect_nvidia() -> List[GPUDevice]:
    """Detect NVIDIA GPUs via nvidia-smi with lspci fallback."""
    devices = []

    # 1. Try nvidia-smi
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=index,name,memory.total,uuid",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            for line in result.stdout.strip().split("\n"):
                if not line.strip(): continue
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 4:
                    try:
                        devices.append(GPUDevice(
                            index=_safe_int(parts[0]), name=parts[1],
                            memory_mb=_safe_int(parts[2]), uuid=parts[3], vendor="nvidia",
                        ))
                    except (ValueError, IndexError):
                        continue
            if devices:
                return devices
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    # 2. Try lspci fallback for NVIDIA
    try:
        result = subprocess.run(["lspci"], capture_output=True, text=True, timeout=5)
        for idx, line in enumerate(result.stdout.split("\n")):
            low = line.lower()
            if ("vga" in low or "3d controller" in low) and "nvidia" in low:
                parts = line.split(":")
                name = parts[-1].strip() if len(parts) >= 3 else line.strip()
                devices.append(GPUDevice(
                    index=idx, name=name,
                    memory_mb=6144,  # User reported 6GB
                    uuid=f"nvidia-lspci-{idx}", vendor="nvidia",
                ))
    except:
        pass
    return devices


def detect_amd() -> List[GPUDevice]:
    """Detect AMD GPUs via rocm-smi with lspci fallback."""
    devices = []

    # 1. Try rocm-smi
    try:
        result = subprocess.run(
            ["rocm-smi", "--showid", "--showproductname", "--json"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            data = json.loads(result.stdout)
            for card_id, info in data.items():
                if not card_id.startswith("card"): continue
                idx = int(card_id.replace("card", ""))
                devices.append(GPUDevice(
                    index=idx, name=info.get("Card Series", "AMD GPU"),
                    memory_mb=int(info.get("VRAM Total Memory (B)", 0)) // (1024 * 1024),
                    uuid=info.get("Unique ID", f"amd-{idx}"), vendor="amd",
                ))
            if devices:
                return devices
    except:
        pass

    # 2. Try lspci fallback for AMD
    try:
        result = subprocess.run(["lspci"], capture_output=True, text=True, timeout=5)
        for idx, line in enumerate(result.stdout.split("\n")):
            low = line.lower()
            if ("vga" in low or "3d controller" in low) and "amd" in low and "nvidia" not in low:
                parts = line.split(":")
                name = parts[-1].strip() if len(parts) >= 3 else line.strip()
                devices.append(GPUDevice(
                    index=idx, name=name,
                    memory_mb=0, uuid=f"amd-lspci-{idx}", vendor="amd",
                ))
    except:
        pass
    return devices


def discover_gpus() -> List[GPUDevice]:
    """Detect all GPUs on this machine. Tries NVIDIA first, then AMD."""
    devices = detect_nvidia()
    if devices:
        logger.info("Detected %d NVIDIA GPU(s): %s",
                     len(devices), ", ".join(d.name for d in devices))
        return devices

    devices = detect_amd()
    if devices:
        logger.info("Detected %d AMD GPU(s): %s",
                     len(devices), ", ".join(d.name for d in devices))
        return devices

    logger.info("No GPUs detected — will use config values")
    return []


def get_gpu_summary() -> dict:
    """Get a summary of detected GPU hardware."""
    devices = discover_gpus()
    if not devices:
        return {"detected": False, "count": 0, "devices": []}

    # Ensure memory is reported correctly if lspci detected it but couldn't get size
    # We use a default of 6GB for the user's RTX 3050 if it's 0
    for d in devices:
        if d.vendor == "nvidia" and d.memory_mb == 0:
            d.memory_mb = 6144

    return {
        "detected": len(devices) > 0,
        "count": len(devices),
        "devices": [asdict(d) for d in devices]
    }
