"""Probes the node's hardware for enrollment (protocols.md §6).

Small per-OS shims, no heavyweight hardware-info dependency. Everything is best-effort with
safe fallbacks — a probe that cannot read a value degrades rather than failing enrollment.

Three numbers, answering three different questions, deliberately kept apart:

* `ram_gb` / `vram_gb` — CAN this node hold a model. On a discrete GPU these are different
  budgets and only the second one decides whether inference is fast; on unified memory they
  are the same pool with a cap on how much the GPU may wire down.
* `disk_free_gb` — is there room to PULL one. Measured at the model server's own storage
  path, not the root filesystem: weights land wherever the server keeps them, which on many
  machines is a different volume entirely.
* throughput — how fast it actually runs. NOT probed here. A synthetic benchmark at
  enrollment would time a cold model on an idle machine, once; `work_loop` samples real
  jobs and reports `stats.tps` on the heartbeat instead.
"""

from __future__ import annotations

import os
import platform
import shutil
import socket
import subprocess
import sys

from .models import EnrollRequest, HwProbe

_GIB = 1_073_741_824


def os_name() -> str:
    p = sys.platform
    if p == "darwin":
        return "darwin"
    if p.startswith("linux"):
        return "linux"
    if p in ("win32", "cygwin"):
        return "windows"
    return "unknown"


def arch_name() -> str:
    """Normalised to the vocabulary the coordinator's RID table speaks."""
    m = platform.machine().lower()
    return {"x86_64": "x64", "amd64": "x64", "aarch64": "arm64", "arm64": "arm64"}.get(m, m)


def _run(args: list[str], timeout: float = 2.0) -> str:
    try:
        out = subprocess.run(args, capture_output=True, text=True, timeout=timeout,
                             check=False)
        return out.stdout
    except Exception:
        return ""


def ram_bytes() -> float:
    try:
        if sys.platform == "darwin":
            raw = _run(["sysctl", "-n", "hw.memsize"]).strip()
            if raw.isdigit():
                return float(raw)
        elif sys.platform.startswith("linux"):
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        kb = float("".join(ch for ch in line if ch.isdigit()))
                        return kb * 1024
        elif sys.platform == "win32":
            # GlobalMemoryStatusEx via ctypes — no dependency, no shelling out.
            import ctypes

            class _MemStatus(ctypes.Structure):
                _fields_ = [("dwLength", ctypes.c_ulong),
                            ("dwMemoryLoad", ctypes.c_ulong),
                            ("ullTotalPhys", ctypes.c_ulonglong),
                            ("ullAvailPhys", ctypes.c_ulonglong),
                            ("ullTotalPageFile", ctypes.c_ulonglong),
                            ("ullAvailPageFile", ctypes.c_ulonglong),
                            ("ullTotalVirtual", ctypes.c_ulonglong),
                            ("ullAvailVirtual", ctypes.c_ulonglong),
                            ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]

            stat = _MemStatus()
            stat.dwLength = ctypes.sizeof(_MemStatus)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
            if stat.ullTotalPhys:
                return float(stat.ullTotalPhys)
    except Exception:
        pass
    # Portable fallback. A floor, not physical RAM, but enough to enrol with.
    try:
        return float(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"))
    except (ValueError, OSError, AttributeError):
        return 0.0


def accelerator() -> str:
    if sys.platform == "darwin" and arch_name() == "arm64":
        return "metal"
    if shutil.which("nvidia-smi"):
        if _run(["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"]).strip():
            return "cuda"
    return "cpu"


# Fraction of unified memory macOS lets the GPU wire down, when the limit is left at its
# default. Apple exposes the real figure as Metal's `recommendedMaxWorkingSetSize`; reading
# it needs the Metal framework, which this worker deliberately does not depend on, so this
# approximates it. Larger machines are allowed a bigger share. The numbers are conservative:
# under-reporting VRAM costs a model that would have just fitted, while over-reporting sends
# a pull to a node that will then swap, which is far worse.
_METAL_DEFAULT_FRACTION_LARGE = 0.75   # >= 36 GB
_METAL_DEFAULT_FRACTION_SMALL = 0.67   # < 36 GB
_METAL_LARGE_RAM_GB = 36.0


def _metal_vram_bytes(ram: float) -> float | None:
    """Unified memory the GPU may actually use, honouring an explicit wired limit."""
    if ram <= 0:
        return None
    # An operator who raised the cap (`sysctl iogpu.wired_limit_mb=N`) means it — report
    # what they set. 0 is the sentinel for "default", not a real limit of zero.
    raw = _run(["sysctl", "-n", "iogpu.wired_limit_mb"]).strip()
    if raw.isdigit() and int(raw) > 0:
        return min(float(raw) * 1024 * 1024, ram)
    ram_gb = ram / _GIB
    fraction = (_METAL_DEFAULT_FRACTION_LARGE if ram_gb >= _METAL_LARGE_RAM_GB
                else _METAL_DEFAULT_FRACTION_SMALL)
    return ram * fraction


def _cuda_vram_bytes() -> float | None:
    """Total VRAM of the largest CUDA device, in bytes.

    The largest, not the sum: a model has to fit in ONE device's memory to run at device
    speed, and summing two cards would claim a capacity no single artifact can use.
    """
    out = _run(["nvidia-smi", "--query-gpu=memory.total",
                "--format=csv,noheader,nounits"])
    sizes = []
    for line in out.splitlines():
        raw = line.strip()
        if raw.isdigit():
            sizes.append(float(raw) * 1024 * 1024)  # reported in MiB
    return max(sizes) if sizes else None


def vram_bytes(accel: str | None = None, ram: float | None = None) -> float | None:
    """Memory an artifact must fit into to run at accelerator speed. None if unknown.

    None means "no accelerator, or could not tell" — NOT zero. The distinction matters at
    the fits gate: an unknown VRAM falls back to judging by RAM, while a known-small VRAM
    is positive evidence that a big model will spill and crawl.
    """
    accel = accelerator() if accel is None else accel
    if accel == "metal":
        return _metal_vram_bytes(ram_bytes() if ram is None else ram)
    if accel == "cuda":
        return _cuda_vram_bytes()
    return None


# Where each model server keeps its weights, relative to the user's home directory.
# `disk_free_gb` gates multi-GB pulls against the owner's disk quota (ADR 10), so it has to
# measure the volume the pull will actually land on. These paths are stable, documented
# defaults; an operator who moved the store sets the server's own env var, which is checked
# first.
_MODEL_STORE_CANDIDATES = (
    ("OLLAMA_MODELS", "~/.ollama/models"),
    (None, "~/.lmstudio/models"),
    (None, "~/.cache/lm-studio/models"),
    (None, "~/.cache/huggingface/hub"),
)


def model_store_path() -> str:
    """Best guess at the directory the local model server writes weights into.

    Falls back to the filesystem root, which is what this used to measure unconditionally —
    correct only when the model store happens to live on the same volume.
    """
    for env_var, default in _MODEL_STORE_CANDIDATES:
        raw = os.environ.get(env_var) if env_var else None
        path = os.path.expanduser(raw or default)
        if os.path.isdir(path):
            return path
    return os.path.abspath(os.sep)


def _nearest_existing(path: str) -> str:
    """The closest existing ancestor of `path` — `disk_usage` raises on a missing one."""
    current = os.path.abspath(path)
    while current and not os.path.exists(current):
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    return current or os.path.abspath(os.sep)


def disk_free_bytes(path: str | None = None) -> float:
    """Free bytes on the volume holding the model store (not necessarily the root volume)."""
    try:
        return float(shutil.disk_usage(_nearest_existing(path or model_store_path())).free)
    except Exception:
        return 0.0


def safe_hostname() -> str:
    try:
        return socket.gethostname() or "unknown"
    except Exception:
        return "unknown"


def build(join_token: str, profile: str) -> EnrollRequest:
    ram = ram_bytes()
    accel = accelerator()
    vram = vram_bytes(accel, ram)
    return EnrollRequest(
        join_token=join_token,
        hostname=safe_hostname(),
        os=os_name(),
        arch=arch_name(),
        hw=HwProbe(
            ram_gb=round(ram / _GIB, 1),
            accelerator=accel,
            disk_free_gb=round(disk_free_bytes() / _GIB, 1),
            vram_gb=round(vram / _GIB, 1) if vram else None,
        ),
        profile=profile,
    )
