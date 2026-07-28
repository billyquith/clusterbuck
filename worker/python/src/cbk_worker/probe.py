"""Probes the node's hardware for enrollment (protocols.md §6).

Small per-OS shims, no heavyweight hardware-info dependency. Everything is best-effort with
safe fallbacks — a probe that cannot read a value degrades rather than failing enrollment.
The optional throughput micro-benchmark (a timed call to the local model server) is left
unset here, as in the .NET worker.
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


def disk_free_bytes() -> float:
    try:
        return float(shutil.disk_usage(os.path.abspath(os.sep)).free)
    except Exception:
        return 0.0


def safe_hostname() -> str:
    try:
        return socket.gethostname() or "unknown"
    except Exception:
        return "unknown"


def build(join_token: str, profile: str) -> EnrollRequest:
    return EnrollRequest(
        join_token=join_token,
        hostname=safe_hostname(),
        os=os_name(),
        arch=arch_name(),
        hw=HwProbe(
            ram_gb=round(ram_bytes() / _GIB, 1),
            accelerator=accelerator(),
            disk_free_gb=round(disk_free_bytes() / _GIB, 1),
        ),
        profile=profile,
    )
