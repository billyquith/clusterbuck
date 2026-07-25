"""Wake-on-LAN (protocols.md §4): waking a sleeping node is a pure network action.

A magic packet is 6 bytes of 0xFF followed by the target MAC repeated 16 times, sent as a
UDP broadcast. No software is needed on the target — it only needs "wake for network
access" enabled and to be on the LAN. Off-LAN machines can't be woken.
"""

from __future__ import annotations

import socket


def parse_mac(mac: str) -> bytes:
    """Parse 'aa:bb:cc:dd:ee:ff' (or '-'/'.' separated, or bare hex) into 6 bytes."""
    cleaned = mac.replace(":", "").replace("-", "").replace(".", "")
    if len(cleaned) != 12:
        raise ValueError(f"invalid MAC address: {mac!r}")
    return bytes.fromhex(cleaned)


def magic_packet(mac: str) -> bytes:
    """Build the 102-byte Wake-on-LAN magic packet for a MAC address."""
    return b"\xff" * 6 + parse_mac(mac) * 16


def send_magic_packet(
    mac: str, *, broadcast: str = "255.255.255.255", port: int = 9
) -> None:
    """Broadcast a magic packet. Fire-and-forget UDP; delivery is best-effort by nature."""
    packet = magic_packet(mac)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.sendto(packet, (broadcast, port))
