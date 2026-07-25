"""Wake-on-LAN magic-packet construction (protocols.md §4). No packet is broadcast."""

from __future__ import annotations

import pytest

from clusterbuck.wol import magic_packet, parse_mac


def test_parse_mac_forms():
    want = bytes([0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF])
    assert parse_mac("aa:bb:cc:dd:ee:ff") == want
    assert parse_mac("AA-BB-CC-DD-EE-FF") == want
    assert parse_mac("aabb.ccdd.eeff") == want
    assert parse_mac("aabbccddeeff") == want


def test_parse_mac_rejects_bad():
    with pytest.raises(ValueError):
        parse_mac("aa:bb:cc")


def test_magic_packet_shape():
    pkt = magic_packet("aa:bb:cc:dd:ee:ff")
    assert len(pkt) == 102          # 6 sync bytes + 16 × 6-byte MAC
    assert pkt[:6] == b"\xff" * 6
    mac = bytes([0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF])
    assert pkt[6:] == mac * 16
