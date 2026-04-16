#!/usr/bin/env python3
"""
Integration test: spins up a tracker and 3 peers in-process,
has peer1 share a file, peer2 and peer3 download it from peer1,
then peer3 downloads from both peer1 and peer2.
"""

import hashlib
import logging
import os
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tracker.tracker import TrackerServer
from peer.uploader import UploaderServer
from peer.tracker_client import TrackerClient
from peer.chunk_manager import build_manifest, FileManifest
from peer.downloader import download_file, PeerAddr

logging.basicConfig(level=logging.WARNING)  # quiet during tests

TRACKER_HOST = "127.0.0.1"
TRACKER_PORT = 15000
PEER_PORTS   = [16001, 16002, 16003]


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def make_test_file(path: str, size_mb: float) -> str:
    """Create a deterministic binary file of the given size."""
    size = int(size_mb * 1024 * 1024)
    with open(path, "wb") as f:
        written = 0
        block = bytes(range(256)) * 256  # 64 KB block
        while written < size:
            to_write = min(len(block), size - written)
            f.write(block[:to_write])
            written += to_write
    return sha256_file(path)


def run_test():
    tmpdir = tempfile.mkdtemp(prefix="p2p_test_")
    print(f"\nTest directory: {tmpdir}\n")

    # ── Start tracker ──────────────────────────────────────────────────────────
    tracker = TrackerServer(host=TRACKER_HOST, port=TRACKER_PORT)
    t_thread = threading.Thread(target=tracker.serve_forever, daemon=True)
    t_thread.start()
    time.sleep(0.2)
    print("✓ Tracker started")

    # ── Shared state for uploaders ─────────────────────────────────────────────
    shared = [{}, {}, {}]  # one dict per peer

    # ── Start uploader servers ─────────────────────────────────────────────────
    uploaders = []
    for i, port in enumerate(PEER_PORTS):
        u = UploaderServer(host=TRACKER_HOST, port=port, shared_files=shared[i])
        u.start()
        uploaders.append(u)
    time.sleep(0.2)
    print("✓ Uploader servers started")

    # ── Register peers with tracker ────────────────────────────────────────────
    clients = []
    for i, port in enumerate(PEER_PORTS):
        c = TrackerClient(
            peer_id=f"peer{i+1}",
            peer_host=TRACKER_HOST,
            peer_port=port,
            tracker_host=TRACKER_HOST,
            tracker_port=TRACKER_PORT,
        )
        c.connect()
        c.register()
        clients.append(c)
    print("✓ All peers registered")

    # ── Create a test file (5 MB) ──────────────────────────────────────────────
    src_path = os.path.join(tmpdir, "bigfile.bin")
    original_hash = make_test_file(src_path, size_mb=5)
    print(f"✓ Test file created (5 MB, sha256={original_hash[:16]}...)")

    # ── Peer1 shares the file ──────────────────────────────────────────────────
    manifest = build_manifest(src_path)
    shared[0]["bigfile.bin"] = (src_path, manifest)
    clients[0].announce_file(manifest)
    print(f"✓ Peer1 announced '{manifest.filename}' ({manifest.total_chunks} chunks)")

    # ── Peer2 downloads from peer1 ─────────────────────────────────────────────
    result = clients[1].get_peers("bigfile.bin")
    assert result["peers"], "No peers returned for peer2"
    peers_for_p2 = [PeerAddr(p["host"], p["port"]) for p in result["peers"]]
    p2_out = os.path.join(tmpdir, "peer2_bigfile.bin")
    download_file(FileManifest.from_dict(result["manifest"]), peers_for_p2, p2_out)
    assert sha256_file(p2_out) == original_hash, "Peer2 download hash mismatch!"
    shared[1]["bigfile.bin"] = (p2_out, manifest)
    clients[1].announce_file(manifest)
    print("✓ Peer2 downloaded and verified — now seeding")

    # ── Peer3 downloads from both peer1 and peer2 ──────────────────────────────
    result = clients[2].get_peers("bigfile.bin")
    assert len(result["peers"]) >= 2, "Expected at least 2 peers for peer3"
    peers_for_p3 = [PeerAddr(p["host"], p["port"]) for p in result["peers"]]
    p3_out = os.path.join(tmpdir, "peer3_bigfile.bin")
    start = time.time()
    download_file(FileManifest.from_dict(result["manifest"]), peers_for_p3, p3_out)
    elapsed = time.time() - start
    assert sha256_file(p3_out) == original_hash, "Peer3 download hash mismatch!"
    print(f"✓ Peer3 downloaded from {len(peers_for_p3)} peers in {elapsed:.2f}s — verified")

    # ── List files ─────────────────────────────────────────────────────────────
    listing = clients[0].get_peers("__list__")
    assert any(f["filename"] == "bigfile.bin" for f in listing["files"])
    print("✓ Tracker listing works")

    # ── Cleanup ────────────────────────────────────────────────────────────────
    for c in clients:
        c.disconnect()
    for u in uploaders:
        u.stop()

    print("\n" + "="*50)
    print("  ALL TESTS PASSED")
    print("="*50 + "\n")


if __name__ == "__main__":
    run_test()