#!/usr/bin/env python3
"""
Peer node — run one instance per peer.

Local testing (same machine):
  python -m peer.peer --id peer1 --port 6001
  python -m peer.peer --id peer2 --port 6002

Across machines:
  python -m peer.peer --id peer1 --port 6001 --tracker-host 192.168.1.10

With bandwidth limits:
  python -m peer.peer --id peer1 --port 6001 --upload-limit 50MB --download-limit 100MB

Interactive commands:
  share <filepath>              Share a file with the swarm
  download <filename>           Download a file (resumes if interrupted)
  list                          List all files in the swarm
  status                        Show this peer's shared files
  throttle up <limit|0>         Change upload limit at runtime  (e.g. 20MB, 0=off)
  throttle down <limit|0>       Change download limit at runtime
  quit                          Disconnect and exit
"""

import argparse
import logging
import os
import socket as _socket
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from peer.uploader import UploaderServer
from peer.tracker_client import TrackerClient
from peer.chunk_manager import build_manifest, FileManifest
from peer.downloader import download_file, PeerAddr
from common.throttle import TokenBucket

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("peer")


def get_lan_ip() -> str:
    try:
        s = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except OSError:
        return "127.0.0.1"


def _fmt_eta(seconds: float) -> str:
    if seconds == float("inf") or seconds > 86400:
        return "--:--"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h{m:02d}m"
    return f"{m:02d}:{s:02d}"


def _fmt_rate(mbps: float) -> str:
    if mbps >= 1000:
        return f"{mbps/1000:.1f} GB/s"
    if mbps >= 1:
        return f"{mbps:.1f} MB/s"
    return f"{mbps*1024:.0f} KB/s"


class Peer:
    def __init__(self, peer_id: str, host: str, port: int,
                 tracker_host: str, tracker_port: int,
                 shared_dir: str, download_dir: str,
                 upload_limit: float = 0, download_limit: float = 0):
        self.peer_id = peer_id
        self.host = host
        self.port = port
        self.shared_dir = shared_dir
        self.download_dir = download_dir

        self.shared_files: dict[str, tuple[str, FileManifest]] = {}

        # Throttle limiters — shared objects so we can update at runtime
        self.upload_limiter   = TokenBucket(upload_limit)
        self.download_limiter = TokenBucket(download_limit)

        self.tracker = TrackerClient(
            peer_id=peer_id,
            peer_host=host,
            peer_port=port,
            tracker_host=tracker_host,
            tracker_port=tracker_port,
        )
        self.uploader = UploaderServer(
            host="0.0.0.0",
            port=port,
            shared_files=self.shared_files,
            upload_limiter=self.upload_limiter,
        )

    def start(self) -> None:
        self.uploader.start()
        logger.info(f"Peer '{self.peer_id}' uploader bound on 0.0.0.0:{self.port} "
                    f"(advertising {self.host}:{self.port})")
        self.tracker.connect()
        self.tracker.register()

        for fname in os.listdir(self.shared_dir):
            fpath = os.path.join(self.shared_dir, fname)
            if os.path.isfile(fpath):
                self._share_file(fpath, silent=True)

    def stop(self) -> None:
        self.tracker.disconnect()
        self.uploader.stop()
        logger.info(f"Peer '{self.peer_id}' stopped.")

    # ── Commands ───────────────────────────────────────────────────────────────

    def _share_file(self, filepath: str, silent=False) -> None:
        if not os.path.isfile(filepath):
            print(f"  [!] File not found: {filepath}")
            return
        print(f"  [~] Hashing '{os.path.basename(filepath)}'...")
        manifest = build_manifest(filepath)
        self.shared_files[manifest.filename] = (filepath, manifest)
        self.tracker.announce_file(manifest)
        if not silent:
            print(f"  [+] Sharing '{manifest.filename}' — "
                  f"{manifest.total_chunks} chunks, {manifest.total_size:,} bytes")

    def _download_file(self, filename: str) -> None:
        print(f"  [~] Querying tracker for '{filename}'...")
        result = self.tracker.get_peers(filename)

        if not result.get("peers"):
            print(f"  [!] No peers have '{filename}'")
            return

        manifest = FileManifest.from_dict(result["manifest"])
        peers = [
            PeerAddr(
                host=p["host"],
                port=p["port"],
                peer_id=p["peer_id"],
                chunks=p.get("chunks", list(range(manifest.total_chunks))),
            )
            for p in result["peers"]
            if p["peer_id"] != self.peer_id
        ]

        if not peers:
            print(f"  [!] You are the only seeder of '{filename}'")
            return

        output_path = os.path.join(self.download_dir, filename)

        # Show resume hint if partial download exists
        import json
        meta = output_path + ".part.json"
        if os.path.exists(meta):
            try:
                with open(meta) as f:
                    state = json.load(f)
                already = len(state.get("done", []))
                print(f"  [~] Resuming from chunk {already}/{manifest.total_chunks} "
                      f"({already/manifest.total_chunks*100:.1f}% already done)")
            except Exception:
                pass
        else:
            print(f"  [~] {len(peers)} peer(s) | {manifest.total_chunks} chunks | "
                  f"{manifest.total_size/1024/1024:.1f} MB | rarest-first scheduling")

        start = time.time()
        last_print = [0.0]

        def on_progress(snap: dict) -> None:
            now = time.monotonic()
            # Throttle terminal updates to ~10/sec
            if now - last_print[0] < 0.1:
                return
            last_print[0] = now

            done  = snap["done"]
            total = snap["total"]
            pct   = snap["pct"]
            speed = _fmt_rate(snap["speed_mbps"])
            eta   = _fmt_eta(snap["eta_sec"])
            filled = int(pct / 5)
            bar = "█" * filled + "░" * (20 - filled)
            print(f"\r  [{bar}] {pct:5.1f}%  {done}/{total}  {speed}  ETA {eta}   ",
                  end="", flush=True)

        limiter = self.download_limiter if self.download_limiter.rate > 0 else None

        try:
            stats = download_file(manifest, peers, output_path,
                                  on_progress=on_progress,
                                  download_limiter=limiter)
            elapsed = time.time() - start
            size_mb = manifest.total_size / 1024 / 1024
            avg_speed = _fmt_rate(size_mb / elapsed)

            print(f"\n  [+] Done in {elapsed:.1f}s — {size_mb:.1f} MB @ {avg_speed}")

            # Per-peer contribution report
            peer_stats = stats.get("peer_stats", {})
            if peer_stats:
                print("  [i] Chunk sources:")
                for pid, s in peer_stats.items():
                    mb = s["bytes"] / 1024 / 1024
                    print(f"        {pid:<20} {s['chunks']:>5} chunks  {mb:.1f} MB")

            # Start seeding immediately
            self.shared_files[filename] = (output_path, manifest)
            self.tracker.announce_file(manifest)
            print(f"  [+] Now seeding '{filename}'")

        except Exception as e:
            print(f"\n  [!] Download failed: {e}")
            print(f"  [i] Partial progress saved — re-run 'download {filename}' to resume")
            logger.debug(e, exc_info=True)

    def _list_files(self) -> None:
        result = self.tracker.get_peers("__list__")
        files = result.get("files", [])
        if not files:
            print("  (no files in swarm)")
            return
        print(f"\n  {'Filename':<45} {'Size':>12}  {'Peers':>6}  {'Chunks':>8}")
        print(f"  {'-'*45} {'-'*12}  {'-'*6}  {'-'*8}")
        for f in files:
            size_str = f"{f['total_size']:,}" if f.get("total_size") else "?"
            name = f["filename"]
            if len(name) > 44:
                name = name[:41] + "..."
            print(f"  {name:<45} {size_str:>12}  {f['peer_count']:>6}  {f['total_chunks']:>8}")
        print()

    def _status(self) -> None:
        if not self.shared_files:
            print("  (not sharing any files)")
            return
        up_label   = f"{self.upload_limiter.rate/1024/1024:.1f} MB/s" \
                     if self.upload_limiter.rate > 0 else "unlimited"
        down_label = f"{self.download_limiter.rate/1024/1024:.1f} MB/s" \
                     if self.download_limiter.rate > 0 else "unlimited"
        print(f"  Upload limit: {up_label}   Download limit: {down_label}")
        print()
        for fname, (path, manifest) in self.shared_files.items():
            name = fname if len(fname) <= 44 else fname[:41] + "..."
            print(f"  {name:<45} {manifest.total_size:>12,} bytes  "
                  f"{manifest.total_chunks} chunks")

    def _set_throttle(self, direction: str, limit_str: str) -> None:
        try:
            rate = TokenBucket.parse_limit(limit_str)
        except ValueError:
            print(f"  [!] Invalid limit '{limit_str}'. Examples: 50MB, 100KB, 0 (unlimited)")
            return

        label = f"{rate/1024/1024:.1f} MB/s" if rate > 0 else "unlimited"
        if direction == "up":
            self.upload_limiter.set_rate(rate)
            self.uploader.upload_limiter = self.upload_limiter
            print(f"  [+] Upload limit → {label}")
        elif direction == "down":
            self.download_limiter.set_rate(rate)
            print(f"  [+] Download limit → {label}")

    # ── REPL ───────────────────────────────────────────────────────────────────

    def repl(self) -> None:
        print(f"\nPeer '{self.peer_id}' ready.")
        print("Commands: share, download, list, status, throttle up/down <limit>, quit\n")
        while True:
            try:
                line = input(f"[{self.peer_id}]> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break

            if not line:
                continue

            # Split only on first space — preserves filenames with spaces
            tokens = line.split(maxsplit=1)
            cmd = tokens[0].lower()
            arg = tokens[1].strip() if len(tokens) > 1 else ""

            if cmd == "quit":
                break
            elif cmd == "share":
                if not arg:
                    print("  Usage: share <filepath>")
                else:
                    self._share_file(arg)
            elif cmd == "download":
                if not arg:
                    print("  Usage: download <filename>")
                else:
                    self._download_file(arg)
            elif cmd == "list":
                self._list_files()
            elif cmd == "status":
                self._status()
            elif cmd == "throttle":
                tparts = arg.split()
                if len(tparts) < 2 or tparts[0] not in ("up", "down"):
                    print("  Usage: throttle up <limit>  |  throttle down <limit>")
                    print("  Examples: throttle up 50MB  |  throttle down 0  (0=unlimited)")
                else:
                    self._set_throttle(tparts[0], tparts[1])
            else:
                print(f"  Unknown command: '{cmd}'")


def main():
    parser = argparse.ArgumentParser(description="P2P File Sharing — Peer Node")
    parser.add_argument("--id",       required=True)
    parser.add_argument("--host",     default=None,
                        help="LAN IP to advertise. Auto-detected if omitted.")
    parser.add_argument("--port",     type=int, required=True)
    parser.add_argument("--tracker-host", default="127.0.0.1")
    parser.add_argument("--tracker-port", type=int, default=5000)
    parser.add_argument("--shared-dir",   default="shared_files")
    parser.add_argument("--download-dir", default="downloads")
    parser.add_argument("--upload-limit",   default="0",
                        help="Max upload speed, e.g. 50MB, 500KB, 0=unlimited")
    parser.add_argument("--download-limit", default="0",
                        help="Max download speed, e.g. 100MB, 0=unlimited")
    args = parser.parse_args()

    host = args.host or get_lan_ip()
    logger.info(f"Using host IP: {host}")

    try:
        upload_rate   = TokenBucket.parse_limit(args.upload_limit)
        download_rate = TokenBucket.parse_limit(args.download_limit)
    except ValueError as e:
        print(f"[!] Bad bandwidth limit: {e}")
        sys.exit(1)

    if upload_rate > 0:
        logger.info(f"Upload limit: {upload_rate/1024/1024:.1f} MB/s")
    if download_rate > 0:
        logger.info(f"Download limit: {download_rate/1024/1024:.1f} MB/s")

    os.makedirs(args.shared_dir, exist_ok=True)
    os.makedirs(args.download_dir, exist_ok=True)

    peer = Peer(
        peer_id=args.id,
        host=host,
        port=args.port,
        tracker_host=args.tracker_host,
        tracker_port=args.tracker_port,
        shared_dir=args.shared_dir,
        download_dir=args.download_dir,
        upload_limit=upload_rate,
        download_limit=download_rate,
    )

    try:
        peer.start()
        peer.repl()
    finally:
        peer.stop()


if __name__ == "__main__":
    main()