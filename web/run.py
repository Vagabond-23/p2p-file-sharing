#!/usr/bin/env python3
"""
Web UI entry point — starts peer + FastAPI server.

Usage:
  python -m web.run --id peer1 --port 6001
  python -m web.run --id peer1 --port 6001 --web-port 8080
  python -m web.run --id peer1 --port 6001 --web-port 8080 --tracker-host 192.168.1.10

With bandwidth limits:
  python -m web.run --id peer1 --port 6001 --upload-limit 50MB --download-limit 100MB
"""

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from peer.peer import Peer, get_lan_ip
from common.throttle import TokenBucket
from web.api import app, set_peer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("web")


def main():
    parser = argparse.ArgumentParser(description="P2P File Sharing — Web UI")
    parser.add_argument("--id",           required=True, help="Unique peer ID")
    parser.add_argument("--host",         default=None,
                        help="LAN IP to advertise. Auto-detected if omitted.")
    parser.add_argument("--port",         type=int, required=True,
                        help="TCP port for P2P chunk transfers")
    parser.add_argument("--web-port",     type=int, default=8080,
                        help="HTTP port for the web UI (default: 8080)")
    parser.add_argument("--tracker-host", default="auto",
                        help="Tracker IP or 'auto' for local network discovery")
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

    # ── Create and start peer ─────────────────────────────────────────────────

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

    peer.start()
    logger.info(f"Peer '{args.id}' started — P2P on port {args.port}")

    # ── Inject peer into API and start web server ─────────────────────────────

    set_peer(peer)

    import uvicorn

    print()
    print(f"  ╔═══════════════════════════════════════════════╗")
    print(f"  ║  P2P File Sharing — Web UI                    ║")
    print(f"  ║                                               ║")
    print(f"  ║  Local:   http://localhost:{args.web_port:<18}║")
    print(f"  ║  Network: http://{host}:{args.web_port:<20}   ║")
    print(f"  ║                                               ║")
    print(f"  ║  Peer ID: {args.id:<34}                       ║")
    print(f"  ║  P2P Port: {args.port:<33}                    ║")
    print(f"  ╚═══════════════════════════════════════════════╝")
    print()

    try:
        uvicorn.run(app, host="0.0.0.0", port=args.web_port, log_level="warning")
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        peer.stop()


if __name__ == "__main__":
    main()
