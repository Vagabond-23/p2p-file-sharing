# P2P File Sharing Engine

A high-performance, decentralized peer-to-peer file sharing system and tracker built from scratch in Python. Inspired by BitTorrent, it coordinates peers via a central tracker and transfers file data directly between peers. 

Complete with a modern, real-time FastAPI & WebSocket-powered Web UI.

## 🚀 Key Features

*   **Rarest-First Download Scheduling:** Prioritizes rare chunks to maximize network availability and download speeds, mimicking BitTorrent's core swarm logic.
*   **Fully Decentralized Transfers:** The tracker only stores metadata. File data flows directly peer-to-peer natively over TCP without centralized bottlenecks.
*   **Real-time Web UI:** A beautiful, responsive dashboard with drag-and-drop file sharing, live download progress reporting via WebSockets, and interactive bandwidth controls.
*   **Dynamic Bandwidth Throttling:** Custom token-bucket algorithm implementation allowing instant upload/download speed limit adjustments with zero downtime.
*   **Resilience & Integrity:** SHA-256 chunk verification prevents data corruption. Stateful downloads allow pausing and resuming of large file transfers across disconnects.
*   **Zero-Dependency Core:** The underlying P2P protocol is built entirely with Python standard libraries (`socket`, `threading`, `json`). 

## 🏗️ Architecture & Protocol

### How It Works
1. **Registration:** A peer starts and registers its details (IP, port, Peer ID) with the central Tracker.
2. **Announce:** When a user shares a file, the file is hashed into 512 KB chunks. A manifest is sent to the Tracker. The actual file bytes never leave the seeder's machine until requested directly by another peer.
3. **Discovery:** A downloader queries the Tracker for a file and receives a list of active peers caching the file's chunks.
4. **Peer Connection:** The downloader connects directly to the seeders via TCP sockets. 
5. **Fetch & Verify:** Chunks are downloaded in parallel (rarest-first), verified against SHA-256 hashes, and assembled. 

### Custom TCP Protocol
All communications across the network utilize **length-prefixed JSON over TCP**:
```text
┌──────────────────────────┬──────────────────────────────────────┐
│  4 bytes (big-endian)    │  N bytes (UTF-8 JSON)                │
│  uint32: body length     │  {"type": "...", "payload": {...}}   │
└──────────────────────────┴──────────────────────────────────────┘
```
This custom framing mechanism cleanly solves TCP's infinite stream boundary problem, guaranteeing a receiver always knows exactly how many bytes to read for each payload.

## 🛠️ Tech Stack

*   **Core Protocol:** Pure Python 3.10+, Sockets, Threading
*   **Web Dashboard:** FastAPI, Uvicorn, WebSockets
*   **Frontend UI:** Vanilla JavaScript, HTML5, CSS3 (Custom Dark Glassmorphism)

## 🏎️ Quick Start

### 1. Install UI Dependencies
While the core P2P backend requires no external dependencies, the Web UI utilizes FastAPI. Install the requirements in a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
pip install fastapi "uvicorn[standard]" python-multipart
```

### 2. Start the Network
You will need at least two terminal windows to simulate a network locally.

```bash
# Terminal 1: Start the central tracker (Listens on 0.0.0.0:5000)
python -m tracker.tracker

# Terminal 2: Start Peer 1 with Web UI
python -m web.run --id peer1 --port 6001 --web-port 8080

# Terminal 3: Start Peer 2 with Web UI
python -m web.run --id peer2 --port 6002 --web-port 8081
```

### 3. Usage
1. Open `http://localhost:8080` in your browser. This is Peer 1.
2. Navigate to the **Share** tab and drag-and-drop a file to seed it.
3. Open `http://localhost:8081` in a second browser window. This is Peer 2.
4. Navigate to the **Swarm** tab to see the shared file, and click **Download**.
5. Watch the live progress bar and peer connections in the **Transfers** tab.

## ⚙️ Running Across Multiple Machines

The system is designed to run across a local area network (LAN) seamlessly. To do this, simply locate the IP address of the machine running the Tracker. 

When starting peers on other computers on the same network, point them to the Tracker IP:

```bash
python -m web.run --id peer_laptop --port 6001 --web-port 8080 --tracker-host 192.168.1.10
```
*(Note: Ensure your firewall allows traffic on TCP ports 5000 and 6001).*

## 📁 Repository Structure
```text
├── common/         # TCP Protocol framing and Token Bucket limiters
├── peer/           # P2P core: Downloader, Uploader, Chunk Manager
├── tracker/        # Central registry for peer discovery
├── web/            # FastAPI Web Server and Frontend UI Assets
└── test_integration.py # Automated integration tests
```