# P2P File Sharing Engine

A high-performance, decentralized peer-to-peer file sharing system and tracker built from scratch in Python. Inspired by BitTorrent, it coordinates peers via a central tracker and transfers file data directly between peers. 

Complete with a modern, real-time FastAPI & WebSocket-powered Web UI.

## 🚀 Key Features

*   **Rarest-First Download Scheduling:** Prioritizes rare chunks to maximize network availability and download speeds, mimicking BitTorrent's core swarm logic.
*   **Zero-Config LAN Discovery:** Peers automatically find the central tracker on local networks via UDP broadcasts, allowing plug-and-play execution without typing IP addresses.
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

# For Bash/Zsh:
source .venv/bin/activate
# For Fish shell:
# source .venv/bin/activate.fish

pip install fastapi "uvicorn[standard]" python-multipart
```

### 2. Run Locally (Same Machine Simulation)
To simulate a swarm on a single computer, you must give each peer a unique `--port` (for P2P TCP connections) and `--web-port` (for the dashboard).

```bash
# Terminal 1: Start the central tracker (Listens on 0.0.0.0:5000)
python -m tracker.tracker
# Terminal 2: Start Peer 1
python -m web.run --id peer1 --port 6001 --web-port 8080

# Terminal 3: Start Peer 2
python -m web.run --id peer2 --port 6002 --web-port 8081
```

*(Note: The peers will automatically find the tracker using local UDP broadcasts).*

### 3. Usage
1. Open `http://localhost:8080` in your browser. This is Peer 1.
2. Navigate to the **Share** tab and drag-and-drop a file to seed it.
3. Open `http://localhost:8081` in a second browser window. This is Peer 2.
4. Navigate to the **Swarm** tab to see the shared file, and click **Download**.
5. Watch the live progress bar and peer connections in the **Transfers** tab.

## ⚙️ Running Across Multiple Machines (Real Network)

The system is designed for true plug-and-play execution across a Local Area Network (LAN) or WiFi network. Thanks to UDP Auto-Discovery, you don't even need to know IP addresses.

**Machine A (The Server):**
```bash
# Start the tracker
python -m tracker.tracker

# (Optional) Run a peer on the server machine as well
python -m web.run --id server_peer --port 6001 --web-port 8080
```

**Machine B, C, D... (The Clients):**
```bash
# Start a peer on any other computer on the network
python -m web.run --id laptop_peer --port 6001 --web-port 8080
```

* Because they are on different machines, all peers can safely reuse the standard `6001` and `8080` ports.
* The peers will automatically broadcast a UDP packet to find the tracker, connect to it, and join the global swarm.
*(Note: Ensure your OS firewall allows inbound traffic on TCP port 5000 for the tracker, and TCP port 6001 for the peers).*

## 📁 Repository Structure
```text
├── common/         # TCP Protocol framing and Token Bucket limiters
├── peer/           # P2P core: Downloader, Uploader, Chunk Manager
├── tracker/        # Central registry for peer discovery
├── web/            # FastAPI Web Server and Frontend UI Assets
└── test_integration.py # Automated integration tests
```