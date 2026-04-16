TRACKER_HOST = "127.0.0.1"
TRACKER_PORT = 5000

PEER_SERVER_PORT_START = 6000   # peers bind to 6000, 6001, 6002, ...

CHUNK_SIZE       = 512 * 1024   # 512 KB per chunk
CONNECT_TIMEOUT  = 5            # seconds
RECV_TIMEOUT     = 30           # seconds (peer-to-peer chunk transfers)
MAX_WORKERS      = 8            # max parallel chunk downloads