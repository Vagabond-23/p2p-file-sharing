/* ═══════════════════════════════════════════════════════════════════════════
   P2P File Sharing — Web UI Frontend
   Vanilla JS single-page app. No build step.
   ═══════════════════════════════════════════════════════════════════════════ */

// ── Helpers ──────────────────────────────────────────────────────────────────

function formatSize(bytes) {
    if (bytes == null || bytes === 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return `${(bytes / Math.pow(1024, i)).toFixed(i > 0 ? 1 : 0)} ${units[i]}`;
}

function formatSpeed(mbps) {
    if (mbps == null || mbps <= 0) return '0 B/s';
    if (mbps >= 1000) return `${(mbps / 1000).toFixed(1)} GB/s`;
    if (mbps >= 1) return `${mbps.toFixed(1)} MB/s`;
    return `${(mbps * 1024).toFixed(0)} KB/s`;
}

function formatEta(seconds) {
    if (seconds == null || !isFinite(seconds) || seconds > 86400) return '--:--';
    const m = Math.floor(seconds / 60);
    const s = Math.floor(seconds % 60);
    const h = Math.floor(m / 60);
    if (h) return `${h}h${String(m % 60).padStart(2, '0')}m`;
    return `${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
}

function fileIcon(filename) {
    const ext = (filename.split('.').pop() || '').toLowerCase();
    const map = {
        mp4: '🎬', mkv: '🎬', avi: '🎬', mov: '🎬', webm: '🎬',
        mp3: '🎵', flac: '🎵', wav: '🎵', ogg: '🎵', aac: '🎵',
        jpg: '🖼️', jpeg: '🖼️', png: '🖼️', gif: '🖼️', webp: '🖼️', svg: '🖼️',
        pdf: '📄', doc: '📄', docx: '📄', txt: '📄',
        zip: '📦', tar: '📦', gz: '📦', rar: '📦', '7z': '📦',
        py: '🐍', js: '📜', html: '📜', css: '📜',
        csv: '📊', xlsx: '📊', xls: '📊',
        iso: '💿', dmg: '💿', img: '💿',
    };
    return map[ext] || '📄';
}


// ── Toast System ─────────────────────────────────────────────────────────────

function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    const icons = { success: '✓', error: '✕', info: 'ℹ', warning: '⚠' };

    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.innerHTML = `
        <span class="toast-icon">${icons[type] || icons.info}</span>
        <span>${message}</span>
    `;
    container.appendChild(toast);

    setTimeout(() => {
        toast.classList.add('toast-exit');
        setTimeout(() => toast.remove(), 350);
    }, 4000);
}


// ── API Client ───────────────────────────────────────────────────────────────

const api = {
    async getStatus() {
        const r = await fetch('/api/status');
        if (!r.ok) throw new Error(`Status ${r.status}`);
        return r.json();
    },
    async getFiles() {
        const r = await fetch('/api/files');
        if (!r.ok) throw new Error(`Status ${r.status}`);
        return r.json();
    },
    async getDownloads() {
        const r = await fetch('/api/downloads');
        if (!r.ok) throw new Error(`Status ${r.status}`);
        return r.json();
    },
    async startDownload(filename) {
        const r = await fetch(`/api/download/${encodeURIComponent(filename)}`, { method: 'POST' });
        if (!r.ok) {
            const data = await r.json().catch(() => ({}));
            throw new Error(data.detail || `Download failed (${r.status})`);
        }
        return r.json();
    },
    async shareFile(file) {
        const fd = new FormData();
        fd.append('file', file);
        const r = await fetch('/api/share', { method: 'POST', body: fd });
        if (!r.ok) {
            const data = await r.json().catch(() => ({}));
            throw new Error(data.detail || `Share failed (${r.status})`);
        }
        return r.json();
    },
    async sharePath(path) {
        const r = await fetch('/api/share-path', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ path }),
        });
        if (!r.ok) {
            const data = await r.json().catch(() => ({}));
            throw new Error(data.detail || `Share failed (${r.status})`);
        }
        return r.json();
    },
    async setThrottle(direction, limit) {
        const r = await fetch('/api/throttle', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ direction, limit }),
        });
        if (!r.ok) {
            const data = await r.json().catch(() => ({}));
            throw new Error(data.detail || `Throttle failed (${r.status})`);
        }
        return r.json();
    },
    async dismissDownload(filename) {
        const r = await fetch(`/api/downloads/${encodeURIComponent(filename)}`, { method: 'DELETE' });
        return r.json();
    },
};


// ── WebSocket Manager ────────────────────────────────────────────────────────

class WS {
    constructor(onEvent) {
        this.onEvent = onEvent;
        this.ws = null;
        this.retries = 0;
        this.maxRetries = 20;
    }

    connect() {
        const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
        const url = `${proto}//${location.host}/ws`;
        this.ws = new WebSocket(url);

        this.ws.onopen = () => {
            this.retries = 0;
            this.onEvent({ type: '_ws_connected' });
        };

        this.ws.onmessage = (e) => {
            try {
                const data = JSON.parse(e.data);
                this.onEvent(data);
            } catch (err) {
                console.warn('WS parse error:', err);
            }
        };

        this.ws.onclose = () => {
            this.onEvent({ type: '_ws_disconnected' });
            this._retry();
        };

        this.ws.onerror = () => {
            // onclose will fire after this
        };
    }

    _retry() {
        if (this.retries >= this.maxRetries) return;
        const delay = Math.min(1000 * Math.pow(1.5, this.retries), 15000);
        this.retries++;
        setTimeout(() => this.connect(), delay);
    }
}


// ── App State ────────────────────────────────────────────────────────────────

const state = {
    status: null,
    files: [],
    downloads: {},        // filename -> download info
    completedDownloads: [],
    wsConnected: false,
};


// ── Rendering ────────────────────────────────────────────────────────────────

function renderStatus(status) {
    if (!status) return;
    state.status = status;

    // Header
    document.getElementById('peer-id-display').textContent = status.peer_id;
    document.getElementById('tracker-badge').textContent =
        `tracker ${status.tracker_host}:${status.tracker_port}`;

    // Stats
    document.getElementById('stat-shared').textContent = status.shared_files.length;
    document.getElementById('stat-upload').textContent = status.upload_limit_label;
    document.getElementById('stat-download').textContent = status.download_limit_label;

    // Settings panel
    document.getElementById('throttle-up-current').textContent = status.upload_limit_label;
    document.getElementById('throttle-down-current').textContent = status.download_limit_label;

    // Peer info
    document.getElementById('info-peer-id').textContent = status.peer_id;
    document.getElementById('info-host').textContent = status.host;
    document.getElementById('info-port').textContent = status.port;
    document.getElementById('info-tracker').textContent =
        `${status.tracker_host}:${status.tracker_port}`;

    // Sync active downloads from status
    if (status.active_downloads) {
        for (const dl of status.active_downloads) {
            state.downloads[dl.filename] = dl;
        }
    }
}


function renderSwarm(files) {
    state.files = files || [];
    document.getElementById('stat-swarm').textContent = files.length;

    const wrap  = document.getElementById('swarm-table-wrap');
    const empty = document.getElementById('swarm-empty');
    const tbody = document.getElementById('swarm-tbody');

    if (files.length === 0) {
        wrap.style.display = 'none';
        empty.style.display = '';
        return;
    }

    wrap.style.display = '';
    empty.style.display = 'none';

    tbody.innerHTML = files.map(f => {
        const icon = fileIcon(f.filename);
        const size = formatSize(f.total_size);
        const peers = f.peer_count || 0;
        const chunks = f.total_chunks || 0;

        let badges = '';
        if (f.is_mine) badges += '<span class="file-badge seeding">Seeding</span>';
        if (f.is_downloading) badges += '<span class="file-badge downloading">Downloading</span>';

        // Health indicator
        let healthClass = 'good';
        if (peers <= 1) healthClass = 'low';
        else if (peers <= 2) healthClass = 'medium';

        const isDownloading = f.is_downloading || (f.filename in state.downloads);
        const isMine = f.is_mine;
        let actionBtn = '';
        if (isDownloading) {
            actionBtn = `<button class="btn btn-ghost btn-sm" disabled>Downloading…</button>`;
        } else if (isMine) {
            actionBtn = `<button class="btn btn-ghost btn-sm" disabled>Seeding</button>`;
        } else {
            actionBtn = `<button class="btn btn-primary btn-sm" onclick="handleDownload('${f.filename.replace(/'/g, "\\'")}')">Download</button>`;
        }

        return `<tr>
            <td><div class="file-name"><span class="file-icon">${icon}</span>${f.filename}${badges}</div></td>
            <td><span class="file-size">${size}</span></td>
            <td><span class="file-peers"><span class="file-health"><span class="health-dot ${healthClass}"></span>${peers}</span></span></td>
            <td><span class="file-size">${chunks}</span></td>
            <td>${actionBtn}</td>
        </tr>`;
    }).join('');
}


function renderTransfers() {
    const list  = document.getElementById('transfers-list');
    const empty = document.getElementById('transfers-empty');

    const activeKeys = Object.keys(state.downloads);
    const completed  = state.completedDownloads;
    const total = activeKeys.length + completed.length;

    // Update badge
    const badge = document.getElementById('transfers-badge');
    if (badge) {
        badge.textContent = activeKeys.length;
        badge.style.display = activeKeys.length > 0 ? '' : 'none';
    }

    if (total === 0) {
        list.innerHTML = '';
        empty.style.display = '';
        return;
    }
    empty.style.display = 'none';

    let html = '';

    // Active downloads
    for (const key of activeKeys) {
        const dl = state.downloads[key];
        html += renderTransferCard(dl);
    }

    // Completed downloads (most recent first)
    for (let i = completed.length - 1; i >= 0; i--) {
        html += renderTransferCard(completed[i]);
    }

    list.innerHTML = html;
}


/**
 * Targeted DOM update for download progress — avoids full innerHTML replacement
 * so the shimmer CSS animation runs smoothly without restarting.
 */
function updateTransferProgress(filename, dl) {
    const cardId = `transfer-${CSS.escape(filename)}`;
    const card = document.getElementById(cardId);
    if (!card) {
        // Card doesn't exist yet — do a full render
        renderTransfers();
        return;
    }

    // Update progress bar width (no innerHTML, so animation stays)
    const fill = card.querySelector('.progress-fill');
    if (fill) {
        fill.style.width = (dl.pct || 0).toFixed(1) + '%';
    }

    // Update stats text
    const stats = card.querySelectorAll('.transfer-stat .value');
    if (stats.length >= 4) {
        stats[0].textContent = formatSpeed(dl.speed_mbps);
        stats[1].textContent = formatEta(dl.eta_sec);
        stats[2].textContent = `${dl.done_chunks || 0}/${dl.total_chunks || '?'}`;
        stats[3].textContent = formatSize(dl.total_size);
    }
}


function renderTransferCard(dl) {
    const icon = fileIcon(dl.filename);
    const pct = dl.pct != null ? dl.pct.toFixed(1) : '0.0';
    const status = dl.status || 'queued';
    const statusLabel = status.charAt(0).toUpperCase() + status.slice(1);

    let progressClass = '';
    if (status === 'completed') progressClass = 'complete';

    let statsHtml = '';
    if (status === 'downloading') {
        statsHtml = `
            <div class="transfer-stats">
                <div class="transfer-stat"><span class="label">Speed</span><span class="value">${formatSpeed(dl.speed_mbps)}</span></div>
                <div class="transfer-stat"><span class="label">ETA</span><span class="value">${formatEta(dl.eta_sec)}</span></div>
                <div class="transfer-stat"><span class="label">Chunks</span><span class="value">${dl.done_chunks || 0}/${dl.total_chunks || '?'}</span></div>
                <div class="transfer-stat"><span class="label">Size</span><span class="value">${formatSize(dl.total_size)}</span></div>
            </div>`;
    } else if (status === 'completed') {
        statsHtml = `
            <div class="transfer-stats">
                <div class="transfer-stat"><span class="label">Size</span><span class="value">${formatSize(dl.total_size)}</span></div>
                <div class="transfer-stat"><span class="label">Avg Speed</span><span class="value">${formatSpeed(dl.speed_mbps)}</span></div>
                <div class="transfer-stat"><span class="label">Time</span><span class="value">${dl.elapsed ? dl.elapsed.toFixed(1) + 's' : '—'}</span></div>
            </div>`;
    } else if (status === 'failed') {
        statsHtml = `
            <div class="transfer-stats">
                <div class="transfer-stat" style="color: var(--error);">${dl.error || 'Unknown error'}</div>
            </div>`;
    }

    // Peer stats chips
    let peersHtml = '';
    if (dl.peer_stats && Object.keys(dl.peer_stats).length > 0) {
        const chips = Object.entries(dl.peer_stats)
            .map(([addr, s]) => `<span class="peer-chip">${addr} · ${s.chunks} chunks · ${formatSize(s.bytes)}</span>`)
            .join('');
        peersHtml = `<div class="transfer-peers">${chips}</div>`;
    }

    let dismissBtn = '';
    if (status === 'failed' || status === 'completed') {
        dismissBtn = `<button class="btn btn-ghost btn-sm" onclick="handleDismiss('${dl.filename.replace(/'/g, "\\'")}', '${status}')">Dismiss</button>`;
    }

    return `
        <div class="transfer-card" id="transfer-${CSS.escape(dl.filename)}">
            <div class="transfer-header">
                <div class="transfer-filename">${icon} ${dl.filename}</div>
                <div style="display:flex;align-items:center;gap:0.5rem;">
                    <span class="transfer-status ${status}">${statusLabel}</span>
                    ${dismissBtn}
                </div>
            </div>
            <div class="transfer-progress">
                <div class="progress-bar">
                    <div class="progress-fill ${progressClass}" style="width: ${pct}%"></div>
                </div>
            </div>
            ${statsHtml}
            ${peersHtml}
        </div>`;
}


// ── Event Handlers ───────────────────────────────────────────────────────────

async function handleDownload(filename) {
    try {
        await api.startDownload(filename);
        state.downloads[filename] = {
            filename, status: 'queued', pct: 0, total_chunks: 0,
            done_chunks: 0, speed_mbps: 0, eta_sec: 0, total_size: 0,
            error: null, peer_stats: {},
        };
        renderTransfers();
        // Switch to transfers tab
        switchTab('transfers');
        showToast(`Downloading "${filename}"…`, 'info');
    } catch (e) {
        showToast(e.message, 'error');
    }
}


async function handleDismiss(filename, status) {
    try {
        if (status === 'failed') {
            await api.dismissDownload(filename);
            delete state.downloads[filename];
        } else if (status === 'completed') {
            state.completedDownloads = state.completedDownloads.filter(d => d.filename !== filename);
        }
        renderTransfers();
    } catch (e) {
        // Just remove from UI anyway
        delete state.downloads[filename];
        state.completedDownloads = state.completedDownloads.filter(d => d.filename !== filename);
        renderTransfers();
    }
}


// ── Throttle ─────────────────────────────────────────────────────────────────

async function applyThrottle(direction, limit) {
    try {
        const result = await api.setThrottle(direction, limit);
        showToast(`${direction === 'up' ? 'Upload' : 'Download'} limit → ${result.limit}`, 'success');
        // Refresh status
        const status = await api.getStatus();
        renderStatus(status);
    } catch (e) {
        showToast(e.message, 'error');
    }
}


// ── Tab Navigation ───────────────────────────────────────────────────────────

function switchTab(tabId) {
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tab === tabId);
    });
    document.querySelectorAll('.tab-panel').forEach(panel => {
        panel.classList.toggle('active', panel.id === `tab-${tabId}`);
    });
}

function initTabs() {
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });
}


// ── Drop Zone ────────────────────────────────────────────────────────────────

function initDropZone() {
    const zone = document.getElementById('drop-zone');
    const input = document.getElementById('file-input');
    let dragCounter = 0;

    zone.addEventListener('click', () => input.click());

    zone.addEventListener('dragenter', (e) => {
        e.preventDefault();
        dragCounter++;
        zone.classList.add('dragover');
    });

    zone.addEventListener('dragleave', (e) => {
        e.preventDefault();
        dragCounter--;
        if (dragCounter <= 0) {
            dragCounter = 0;
            zone.classList.remove('dragover');
        }
    });

    zone.addEventListener('dragover', (e) => {
        e.preventDefault();
    });

    zone.addEventListener('drop', (e) => {
        e.preventDefault();
        dragCounter = 0;
        zone.classList.remove('dragover');
        const files = e.dataTransfer.files;
        if (files.length > 0) uploadFile(files[0]);
    });

    input.addEventListener('change', () => {
        if (input.files.length > 0) uploadFile(input.files[0]);
        input.value = '';
    });
}


async function uploadFile(file) {
    const area = document.getElementById('upload-progress-area');

    // Show upload card
    area.innerHTML = `
        <div class="upload-status-card" id="upload-card">
            <div class="upload-info">
                <span class="upload-filename">${fileIcon(file.name)} ${file.name}</span>
                <span class="upload-status-text" id="upload-status-text">Uploading…</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill" id="upload-fill" style="width: 0%"></div>
            </div>
        </div>`;

    const fill = document.getElementById('upload-fill');
    const statusText = document.getElementById('upload-status-text');

    // Simulate upload progress (we can't track real multipart upload progress with fetch easily)
    let fakePct = 0;
    const fakeInterval = setInterval(() => {
        fakePct = Math.min(fakePct + Math.random() * 15, 85);
        fill.style.width = fakePct + '%';
    }, 200);

    try {
        statusText.textContent = 'Uploading…';
        const result = await api.shareFile(file);

        clearInterval(fakeInterval);
        fill.style.width = '100%';
        fill.classList.add('complete');
        statusText.textContent = 'Shared!';
        showToast(`"${file.name}" shared successfully`, 'success');

        // Refresh swarm
        setTimeout(async () => {
            const files = await api.getFiles();
            renderSwarm(files.files);
            const status = await api.getStatus();
            renderStatus(status);
        }, 500);

        // Remove upload card after a moment
        setTimeout(() => {
            const card = document.getElementById('upload-card');
            if (card) card.style.opacity = '0';
            setTimeout(() => { area.innerHTML = ''; }, 300);
        }, 3000);

    } catch (e) {
        clearInterval(fakeInterval);
        fill.style.width = '100%';
        fill.style.background = 'var(--error)';
        statusText.textContent = 'Failed';
        showToast(`Upload failed: ${e.message}`, 'error');
    }
}


// ── Share by path ────────────────────────────────────────────────────────────

function initSharePath() {
    const input = document.getElementById('path-input');
    const btn   = document.getElementById('btn-share-path');

    async function doShare() {
        const path = input.value.trim();
        if (!path) {
            showToast('Enter a file path', 'warning');
            return;
        }
        btn.disabled = true;
        btn.textContent = 'Sharing…';

        try {
            await api.sharePath(path);
            showToast(`Shared successfully`, 'success');
            input.value = '';

            // Refresh data
            const [files, status] = await Promise.all([api.getFiles(), api.getStatus()]);
            renderSwarm(files.files);
            renderStatus(status);
        } catch (e) {
            showToast(e.message, 'error');
        } finally {
            btn.disabled = false;
            btn.textContent = 'Share';
        }
    }

    btn.addEventListener('click', doShare);
    input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') doShare();
    });
}


// ── Settings ─────────────────────────────────────────────────────────────────

function initSettings() {
    // Upload throttle
    document.getElementById('btn-apply-up').addEventListener('click', () => {
        const val = document.getElementById('throttle-up-input').value.trim();
        if (val) applyThrottle('up', val);
    });
    document.getElementById('throttle-up-input').addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            const val = e.target.value.trim();
            if (val) applyThrottle('up', val);
        }
    });

    // Download throttle
    document.getElementById('btn-apply-down').addEventListener('click', () => {
        const val = document.getElementById('throttle-down-input').value.trim();
        if (val) applyThrottle('down', val);
    });
    document.getElementById('throttle-down-input').addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            const val = e.target.value.trim();
            if (val) applyThrottle('down', val);
        }
    });

    // Preset buttons
    document.getElementById('presets-up').addEventListener('click', (e) => {
        const btn = e.target.closest('.preset-btn');
        if (btn) applyThrottle('up', btn.dataset.val);
    });
    document.getElementById('presets-down').addEventListener('click', (e) => {
        const btn = e.target.closest('.preset-btn');
        if (btn) applyThrottle('down', btn.dataset.val);
    });
}


// ── Refresh button ───────────────────────────────────────────────────────────

function initRefresh() {
    const btn = document.getElementById('btn-refresh-swarm');
    btn.addEventListener('click', async () => {
        btn.classList.add('spinning');
        try {
            const [files, status] = await Promise.all([api.getFiles(), api.getStatus()]);
            renderSwarm(files.files);
            renderStatus(status);
        } catch (e) {
            showToast('Refresh failed', 'error');
        }
        setTimeout(() => btn.classList.remove('spinning'), 600);
    });
}


// ── WebSocket Event Dispatcher ───────────────────────────────────────────────

function handleWSEvent(event) {
    switch (event.type) {
        case '_ws_connected':
            state.wsConnected = true;
            document.getElementById('status-dot').classList.remove('disconnected');
            break;

        case '_ws_disconnected':
            state.wsConnected = false;
            document.getElementById('status-dot').classList.add('disconnected');
            break;

        case 'download_progress': {
            const dl = state.downloads[event.filename] || {};
            state.downloads[event.filename] = {
                ...dl,
                filename: event.filename,
                status: 'downloading',
                done_chunks: event.done,
                total_chunks: event.total,
                pct: event.pct,
                speed_mbps: event.speed_mbps,
                eta_sec: event.eta_sec,
            };
            // Targeted update — avoids full re-render so shimmer animation stays smooth
            updateTransferProgress(event.filename, state.downloads[event.filename]);
            break;
        }

        case 'download_complete': {
            const dl = state.downloads[event.filename] || {};
            delete state.downloads[event.filename];
            state.completedDownloads.push({
                ...dl,
                filename: event.filename,
                status: 'completed',
                pct: 100,
                elapsed: event.elapsed,
                speed_mbps: event.speed_mbps,
                total_size: event.total_size || dl.total_size,
                peer_stats: event.peer_stats,
            });
            renderTransfers();
            showToast(`"${event.filename}" downloaded successfully!`, 'success');

            // Refresh swarm (we're now seeding)
            setTimeout(async () => {
                const [files, status] = await Promise.all([api.getFiles(), api.getStatus()]);
                renderSwarm(files.files);
                renderStatus(status);
            }, 500);
            break;
        }

        case 'download_error': {
            if (state.downloads[event.filename]) {
                state.downloads[event.filename].status = 'failed';
                state.downloads[event.filename].error = event.error;
            }
            renderTransfers();
            showToast(`Download failed: ${event.error}`, 'error');
            break;
        }

        case 'file_shared': {
            showToast(`"${event.filename}" is now being shared`, 'success');
            // Refresh
            setTimeout(async () => {
                const [files, status] = await Promise.all([api.getFiles(), api.getStatus()]);
                renderSwarm(files.files);
                renderStatus(status);
            }, 300);
            break;
        }
    }
}


// ── Initialization ───────────────────────────────────────────────────────────

async function init() {
    // Init UI components
    initTabs();
    initDropZone();
    initSharePath();
    initSettings();
    initRefresh();

    // Initial data load
    try {
        const [status, files, downloads] = await Promise.all([
            api.getStatus(),
            api.getFiles(),
            api.getDownloads(),
        ]);

        renderStatus(status);
        renderSwarm(files.files);

        // Restore active downloads
        if (downloads.active) {
            for (const [k, v] of Object.entries(downloads.active)) {
                state.downloads[k] = v;
            }
        }
        if (downloads.completed) {
            state.completedDownloads = downloads.completed;
        }
        renderTransfers();

    } catch (e) {
        console.error('Initial load failed:', e);
        showToast('Failed to connect to peer', 'error');
    }

    // Connect WebSocket
    const ws = new WS(handleWSEvent);
    ws.connect();

    // Auto-refresh swarm every 10s
    setInterval(async () => {
        try {
            const files = await api.getFiles();
            renderSwarm(files.files);
        } catch (e) { /* silent */ }
    }, 10000);

    // Auto-refresh status every 8s
    setInterval(async () => {
        try {
            const status = await api.getStatus();
            renderStatus(status);
        } catch (e) { /* silent */ }
    }, 8000);
}

document.addEventListener('DOMContentLoaded', init);
