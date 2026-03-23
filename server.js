const express = require('express');
const http = require('http');
const { Server: WebSocketServer } = require('ws');
const { Client } = require('ssh2');
const net = require('net');
const fs = require('fs');
const path = require('path');
const dgram = require('dgram');
const os = require('os');

const crypto = require('crypto');
const dns = require('dns');
const { spawn, execSync } = require('child_process');
const multer = require('multer');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// ─── Global App Settings ────────────────────────────────────────────
const APP_SETTINGS_FILE = path.join(__dirname, 'app-settings.json');
let appSettings = {
  xrayBinaryPath: '',  // empty = auto-detect
  ocMode: 'network',   // 'local' or 'network'
  upstreamVpnMode: 'openvpn', // openvpn | openconnect | both
  adminUsername: 'admin',
  adminPassword: 'admin',
  ocConfig: {          // used in local mode
    url: '',
    authgroup: '',
    username: '',
    password: '',
  },
  dnsConfig: {
    servers: [],
    updateUrl: '',
  },
  spoofDpi: {
    port: 8080,
  },
};

function loadAppSettings() {
  try {
    if (fs.existsSync(APP_SETTINGS_FILE)) {
      appSettings = { ...appSettings, ...JSON.parse(fs.readFileSync(APP_SETTINGS_FILE, 'utf8')) };
      appSettings.ocConfig = { ...{
        url: '',
        authgroup: '',
        username: '',
        password: '',
      }, ...(appSettings.ocConfig || {}) };
      appSettings.dnsConfig = { ...{
        servers: [],
        updateUrl: '',
      }, ...(appSettings.dnsConfig || {}) };
      appSettings.spoofDpi = { ...{
        port: 8080,
      }, ...(appSettings.spoofDpi || {}) };
    }
  } catch {}
}
function saveAppSettings() {
  fs.writeFileSync(APP_SETTINGS_FILE, JSON.stringify(appSettings, null, 2));
}
loadAppSettings();

const SPOOFDPI_DEFAULTS = {
  host: '127.0.0.1',
  port: 8080,
};

let spoofDpiProcState = null;
let spoofDpiLogs = [];

function normalizeSpoofDpiPort(value, fallback = SPOOFDPI_DEFAULTS.port) {
  const port = parseInt(value, 10);
  if (port >= 1 && port <= 65535) return port;
  return fallback;
}

function getConfiguredSpoofDpiPort() {
  return normalizeSpoofDpiPort((appSettings.spoofDpi || {}).port, SPOOFDPI_DEFAULTS.port);
}

function getDefaultXrayRouteSocks() {
  return {
    host: SPOOFDPI_DEFAULTS.host,
    port: getConfiguredSpoofDpiPort(),
  };
}

function normalizeXrayRouteSocks(value, fallback = getDefaultXrayRouteSocks()) {
  const base = fallback && typeof fallback === 'object' ? fallback : getDefaultXrayRouteSocks();
  const next = value && typeof value === 'object' ? value : {};
  const host = String(next.host || base.host || SPOOFDPI_DEFAULTS.host).trim() || SPOOFDPI_DEFAULTS.host;
  return {
    host,
    port: normalizeSpoofDpiPort(next.port, base.port || SPOOFDPI_DEFAULTS.port),
  };
}

function findSpoofDpiBinary() {
  try {
    return execSync('command -v spoofdpi 2>/dev/null', { encoding: 'utf8', timeout: 2000 }).trim();
  } catch {}
  return '';
}

function appendSpoofDpiLog(line) {
  if (!line) return;
  spoofDpiLogs.push({ t: Date.now(), m: line });
  if (spoofDpiLogs.length > 400) spoofDpiLogs.shift();
}

function isSpoofDpiRunning() {
  return !!(spoofDpiProcState && !spoofDpiProcState.stopping);
}

function getSpoofDpiStatusPayload() {
  const port = spoofDpiProcState ? spoofDpiProcState.port : getConfiguredSpoofDpiPort();
  const host = SPOOFDPI_DEFAULTS.host;
  const binary = findSpoofDpiBinary();
  return {
    host,
    port,
    listenAddr: `${host}:${port}`,
    command: `spoofdpi --listen-addr ${host}:${port}`,
    binary,
    commandAvailable: !!binary,
    running: isSpoofDpiRunning(),
    pid: spoofDpiProcState && spoofDpiProcState.proc ? spoofDpiProcState.proc.pid : null,
    startedAt: spoofDpiProcState ? spoofDpiProcState.startedAt : null,
    logs: spoofDpiLogs.slice(-80),
  };
}

async function waitForSpoofDpiStop(timeoutMs = 3000) {
  const startedAt = Date.now();
  while (isSpoofDpiRunning() && (Date.now() - startedAt) < timeoutMs) {
    await new Promise(resolve => setTimeout(resolve, 100));
  }
}

async function startSpoofDpiProcess(portOverride = null) {
  if (isSpoofDpiRunning()) return spoofDpiProcState;

  const binary = findSpoofDpiBinary();
  if (!binary) throw new Error('spoofdpi binary not found in PATH');

  const port = normalizeSpoofDpiPort(portOverride, getConfiguredSpoofDpiPort());
  if (isTcpPortInUse(port)) {
    throw new Error(`Port ${port} is already in use by another process.`);
  }

  const listenAddr = `${SPOOFDPI_DEFAULTS.host}:${port}`;
  appendSpoofDpiLog(`[panel] starting ${binary} --listen-addr ${listenAddr}`);

  let proc;
  try {
    proc = spawn(binary, ['--listen-addr', listenAddr], {
      stdio: ['ignore', 'pipe', 'pipe'],
    });
  } catch (e) {
    throw new Error(`Failed to spawn spoofdpi: ${e.message}`);
  }

  const state = {
    proc,
    port,
    listenAddr,
    startedAt: Date.now(),
    stopping: false,
    finalized: false,
    recentStderr: '',
  };
  spoofDpiProcState = state;

  const finalize = (line) => {
    if (state.finalized) return;
    state.finalized = true;
    if (line) appendSpoofDpiLog(line);
    if (spoofDpiProcState === state) spoofDpiProcState = null;
  };

  proc.stdout.on('data', data => {
    const text = data.toString().trim();
    if (!text) return;
    text.split('\n').forEach(line => appendSpoofDpiLog(`[stdout] ${line}`));
  });

  proc.stderr.on('data', data => {
    const text = data.toString().trim();
    if (!text) return;
    state.recentStderr += text + '\n';
    state.recentStderr = state.recentStderr.slice(-4000);
    text.split('\n').forEach(line => appendSpoofDpiLog(`[stderr] ${line}`));
  });

  proc.on('error', err => finalize(`[process error: ${err.message}]`));
  proc.on('exit', code => finalize(`[process exited code=${code}]`));

  await new Promise(resolve => setTimeout(resolve, 700));
  if (proc.exitCode !== null) {
    throw new Error(`SpoofDPI exited immediately: ${state.recentStderr.trim() || `exit code ${proc.exitCode}`}`);
  }

  return state;
}

function stopSpoofDpiProcess(reason = 'Stopped by user') {
  const state = spoofDpiProcState;
  if (!state || state.stopping) return false;
  state.stopping = true;
  appendSpoofDpiLog(`[panel] ${reason}`);
  try { state.proc.kill('SIGTERM'); } catch {}
  return true;
}

async function restartSpoofDpiProcess(reason = 'Restarted by panel') {
  if (isSpoofDpiRunning()) {
    stopSpoofDpiProcess(reason);
    await waitForSpoofDpiStop();
  }
  return startSpoofDpiProcess();
}

const additiveJsonBackups = new Set();

function ensureAdditiveJsonBackup(filePath, backupKey, shouldBackup = true) {
  if (!shouldBackup || additiveJsonBackups.has(backupKey)) return;
  additiveJsonBackups.add(backupKey);
  if (!fs.existsSync(filePath)) return;
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const backupPath = `${filePath}.bak-${stamp}`;
  try {
    fs.copyFileSync(filePath, backupPath);
    console.log(`[Backup] Preserved ${path.basename(filePath)} -> ${path.basename(backupPath)}`);
  } catch (e) {
    console.error(`[Backup] Failed to preserve ${filePath}:`, e.message);
  }
}

function safeIdFragment(value) {
  return String(value || '')
    .replace(/[^a-zA-Z0-9_-]/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .slice(0, 48) || 'default';
}

function autoDetectXrayBinary() {
  // 1. Bundled binaries in bin/ folder based on OS and arch
  const platform = os.platform(); // 'linux', 'darwin', etc.
  const arch = os.arch(); // 'x64', 'arm64', etc.
  const binDir = path.join(__dirname, 'bin');

  if (platform === 'darwin') {
    // macOS
    const armPath = path.join(binDir, 'macos', 'arm', 'xray');
    const x64Path = path.join(binDir, 'macos', 'x64', 'xray');
    if (arch === 'arm64' && fs.existsSync(armPath)) return armPath;
    if (arch === 'x64' && fs.existsSync(x64Path)) return x64Path;
    // fallback: try the other arch
    if (fs.existsSync(armPath)) return armPath;
    if (fs.existsSync(x64Path)) return x64Path;
  } else if (platform === 'linux') {
    const x64Path = path.join(binDir, 'linux', 'x64', 'xray');
    const arm64Path = path.join(binDir, 'linux', 'arm64', 'xray');
    if (arch === 'x64' && fs.existsSync(x64Path)) return x64Path;
    if (arch === 'arm64' && fs.existsSync(arm64Path)) return arm64Path;
    if (fs.existsSync(x64Path)) return x64Path;
  }

  // 2. System-installed xray
  for (const p of ['/opt/homebrew/bin/xray', '/usr/local/bin/xray', '/usr/bin/xray']) {
    if (fs.existsSync(p)) return p;
  }
  try { return execSync('which xray 2>/dev/null').toString().trim(); } catch {}
  return null;
}

// ─── Authentication ────────────────────────────────────────────────
function getAuthUser() {
  return String(appSettings.adminUsername || 'admin').trim() || 'admin';
}

function getAuthPass() {
  return String(appSettings.adminPassword || 'admin');
}

const activeSessions = new Map(); // token -> { role, username }

function generateToken() {
  return crypto.randomBytes(32).toString('hex');
}

function extractToken(req) {
  const cookies = req.headers.cookie || '';
  const match = cookies.match(/(?:^|;\s*)token=([^;]+)/);
  return match ? match[1] : null;
}

function getSession(req) {
  const token = extractToken(req);
  return token ? activeSessions.get(token) : null;
}

function isAuthenticated(req) {
  return !!getSession(req);
}

function isAdmin(req) {
  const session = getSession(req);
  return session && session.role === 'admin';
}

// Login page route
app.get('/login', (req, res) => {
  if (isAuthenticated(req)) return res.redirect('/');
  res.sendFile(path.join(__dirname, 'public', 'login.html'));
});

app.post('/login', (req, res) => {
  const { username, password } = req.body;
  if (username === getAuthUser() && password === getAuthPass()) {
    const token = generateToken();
    activeSessions.set(token, { role: 'admin', username: getAuthUser() });
    res.setHeader('Set-Cookie', `token=${token}; Path=/; HttpOnly; SameSite=Strict; Max-Age=86400`);
    return res.redirect('/');
  }
  res.redirect('/login?error=1');
});

app.post('/logout', (req, res) => {
  const token = extractToken(req);
  if (token) activeSessions.delete(token);
  res.setHeader('Set-Cookie', `token=; Path=/; HttpOnly; Max-Age=0`);
  res.redirect('/login');
});
// Auth middleware — protect everything except /login and its assets
app.use((req, res, next) => {
  if (req.path === '/login' || req.path === '/login.html') return next();
  if (req.path.startsWith('/sub/')) return next();
  if (!isAuthenticated(req)) return res.redirect('/login');
  next();
});

app.use(express.static(path.join(__dirname, 'public')));

// ─── In-memory state ────────────────────────────────────────────────
const DATA_FILE = path.join(__dirname, 'tunnels.json');

let tunnels = [];          // Array of tunnel configs
let passwords = {};        // { tunnelId: password }  – never persisted
let activeTunnelId = null;
let sshConnection = null;
let socksTunnelProc = null;  // child process for SOCKS (-D) tunnel
let socksHealthTimer = null; // interval for SOCKS tunnel health monitoring
let reconnectTimer = null;
let reconnectAttempt = 0;
let shouldReconnect = false;
let connectionStatus = 'disconnected'; // connecting | connected | disconnected | reconnecting | error
let statusMessage = '';
let sessionStats = {
  bytesIn: 0,
  bytesOut: 0,
  activeConnections: 0,
  connectedSince: null,
  reconnectCount: 0,
};
let localConnections = new Map(); // socketId -> { socket, stream, srcIP, destPort, bytesIn, bytesOut, connectedAt }
let connIdCounter = 0;
let ipInfo = {};        // { ip: { rdns, firstSeen, totalBytesIn, totalBytesOut, totalConnections } }
let ipSpeedLimits = {}; // { ip: { download: bytes/s, upload: bytes/s } }  — 0 = unlimited
let ipThrottles = {};   // { ip: { dlBucket, ulBucket, paused: Set<streamId> } }
let remoteClients = []; // [{ clientIP, clientPort, serverPort }] from `ss` on remote
let panelExposed = false;  // Whether panel is forwarded through SSH
let lastBytesIn = 0;
let lastBytesOut = 0;
let bandwidthIn = 0;       // bytes/sec download
let bandwidthOut = 0;      // bytes/sec upload
const PANEL_REMOTE_PORT = 3001; // Port on SSH server to access the panel
const SSH_ROUTE_DEVICE_DEFAULT = 'tun0';
let sshTunnelLogs = [];
const MAX_SSH_TUNNEL_LOGS = 500;

// Health check state
let healthStatus = { latency: null, lastCheck: null, healthy: true, history: [] };
const MAX_HEALTH_HISTORY = 60; // ~30 min at 30s interval
const MAX_TUNNEL_HEALTH_FAILURES = 2;
let healthFailureStreak = 0;

// ─── Persistence (configs only, no passwords) ───────────────────────
function loadTunnels() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      tunnels = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    }
  } catch (e) {
    console.error('Failed to load tunnels:', e.message);
    tunnels = [];
  }
}

function saveTunnels() {
  fs.writeFileSync(DATA_FILE, JSON.stringify(tunnels, null, 2));
}

loadTunnels();

// ─── Audit Log (persisted) ──────────────────────────────────────────
const AUDIT_FILE = path.join(__dirname, 'audit.json');
let auditLog = [];
const MAX_AUDIT = 500;
let auditWriteTimer = null;

function loadAudit() {
  try {
    if (fs.existsSync(AUDIT_FILE)) auditLog = JSON.parse(fs.readFileSync(AUDIT_FILE, 'utf8'));
  } catch { auditLog = []; }
}

function addAudit(event, detail = '') {
  const entry = { time: Date.now(), event, detail };
  auditLog.push(entry);
  if (auditLog.length > MAX_AUDIT) auditLog = auditLog.slice(-MAX_AUDIT);
  broadcast({ type: 'audit', entry });
  if (!auditWriteTimer) {
    auditWriteTimer = setTimeout(() => {
      auditWriteTimer = null;
      try { fs.writeFileSync(AUDIT_FILE, JSON.stringify(auditLog)); } catch {}
    }, 2000);
  }
}

loadAudit();

// ─── Helper: generate simple ID ─────────────────────────────────────
function genId() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
}

// ─── Remote Client Discovery (real IPs via SSH exec) ────────────────
let remoteClientPollTimer = null;

function startRemoteClientPolling(conn, remotePort) {
  stopRemoteClientPolling();
  pollRemoteClients(conn, remotePort);
  remoteClientPollTimer = setInterval(() => pollRemoteClients(conn, remotePort), 3000);
}

function stopRemoteClientPolling() {
  clearInterval(remoteClientPollTimer);
  remoteClientPollTimer = null;
  remoteClients = [];
}

function pollRemoteClients(conn, remotePort) {
  if (!conn || !sshConnection) return;
  // Use conntrack to see real client IPs before iptables DNAT translation
  // conntrack output: tcp 6 431997 ESTABLISHED src=<REAL_IP> dst=185.x.x.x sport=xxxxx dport=1234 src=127.0.0.1 dst=127.0.0.1 sport=1234 dport=xxxxx
  const cmd = `conntrack -L -p tcp --dport ${remotePort} 2>/dev/null`;
  conn.exec(cmd, (err, stream) => {
    if (err) return;
    let output = '';
    stream.on('data', (data) => { output += data.toString(); });
    stream.stderr.on('data', () => {}); // ignore stderr
    stream.on('close', () => {
      const clients = [];
      const lines = output.split('\n').filter(l => l.trim());
      if (remoteClients.length === 0 && lines.length > 0) {
        console.log(`[RemoteClients] conntrack output (${lines.length} lines), first 5:`);
        lines.slice(0, 5).forEach(l => console.log(`  ${l}`));
      }
      for (const line of lines) {
        // Parse first src= field (real client IP before NAT)
        const srcMatch = line.match(/src=(\d+\.\d+\.\d+\.\d+)/);
        const sportMatch = line.match(/sport=(\d+)/);
        if (!srcMatch) continue;
        const clientIP = srcMatch[1];
        const clientPort = sportMatch ? parseInt(sportMatch[1]) : 0;
        // Skip loopback/local IPs
        if (clientIP === '127.0.0.1' || clientIP === '0.0.0.0') continue;
        clients.push({ clientIP, clientPort });
      }
      // Deduplicate
      const seen = new Set();
      remoteClients = clients.filter(c => {
        const key = `${c.clientIP}:${c.clientPort}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
      // Resolve DNS for new IPs
      for (const rc of remoteClients) {
        resolveIpInfo(rc.clientIP);
      }
    });
  });
}

// ─── IP Info & Throttling ────────────────────────────────────────────
function resolveIpInfo(ip) {
  if (ipInfo[ip]) return;
  ipInfo[ip] = { rdns: null, firstSeen: Date.now(), totalBytesIn: 0, totalBytesOut: 0, totalConnections: 0 };
  dns.reverse(ip, (err, hostnames) => {
    if (!err && hostnames && hostnames.length > 0) {
      ipInfo[ip].rdns = hostnames[0];
    }
  });
}

const geoIpLookupCache = new Map(); // ip -> { country, cc, city, lat, lon } | null

function isPrivateOrLocalIp(ip) {
  const value = String(ip || '').trim();
  if (!value) return true;
  if (value === '::1' || /^fe80:/i.test(value) || /^fc/i.test(value) || /^fd/i.test(value)) return true;
  if (!net.isIP(value)) return true;
  if (/^10\./.test(value) || /^127\./.test(value) || /^169\.254\./.test(value) || /^192\.168\./.test(value)) return true;
  const octets = value.split('.').map(part => parseInt(part, 10));
  if (octets.length === 4 && octets[0] === 172 && octets[1] >= 16 && octets[1] <= 31) return true;
  return false;
}

async function lookupGeoIpBatch(ips) {
  const uniqueIps = [...new Set((Array.isArray(ips) ? ips : []).map(ip => String(ip || '').trim()).filter(Boolean))];
  const result = {};
  const pending = [];

  for (const ip of uniqueIps) {
    if (isPrivateOrLocalIp(ip)) {
      result[ip] = null;
      continue;
    }
    if (geoIpLookupCache.has(ip)) {
      result[ip] = geoIpLookupCache.get(ip);
      continue;
    }
    pending.push(ip);
  }

  if (pending.length > 0) {
    for (let i = 0; i < pending.length; i += 50) {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 8000);
      try {
        const response = await fetch('http://ip-api.com/batch?fields=status,message,query,country,countryCode,city,lat,lon', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(pending.slice(i, i + 50).map(ip => ({ query: ip }))),
          signal: controller.signal,
        });
        const payload = await response.json();
        if (Array.isArray(payload)) {
          for (const entry of payload) {
            const ip = String(entry && entry.query || '').trim();
            if (!ip) continue;
            const geo = entry.status === 'success'
              ? {
                  country: entry.country || '',
                  cc: entry.countryCode || '',
                  city: entry.city || '',
                  lat: typeof entry.lat === 'number' ? entry.lat : null,
                  lon: typeof entry.lon === 'number' ? entry.lon : null,
                }
              : null;
            geoIpLookupCache.set(ip, geo);
            result[ip] = geo;
          }
        }
      } catch (e) {
        console.error('[GeoIP] Batch lookup failed:', e.message);
      } finally {
        clearTimeout(timeout);
      }
    }
  }

  for (const ip of uniqueIps) {
    if (!(ip in result)) result[ip] = geoIpLookupCache.has(ip) ? geoIpLookupCache.get(ip) : null;
  }

  return result;
}

function getIpThrottle(ip) {
  if (!ipThrottles[ip]) {
    ipThrottles[ip] = { dlBytes: 0, ulBytes: 0, lastReset: Date.now(), pausedStreams: new Map() };
  }
  return ipThrottles[ip];
}

// Reset throttle buckets every second
setInterval(() => {
  const now = Date.now();
  for (const ip in ipThrottles) {
    const t = ipThrottles[ip];
    if (now - t.lastReset >= 1000) {
      t.dlBytes = 0;
      t.ulBytes = 0;
      t.lastReset = now;
      // Resume any paused streams
      for (const [, info] of t.pausedStreams) {
        if (info.stream && !info.stream.destroyed) info.stream.resume();
        if (info.socket && !info.socket.destroyed) info.socket.resume();
      }
      t.pausedStreams.clear();
    }
  }
}, 200);

function throttleCheck(ip, connId, stream, socket, direction, chunkLen) {
  const limit = ipSpeedLimits[ip];
  if (!limit) return false;
  const t = getIpThrottle(ip);
  const maxBytes = direction === 'dl' ? limit.download : limit.upload;
  if (!maxBytes || maxBytes <= 0) return false;
  const current = direction === 'dl' ? t.dlBytes : t.ulBytes;
  if (current + chunkLen > maxBytes) {
    // Pause the source
    if (direction === 'dl' && socket && !socket.destroyed) socket.pause();
    if (direction === 'ul' && stream && !stream.destroyed) stream.pause();
    t.pausedStreams.set(connId, { stream, socket });
    return true;
  }
  if (direction === 'dl') t.dlBytes += chunkLen;
  else t.ulBytes += chunkLen;
  return false;
}

// ─── WebSocket broadcast ────────────────────────────────────────────
function broadcast(data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(c => {
    if (c.readyState === 1) c.send(msg);
  });
}

function broadcastNotification(event, message) {
  broadcast({ type: 'notification', event, message, time: Date.now() });
}

function addSshTunnelLog(text) {
  const entry = { time: Date.now(), text };
  sshTunnelLogs.push(entry);
  if (sshTunnelLogs.length > MAX_SSH_TUNNEL_LOGS) {
    sshTunnelLogs = sshTunnelLogs.slice(-MAX_SSH_TUNNEL_LOGS);
  }
  broadcast({ type: 'ssh_tunnel_log', entry });
}

function getConnectionsList() {
  const conns = [];
  for (const [id, c] of localConnections) {
    const info = ipInfo[c.srcIP] || {};
    const limit = ipSpeedLimits[c.srcIP] || null;
    conns.push({
      id, srcIP: c.srcIP, srcPort: c.srcPort, destPort: c.destPort,
      bytesIn: c.bytesIn, bytesOut: c.bytesOut,
      connectedAt: c.connectedAt, duration: Date.now() - c.connectedAt,
      rdns: info.rdns || null,
      ipTotalBytesIn: info.totalBytesIn || 0,
      ipTotalBytesOut: info.totalBytesOut || 0,
      ipTotalConnections: info.totalConnections || 0,
      ipFirstSeen: info.firstSeen || null,
      speedLimit: limit,
    });
  }
  return conns;
}

function getRemoteClientsList() {
  return remoteClients.map(rc => {
    const info = ipInfo[rc.clientIP] || {};
    const limit = ipSpeedLimits[rc.clientIP] || null;
    return {
      clientIP: rc.clientIP,
      clientPort: rc.clientPort,
      rdns: info.rdns || null,
      totalBytesIn: info.totalBytesIn || 0,
      totalBytesOut: info.totalBytesOut || 0,
      totalConnections: info.totalConnections || 0,
      firstSeen: info.firstSeen || null,
      speedLimit: limit,
    };
  });
}

function broadcastState() {
  const activeTunnel = tunnels.find(t => t.id === activeTunnelId);
  broadcast({
    type: 'state',
    activeTunnelId,
    status: connectionStatus,
    statusMessage,
    panelUrl: panelExposed && activeTunnel ? `http://${activeTunnel.host}:${PANEL_REMOTE_PORT}` : null,
    stats: {
      ...sessionStats,
      uptime: sessionStats.connectedSince
        ? Date.now() - sessionStats.connectedSince
        : 0,
      bandwidthIn,
      bandwidthOut,
    },
    connections: getConnectionsList(),
    remoteClients: getRemoteClientsList(),
    health: healthStatus,
  });
}

// Periodic broadcast (1s) — also computes bandwidth rate
setInterval(() => {
  if (activeTunnelId) {
    bandwidthIn = sessionStats.bytesIn - lastBytesIn;
    bandwidthOut = sessionStats.bytesOut - lastBytesOut;
    lastBytesIn = sessionStats.bytesIn;
    lastBytesOut = sessionStats.bytesOut;
    broadcastState();
  }
}, 1000);

// ─── SSH Tunnel Logic ───────────────────────────────────────────────

function stopTunnel(reason = '') {
  addSshTunnelLog(`Stopping tunnel${reason ? `: ${reason}` : ''}`);
  shouldReconnect = false;
  clearTimeout(reconnectTimer);
  reconnectTimer = null;
  stopRemoteClientPolling();

  // Stop SOCKS tunnel if active
  if (socksTunnelProc) {
    stopSocksTunnel();
  }

  // Close all local connections
  for (const [, conn] of localConnections) {
    conn.stream.destroy();
    conn.socket.destroy();
  }
  localConnections.clear();

  if (sshConnection) {
    sshConnection.end();
    sshConnection = null;
  }

  connectionStatus = 'disconnected';
  statusMessage = reason || 'Stopped by user';
  panelExposed = false;
  const stoppedId = activeTunnelId;
  activeTunnelId = null;
  sessionStats = { bytesIn: 0, bytesOut: 0, activeConnections: 0, connectedSince: null, reconnectCount: 0 };
  broadcastState();
  console.log(`[Tunnel] Stopped${reason ? ': ' + reason : ''}`);
  addAudit('tunnel_stopped', reason || 'User action');
  return stoppedId;
}

function getLinuxRouteDevice(host) {
  if (process.platform !== 'linux' || !host || !net.isIP(host)) return null;
  try {
    const output = execSync(`ip route get ${host}`, {
      stdio: ['ignore', 'pipe', 'pipe'],
      encoding: 'utf8',
      timeout: 5000,
    }).trim();
    const match = output.match(/\bdev\s+(\S+)/);
    return match ? match[1] : null;
  } catch {
    return null;
  }
}

function ensureTunnelHostRoute(host, routeInterface) {
  // routeInterface: 'tun0' (force VPN), 'direct' (skip routing), or undefined/auto (use tun0)
  if (routeInterface === 'direct') {
    addSshTunnelLog(`Route skipped for ${host} (direct mode)`);
    return { ok: true, skipped: true, device: 'direct' };
  }

  if (!host || !net.isIP(host) || process.platform !== 'linux') {
    return { ok: true, skipped: true };
  }

  const device = routeInterface || SSH_ROUTE_DEVICE_DEFAULT;

  addSshTunnelLog(`Checking route for ${host} via ${device}`);
  const currentDevice = getLinuxRouteDevice(host);
  if (currentDevice === device) {
    addSshTunnelLog(`Route already present: ${host} -> ${device}`);
    return { ok: true, alreadyPresent: true, device };
  }

  try {
    execSync(`ip route add ${host} dev ${device}`, {
      stdio: ['ignore', 'pipe', 'pipe'],
      timeout: 5000,
    });
  } catch (addErr) {
    try {
      execSync(`ip route replace ${host} dev ${device}`, {
        stdio: ['ignore', 'pipe', 'pipe'],
        timeout: 5000,
      });
    } catch (replaceErr) {
      const stderr = replaceErr.stderr ? replaceErr.stderr.toString().trim() : '';
      return {
        ok: false,
        error: stderr || replaceErr.message || `Failed to route ${host} via ${device}`,
      };
    }
  }

  const verifiedDevice = getLinuxRouteDevice(host);
  if (verifiedDevice !== device) {
    return {
      ok: false,
      error: `Route for ${host} is using ${verifiedDevice || 'unknown'} instead of ${device}`,
    };
  }

  console.log(`[Tunnel] Route ensured: ${host} -> ${device}`);
  addSshTunnelLog(`Route ensured: ${host} -> ${device}`);
  return { ok: true, added: true, device };
}

function scheduleReconnectSsh(tunnel, password) {
  if (!shouldReconnect || !activeTunnelId) return;

  reconnectAttempt++;
  sessionStats.reconnectCount++;
  const delay = Math.min(1000 * Math.pow(2, reconnectAttempt - 1), 30000);
  clearTimeout(reconnectTimer);
  reconnectTimer = null;
  connectionStatus = 'reconnecting';
  statusMessage = `Reconnecting in ${(delay / 1000).toFixed(0)}s (attempt ${reconnectAttempt})...`;
  broadcastState();
  console.log(`[SSH] Reconnecting in ${delay}ms...`);
  addSshTunnelLog(`Scheduling reconnect in ${(delay / 1000).toFixed(0)}s (attempt ${reconnectAttempt})`);
  broadcastNotification('tunnel_reconnecting', `SSH tunnel lost, reconnecting in ${(delay / 1000).toFixed(0)}s...`);
  addAudit('tunnel_reconnecting', `Attempt ${reconnectAttempt}`);
  reconnectTimer = setTimeout(() => {
    if (shouldReconnect && activeTunnelId) {
      const t = tunnels.find(x => x.id === activeTunnelId);
      const p = passwords[activeTunnelId];
      if (t && p) connect(t, p);
    }
  }, delay);
}

function maybeReconnectTunnelFromHealthFailure(tunnel, reason) {
  if (!tunnel || tunnel.type === 'socks' || !sshConnection || connectionStatus !== 'connected') return;

  healthFailureStreak++;
  addSshTunnelLog(`Health check failed for ${tunnel.host} (${reason}), streak ${healthFailureStreak}/${MAX_TUNNEL_HEALTH_FAILURES}`);
  if (healthFailureStreak < MAX_TUNNEL_HEALTH_FAILURES) return;

  healthFailureStreak = 0;
  console.log(`[SSH] Health check forcing reconnect: ${reason}`);
  addSshTunnelLog(`Health check forcing reconnect for ${tunnel.host}: ${reason}`);
  statusMessage = `Health check failed, reconnecting...`;
  broadcastState();
  broadcastNotification('tunnel_reconnecting', `Health check failed for ${tunnel.host}, reconnecting...`);
  try { sshConnection.end(); } catch {}
}

function startTunnel(tunnelId) {
  const tunnel = tunnels.find(t => t.id === tunnelId);
  if (!tunnel) throw new Error('Tunnel not found');

  const password = passwords[tunnelId];
  if (!password) throw new Error('Password not set for this tunnel');

  // Stop any existing tunnel
  if (activeTunnelId) stopTunnel('Switching tunnel');

  activeTunnelId = tunnelId;
  shouldReconnect = true;
  reconnectAttempt = 0;
  sessionStats = { bytesIn: 0, bytesOut: 0, activeConnections: 0, connectedSince: null, reconnectCount: 0 };
  addSshTunnelLog(`Starting ${tunnel.type === 'socks' ? 'SOCKS' : 'reverse'} tunnel ${tunnel.name} to ${tunnel.username}@${tunnel.host}:${tunnel.sshPort || 22}`);

  if (tunnel.type === 'socks') {
    connectSocks(tunnel, password);
  } else {
    connect(tunnel, password);
  }
}

function connect(tunnel, password) {
  connectionStatus = reconnectAttempt > 0 ? 'reconnecting' : 'connecting';
  statusMessage = reconnectAttempt > 0
    ? `Reconnect attempt ${reconnectAttempt}...`
    : 'Connecting...';
  broadcastState();
  addSshTunnelLog(`Connecting reverse tunnel to ${tunnel.username}@${tunnel.host}:${tunnel.sshPort || 22}`);

  const routeResult = ensureTunnelHostRoute(tunnel.host, tunnel.routeInterface);
  if (!routeResult.ok) {
    console.error(`[SSH] Route preflight failed for ${tunnel.host}: ${routeResult.error}`);
    addSshTunnelLog(`Route preflight failed for ${tunnel.host}: ${routeResult.error}`);
    connectionStatus = 'error';
    statusMessage = `Route failed: ${routeResult.error}`;
    broadcastState();
    broadcastNotification('tunnel_error', `Route failed for ${tunnel.host}: ${routeResult.error}`);
    addAudit('tunnel_error', `Route failed for ${tunnel.host}: ${routeResult.error}`);
    scheduleReconnectSsh(tunnel, password);
    return;
  }
  if (routeResult.added) {
    statusMessage = `Route ${tunnel.host} -> ${routeResult.device} added. Connecting...`;
    broadcastState();
  }

  const conn = new Client();
  sshConnection = conn;

  conn.on('ready', () => {
    const wasReconnect = reconnectAttempt > 0;
    console.log(`[SSH] Connected to ${tunnel.host}:${tunnel.sshPort}`);
    addSshTunnelLog(`SSH connected to ${tunnel.host}:${tunnel.sshPort || 22}`);
    connectionStatus = 'connected';
    statusMessage = `Connected to ${tunnel.host}`;
    sessionStats.connectedSince = Date.now();
    reconnectAttempt = 0;
    healthFailureStreak = 0;
    panelExposed = false;
    broadcastState();
    broadcastNotification(wasReconnect ? 'tunnel_reconnected' : 'tunnel_connected',
      `SSH tunnel ${wasReconnect ? 'reconnected' : 'connected'} to ${tunnel.host}`);
    addAudit(wasReconnect ? 'tunnel_reconnected' : 'tunnel_connected', `${tunnel.name} (${tunnel.host})`);

    // Request reverse port forwarding for the tunnel
    conn.forwardIn('0.0.0.0', tunnel.remotePort, (err) => {
      if (err) {
        console.error(`[SSH] Forward error:`, err.message);
        addSshTunnelLog(`Reverse forward failed on remote port ${tunnel.remotePort}: ${err.message}`);
        connectionStatus = 'error';
        statusMessage = `Forward failed: ${err.message}`;
        broadcastState();
        conn.end();
        return;
      }
      console.log(`[SSH] Reverse forwarding: remote:${tunnel.remotePort} -> localhost:${tunnel.localPort}`);
      addSshTunnelLog(`Reverse forwarding active: remote:${tunnel.remotePort} -> ${tunnel.localHost || 'localhost'}:${tunnel.localPort}`);
      statusMessage = `Forwarding remote:${tunnel.remotePort} → localhost:${tunnel.localPort}`;
      broadcastState();
      // Start polling remote server for real client IPs
      startRemoteClientPolling(conn, tunnel.remotePort);
    });

    // Also forward the panel itself so it's accessible via SSH server
    conn.forwardIn('0.0.0.0', PANEL_REMOTE_PORT, (err) => {
      if (err) {
        console.error(`[SSH] Panel forward error:`, err.message);
        addSshTunnelLog(`Panel forward failed on remote port ${PANEL_REMOTE_PORT}: ${err.message}`);
        return;
      }
      panelExposed = true;
      console.log(`[SSH] Panel accessible at http://${tunnel.host}:${PANEL_REMOTE_PORT}`);
      addSshTunnelLog(`Panel forward active on remote port ${PANEL_REMOTE_PORT}`);
      broadcastState();
    });
  });

  // Handle incoming forwarded connections (tunnel + panel)
  const panelLocalPort = parseInt(process.env.PORT) || 3000;
  conn.on('tcp connection', (info, accept, reject) => {
    console.log(`[SSH] Incoming connection from ${info.srcIP}:${info.srcPort} -> port ${info.destPort}`);
    const stream = accept();
    sessionStats.activeConnections++;

    // Route to panel or to the configured tunnel target
    const targetPort = info.destPort === PANEL_REMOTE_PORT ? panelLocalPort : tunnel.localPort;
    const targetHost = info.destPort === PANEL_REMOTE_PORT ? 'localhost' : (tunnel.localHost || 'localhost');

    const localSocket = net.createConnection({
      host: targetHost,
      port: targetPort,
    });

    const connId = ++connIdCounter;
    const srcIP = info.srcIP;
    const connEntry = {
      socket: localSocket,
      stream,
      srcIP,
      srcPort: info.srcPort,
      destPort: info.destPort,
      bytesIn: 0,
      bytesOut: 0,
      connectedAt: Date.now(),
    };
    localConnections.set(connId, connEntry);

    // Track IP info
    resolveIpInfo(srcIP);
    ipInfo[srcIP].totalConnections++;

    // Use manual piping for throttle support
    stream.on('data', (chunk) => {
      sessionStats.bytesIn += chunk.length;
      connEntry.bytesIn += chunk.length;
      ipInfo[srcIP].totalBytesIn += chunk.length;
      if (!throttleCheck(srcIP, connId, stream, localSocket, 'ul', chunk.length)) {
        if (!localSocket.destroyed) localSocket.write(chunk);
      } else {
        // Buffered — write anyway but pause source
        if (!localSocket.destroyed) localSocket.write(chunk);
      }
    });
    localSocket.on('data', (chunk) => {
      sessionStats.bytesOut += chunk.length;
      connEntry.bytesOut += chunk.length;
      ipInfo[srcIP].totalBytesOut += chunk.length;
      if (!throttleCheck(srcIP, connId, stream, localSocket, 'dl', chunk.length)) {
        if (!stream.destroyed) stream.write(chunk);
      } else {
        if (!stream.destroyed) stream.write(chunk);
      }
    });

    const cleanup = () => {
      sessionStats.activeConnections = Math.max(0, sessionStats.activeConnections - 1);
      localConnections.delete(connId);
      stream.destroy();
      localSocket.destroy();
    };

    stream.on('close', cleanup);
    stream.on('error', cleanup);
    localSocket.on('close', cleanup);
    localSocket.on('error', cleanup);
  });

  conn.on('error', (err) => {
    console.error(`[SSH] Error:`, err.message);
    addSshTunnelLog(`SSH error: ${err.message}`);
    connectionStatus = 'error';
    statusMessage = err.message;
    broadcastState();
    broadcastNotification('tunnel_error', `SSH error: ${err.message}`);
    addAudit('tunnel_error', err.message);
  });

  conn.on('close', () => {
    console.log(`[SSH] Connection closed`);
    addSshTunnelLog('SSH connection closed');
    sshConnection = null;

    // Close all local connections
    for (const [, c] of localConnections) {
      c.stream.destroy();
      c.socket.destroy();
    }
    localConnections.clear();
    sessionStats.activeConnections = 0;

    if (shouldReconnect && activeTunnelId) {
      scheduleReconnectSsh(tunnel, password);
    } else {
      connectionStatus = 'disconnected';
      broadcastState();
    }
  });
  
  conn.connect({
    host: tunnel.host,
    port: tunnel.sshPort || 22,
    username: tunnel.username,
    password: password,
    readyTimeout: 15000,
    keepaliveInterval: 10000,
    keepaliveCountMax: 3,
  });
}

// ─── SOCKS Tunnel (-D) via ssh CLI ──────────────────────────────────
function connectSocks(tunnel, password) {
  connectionStatus = reconnectAttempt > 0 ? 'reconnecting' : 'connecting';
  statusMessage = reconnectAttempt > 0
    ? `Reconnecting SOCKS (attempt ${reconnectAttempt})...`
    : 'Starting SOCKS proxy...';
  broadcastState();
  addSshTunnelLog(`Starting SOCKS tunnel to ${tunnel.username}@${tunnel.host}:${tunnel.sshPort || 22} on local port ${tunnel.socksPort}`);

  const routeResult = ensureTunnelHostRoute(tunnel.host, tunnel.routeInterface);
  if (!routeResult.ok) {
    console.error(`[SOCKS] Route preflight failed for ${tunnel.host}: ${routeResult.error}`);
    addSshTunnelLog(`Route preflight failed for ${tunnel.host}: ${routeResult.error}`);
    connectionStatus = 'error';
    statusMessage = `Route failed: ${routeResult.error}`;
    activeTunnelId = null;
    broadcastState();
    addAudit('tunnel_error', `Route preflight failed: ${routeResult.error}`);
    return;
  }

  // Run SSH in foreground (no -f). Node.js manages the process directly.
  // This avoids the sshpass + -f incompatibility where the forked background
  // process loses access to the password pipe and silently fails.
  const args = [
    '-N',
    '-D', `0.0.0.0:${tunnel.socksPort}`,
    '-o', 'ExitOnForwardFailure=yes',
    '-o', 'ServerAliveInterval=15',
    '-o', 'ServerAliveCountMax=3',
    '-o', 'StrictHostKeyChecking=accept-new',
    '-p', String(tunnel.sshPort || 22),
    `${tunnel.username}@${tunnel.host}`,
  ];
  addSshTunnelLog(`Launching ssh -N -D 0.0.0.0:${tunnel.socksPort} ${tunnel.username}@${tunnel.host}${password ? ' via sshpass' : ''}`);

  const proc = password
    ? spawn('sshpass', ['-p', password, 'ssh', ...args], { stdio: ['ignore', 'pipe', 'pipe'] })
    : spawn('ssh', args, { stdio: ['ignore', 'pipe', 'pipe'] });

  let stderr = '';
  let connected = false;

  proc.stdout.on('data', (d) => {
    const lines = d.toString().split('\n').map(s => s.trim()).filter(Boolean);
    lines.forEach(line => addSshTunnelLog(`[ssh stdout] ${line}`));
  });
  proc.stderr.on('data', (d) => {
    const chunk = d.toString();
    stderr += chunk;
    chunk.split('\n').map(s => s.trim()).filter(Boolean).forEach(line => addSshTunnelLog(`[ssh stderr] ${line}`));
  });

  proc.on('error', (err) => {
    console.error('[SOCKS] Spawn error:', err.message);
    addSshTunnelLog(`SOCKS spawn error: ${err.message}`);
    if (!connected) {
      connectionStatus = 'error';
      statusMessage = `SOCKS error: ${err.message}`;
      activeTunnelId = null;
      broadcastState();
      addAudit('tunnel_error', err.message);
    }
  });

  proc.on('close', (code) => {
    if (!connected) {
      // Process exited before we confirmed the port was listening
      console.error(`[SOCKS] Exit code ${code} before connected: ${stderr.trim()}`);
      addSshTunnelLog(`SOCKS process exited (code ${code}) before port was ready: ${stderr.trim() || 'no output'}`);
      connectionStatus = 'error';
      statusMessage = `SOCKS failed: ${stderr.trim() || 'exit ' + code}`;
      socksTunnelProc = null;
      broadcastState();
      broadcastNotification('tunnel_error', `SOCKS proxy failed: ${stderr.trim() || 'exit ' + code}`);
      addAudit('tunnel_error', `SOCKS exit ${code}: ${stderr.trim()}`);
      scheduleReconnectSocks(tunnel, password);
    } else {
      // Tunnel was running and died unexpectedly
      console.log(`[SOCKS] Tunnel process exited (code ${code})`);
      addSshTunnelLog(`SOCKS process exited (code ${code})${stderr.trim() ? ': ' + stderr.trim() : ''}`);
      socksTunnelProc = null;
      if (shouldReconnect && activeTunnelId) {
        handleSocksDrop(tunnel, password);
      }
    }
  });

  // SSH with -N stays running — poll until the port starts accepting connections
  const startTime = Date.now();
  const pollInterval = setInterval(() => {
    if (proc.exitCode !== null) {
      clearInterval(pollInterval);
      return; // close handler will deal with it
    }
    verifySocksListening(tunnel.socksPort, (listening) => {
      if (!listening) {
        if (Date.now() - startTime > 15000) {
          // Timeout — give up and kill the process
          clearInterval(pollInterval);
          addSshTunnelLog(`SOCKS port ${tunnel.socksPort} never started listening after 15s, killing`);
          proc.kill('SIGTERM');
        }
        return;
      }
      clearInterval(pollInterval);
      if (connected) return;
      connected = true;
      socksTunnelProc = { proc, tunnel };
      connectionStatus = 'connected';
      statusMessage = `SOCKS5 on :${tunnel.socksPort} via ${tunnel.host}`;
      sessionStats.connectedSince = Date.now();
      reconnectAttempt = 0;
      healthFailureStreak = 0;
      broadcastState();
      broadcastNotification('tunnel_connected', `SOCKS proxy connected via ${tunnel.host}:${tunnel.socksPort}`);
      addAudit('tunnel_connected', `SOCKS ${tunnel.name} (${tunnel.host}:${tunnel.socksPort})`);
      console.log(`[SOCKS] Connected: -D 0.0.0.0:${tunnel.socksPort} via ${tunnel.host}`);
      addSshTunnelLog(`SOCKS tunnel connected on local port ${tunnel.socksPort}`);
      startSocksHealthMonitor(tunnel, password);
    });
  }, 1000);
}

function verifySocksListening(port, cb) {
  const sock = net.createConnection({ host: '127.0.0.1', port, timeout: 3000 });
  sock.on('connect', () => { sock.destroy(); cb(true); });
  sock.on('error', () => { cb(false); });
  sock.on('timeout', () => { sock.destroy(); cb(false); });
}

function startSocksHealthMonitor(tunnel, password) {
  stopSocksHealthMonitor();

  socksHealthTimer = setInterval(() => {
    if (!socksTunnelProc || !activeTunnelId || activeTunnelId !== tunnel.id) {
      stopSocksHealthMonitor();
      return;
    }

    // Check 1: process still alive
    if (socksTunnelProc.proc.exitCode !== null) {
      console.log(`[SOCKS] Health check: process exited (code ${socksTunnelProc.proc.exitCode})`);
      addSshTunnelLog(`SOCKS health check: process already exited`);
      handleSocksDrop(tunnel, password);
      return;
    }

    // Check 2: SOCKS port still accepts connections
    const sock = net.createConnection({ host: '127.0.0.1', port: tunnel.socksPort, timeout: 5000 });
    sock.on('connect', () => { sock.destroy(); });
    sock.on('error', () => {
      console.log(`[SOCKS] Health check failed (port ${tunnel.socksPort} not responding)`);
      addSshTunnelLog(`SOCKS health check failed: port ${tunnel.socksPort} not responding`);
      handleSocksDrop(tunnel, password);
    });
    sock.on('timeout', () => {
      sock.destroy();
      console.log(`[SOCKS] Health check failed (port ${tunnel.socksPort} timeout)`);
      addSshTunnelLog(`SOCKS health check failed: port ${tunnel.socksPort} timeout`);
      handleSocksDrop(tunnel, password);
    });
  }, 10000); // Check every 10 seconds
}

function stopSocksHealthMonitor() {
  if (socksHealthTimer) {
    clearInterval(socksHealthTimer);
    socksHealthTimer = null;
  }
}

function handleSocksDrop(tunnel, password) {
  stopSocksHealthMonitor();
  console.log('[SOCKS] Tunnel dropped, cleaning up...');
  addSshTunnelLog(`SOCKS tunnel dropped for ${tunnel.host}, cleaning up`);

  // Clean up dead tunnel
  stopSocksTunnel();
  connectionStatus = 'disconnected';
  statusMessage = 'SOCKS tunnel dropped';
  broadcastState();
  broadcastNotification('tunnel_reconnecting', `SOCKS tunnel lost, reconnecting...`);
  addAudit('tunnel_disconnected', `SOCKS ${tunnel.name} dropped`);

  scheduleReconnectSocks(tunnel, password);
}

function scheduleReconnectSocks(tunnel, password) {
  if (!shouldReconnect || !activeTunnelId) return;

  reconnectAttempt++;
  sessionStats.reconnectCount++;
  const delay = Math.min(1000 * Math.pow(2, reconnectAttempt - 1), 30000);
  connectionStatus = 'reconnecting';
  statusMessage = `Reconnecting SOCKS in ${(delay / 1000).toFixed(0)}s (attempt ${reconnectAttempt})...`;
  broadcastState();
  console.log(`[SOCKS] Reconnecting in ${delay}ms (attempt ${reconnectAttempt})...`);
  addSshTunnelLog(`Scheduling SOCKS reconnect in ${(delay / 1000).toFixed(0)}s (attempt ${reconnectAttempt})`);
  addAudit('tunnel_reconnecting', `SOCKS attempt ${reconnectAttempt}`);

  reconnectTimer = setTimeout(() => {
    if (shouldReconnect && activeTunnelId) {
      const t = tunnels.find(x => x.id === activeTunnelId);
      const p = passwords[activeTunnelId];
      if (t && p) connectSocks(t, p);
    }
  }, delay);
}

function stopSocksTunnel() {
  stopSocksHealthMonitor();
  if (!socksTunnelProc) return;
  addSshTunnelLog('Stopping SOCKS tunnel');
  try { socksTunnelProc.proc.kill('SIGTERM'); } catch (e) {
    console.error('[SOCKS] Stop error:', e.message);
  }
  socksTunnelProc = null;
}

// ─── OpenConnect (dual mode: local or network agent) ────────────────
const OC_BROADCAST_PORT = 7801;
const OC_TUN_DEVICE = 'ocvpn0';

// --- Shared OC state ---
let ocAgent = null;       // { hostname, ip, port, lastSeen }
let ocStatus = 'disconnected';
let ocStatusMessage = 'Idle';
let ocConnectedSince = null;
let ocVpnConfig = null;   // { url, authgroup, username }
let ocTunDevice = null;
let ocRoutedHosts = new Set();

// --- Local mode state ---
let ocLocalProcess = null;
let ocLocalShouldReconnect = false;
let ocLocalReconnectTimer = null;
let ocLocalReconnectAttempt = 0;
let ocLocalLogs = [];
const OC_MAX_LOGS = 300;
let dnsEnforceTimer = null;
let originalResolvConf = null;
let vpnDnsApplied = false;

function normalizeDnsServers(values) {
  const items = Array.isArray(values) ? values : [values];
  const seen = new Set();
  const servers = [];
  for (const item of items) {
    if (item == null) continue;
    const parts = Array.isArray(item)
      ? item
      : String(item)
        .split(/[\n,]+/)
        .map(part => part.trim())
        .filter(Boolean);
    for (const part of parts) {
      if (!net.isIP(part) || seen.has(part)) continue;
      seen.add(part);
      servers.push(part);
    }
  }
  return servers;
}

function normalizeDnsUpdateUrl(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const parsed = new URL(raw);
  if (!['http:', 'https:'].includes(parsed.protocol)) {
    throw new Error('DNS update URL must start with http:// or https://');
  }
  return parsed.toString();
}

function getConfiguredDnsServers() {
  return normalizeDnsServers((appSettings.dnsConfig || {}).servers || []);
}

function buildConfiguredResolvConf() {
  const servers = getConfiguredDnsServers();
  if (!servers.length) return '';
  return servers.map(server => `nameserver ${server}`).join('\n') + '\n';
}

function writeConfiguredDns(reason = '') {
  const desiredRaw = buildConfiguredResolvConf();
  const desired = desiredRaw.trim();
  if (!desired) return false;

  try {
    const currentRaw = fs.existsSync('/etc/resolv.conf')
      ? fs.readFileSync('/etc/resolv.conf', 'utf8')
      : '';
    const current = currentRaw.trim();
    if (current === desired) {
      vpnDnsApplied = true;
      return true;
    }
    if (originalResolvConf == null) originalResolvConf = currentRaw;
    fs.writeFileSync('/etc/resolv.conf', desiredRaw);
    vpnDnsApplied = true;
    console.log(`[DNS] Applied configured resolv.conf${reason ? ` (${reason})` : ''}`);
    return true;
  } catch (e) {
    console.error('[DNS] Failed to apply configured resolv.conf:', e.message);
    return false;
  }
}

function restoreOriginalDns(reason = '') {
  if (!vpnDnsApplied || originalResolvConf == null) return false;
  try {
    fs.writeFileSync('/etc/resolv.conf', originalResolvConf);
    console.log(`[DNS] Restored original resolv.conf${reason ? ` (${reason})` : ''}`);
    originalResolvConf = null;
    vpnDnsApplied = false;
    return true;
  } catch (e) {
    console.error('[DNS] Failed to restore original resolv.conf:', e.message);
    return false;
  }
}

function shouldEnforceConfiguredDns() {
  return getConfiguredDnsServers().length > 0 && (
    !!ocLocalProcess
    || ['connecting', 'connected', 'reconnecting'].includes(ocStatus)
    || !!ovpnProcess
    || ['connecting', 'connected', 'reconnecting'].includes(ovpnStatus)
  );
}

function refreshConfiguredDnsEnforcement(reason = '') {
  if (shouldEnforceConfiguredDns()) {
    writeConfiguredDns(reason);
    if (!dnsEnforceTimer) {
      dnsEnforceTimer = setInterval(() => {
        if (!shouldEnforceConfiguredDns()) {
          clearInterval(dnsEnforceTimer);
          dnsEnforceTimer = null;
          restoreOriginalDns('VPN DNS disabled');
          return;
        }
        writeConfiguredDns('VPN DNS keepalive');
      }, 4000);
    }
    return;
  }

  if (dnsEnforceTimer) {
    clearInterval(dnsEnforceTimer);
    dnsEnforceTimer = null;
  }
  restoreOriginalDns(reason);
}

function addOcLocalLog(line) {
  ocLocalLogs.push({ time: Date.now(), text: line });
  if (ocLocalLogs.length > OC_MAX_LOGS) ocLocalLogs.shift();
  broadcast({ type: 'oc_log', line });
}

function ocLocalStop(reason = '') {
  ocLocalShouldReconnect = false;
  clearTimeout(ocLocalReconnectTimer);
  ocLocalReconnectTimer = null;

  if (ocLocalProcess) {
    try { ocLocalProcess.kill('SIGTERM'); } catch (e) { /* ignore */ }
    ocLocalProcess = null;
  }

  ocStatus = 'disconnected';
  ocStatusMessage = reason || 'Stopped by user';
  ocConnectedSince = null;
  ocTunDevice = null;
  ocLocalReconnectAttempt = 0;
  addOcLocalLog(`[oc-local] Stopped: ${ocStatusMessage}`);
  console.log(`[OC Local] Stopped: ${ocStatusMessage}`);
  broadcastOcState();
  refreshConfiguredDnsEnforcement('OpenConnect stopped');
  refreshVpnRoutes().catch(() => {});
}

function ocLocalStart() {
  if (ocLocalProcess) ocLocalStop('Restarting');
  ocLocalShouldReconnect = true;
  ocLocalReconnectAttempt = 0;
  ocLocalLogs = [];
  refreshConfiguredDnsEnforcement('OpenConnect starting');
  ocLocalConnect();
}

function ocLocalConnect() {
  const cfg = appSettings.ocConfig || {};
  if (!cfg.url || !cfg.username || !cfg.password) {
    ocStatus = 'error';
    ocStatusMessage = 'VPN config incomplete — set URL, username, and password in Settings';
    addOcLocalLog(`[oc-local] ${ocStatusMessage}`);
    broadcastOcState();
    return;
  }

  ocStatus = ocLocalReconnectAttempt > 0 ? 'reconnecting' : 'connecting';
  ocStatusMessage = ocLocalReconnectAttempt > 0
    ? `Reconnect attempt ${ocLocalReconnectAttempt}...`
    : 'Connecting...';
  ocVpnConfig = { url: cfg.url, authgroup: cfg.authgroup, username: cfg.username };
  refreshConfiguredDnsEnforcement('OpenConnect connecting');

  const args = [
    '--protocol=anyconnect',
    `--interface=${OC_TUN_DEVICE}`,
    `--user=${cfg.username}`,
    ...(cfg.authgroup ? [`--authgroup=${cfg.authgroup}`] : []),
    '--passwd-on-stdin',
    cfg.url,
  ];

  console.log(`[OC Local] Spawning: openconnect ${args.join(' ')}`);
  addOcLocalLog(`[oc-local] Connecting to ${cfg.url}${cfg.authgroup ? ` (group: ${cfg.authgroup})` : ''}...`);
  broadcastOcState();

  let proc;
  try {
    proc = spawn('openconnect', args, { stdio: ['pipe', 'pipe', 'pipe'] });
  } catch (err) {
    ocStatus = 'error';
    ocStatusMessage = `Failed to spawn openconnect: ${err.message}`;
    addOcLocalLog(`[oc-local] ${ocStatusMessage}`);
    broadcastOcState();
    return;
  }

  ocLocalProcess = proc;
  proc.stdin.write(cfg.password + '\n');
  proc.stdin.end();

  const handleOutput = (data) => {
    const lines = data.toString().split('\n').filter(l => l.trim());
    for (const line of lines) {
      console.log(`[OC Local] ${line}`);
      addOcLocalLog(line);

      if (line.includes('Connected as') || line.includes('ESP session established') || line.includes('DTLS connected')) {
        ocStatus = 'connected';
        ocStatusMessage = line;
        ocTunDevice = OC_TUN_DEVICE;
        if (!ocConnectedSince) ocConnectedSince = Date.now();
        ocLocalReconnectAttempt = 0;
        broadcastOcState();
        refreshConfiguredDnsEnforcement('OpenConnect connected');
        refreshVpnRoutes().catch(() => {});
      }
    }
  };

  proc.stdout.on('data', handleOutput);
  proc.stderr.on('data', handleOutput);

  proc.on('error', (err) => {
    console.error(`[OC Local] Process error:`, err.message);
    addOcLocalLog(`[oc-local] Error: ${err.message}`);
    ocStatus = 'error';
    ocStatusMessage = err.message;
    ocTunDevice = null;
    broadcastOcState();
    refreshConfiguredDnsEnforcement('OpenConnect error');
    refreshVpnRoutes().catch(() => {});
  });

  proc.on('close', (code) => {
    console.log(`[OC Local] Exited with code ${code}`);
    addOcLocalLog(`[oc-local] Process exited with code ${code}`);
    ocLocalProcess = null;
    ocConnectedSince = null;
    ocTunDevice = null;

    if (ocLocalShouldReconnect) {
      ocLocalReconnectAttempt++;
      const delay = Math.min(1000 * Math.pow(2, ocLocalReconnectAttempt - 1), 30000);
      ocStatus = 'reconnecting';
      ocStatusMessage = `Reconnecting in ${(delay / 1000).toFixed(0)}s (attempt ${ocLocalReconnectAttempt})...`;
      addOcLocalLog(`[oc-local] ${ocStatusMessage}`);
      console.log(`[OC Local] ${ocStatusMessage}`);
      broadcastOcState();
      ocLocalReconnectTimer = setTimeout(() => {
        if (ocLocalShouldReconnect) ocLocalConnect();
      }, delay);
    } else {
      ocStatus = 'disconnected';
      ocStatusMessage = `Exited with code ${code}`;
      broadcastOcState();
    }
    refreshConfiguredDnsEnforcement('OpenConnect exited');
    refreshVpnRoutes().catch(() => {});
  });
}

// --- Network mode: UDP discovery ---
let udpListener = null;
let udpCheckInterval = null;

function startNetworkDiscovery() {
  if (udpListener) return; // already running
  ocStatus = 'searching';
  ocStatusMessage = 'Searching for agent on LAN...';
  ocAgent = null;

  udpListener = dgram.createSocket({ type: 'udp4', reuseAddr: true });

  udpListener.on('message', (msg, rinfo) => {
    try {
      const data = JSON.parse(msg.toString());
      if (data.type !== 'oc-agent') return;

      const wasFound = !!ocAgent;
      ocAgent = {
        hostname: data.hostname,
        ip: rinfo.address,
        port: data.port,
        lastSeen: Date.now(),
      };
      ocVpnConfig = data.vpnConfig || { url: 'ra.albb.ir', authgroup: data.authgroup || 'HQ', username: data.username };
      ocStatus = data.status || 'disconnected';
      ocStatusMessage = data.statusMessage || '';
      ocConnectedSince = data.connectedSince || null;

      if (!wasFound) {
        console.log(`[OC Discovery] Found agent: ${data.hostname} at ${rinfo.address}:${data.port}`);
      }
      broadcastOcState();
    } catch (e) { /* ignore malformed */ }
  });

  udpListener.on('error', (err) => {
    console.error('[OC Discovery] UDP error:', err.message);
  });

  udpListener.bind(OC_BROADCAST_PORT, () => {
    console.log(`[OC Discovery] Listening for agent broadcasts on UDP port ${OC_BROADCAST_PORT}`);
  });

  udpCheckInterval = setInterval(() => {
    if (ocAgent && Date.now() - ocAgent.lastSeen > 10000) {
      console.log('[OC Discovery] Agent lost (no broadcast for 10s)');
      ocAgent = null;
      ocStatus = 'searching';
      ocStatusMessage = 'Agent lost, searching...';
      ocConnectedSince = null;
      broadcastOcState();
    }
  }, 3000);
}

function stopNetworkDiscovery() {
  if (udpCheckInterval) { clearInterval(udpCheckInterval); udpCheckInterval = null; }
  if (udpListener) {
    try { udpListener.close(); } catch (e) { /* ignore */ }
    udpListener = null;
  }
  ocAgent = null;
}

// Helper: make HTTP request to remote agent
function agentRequest(agentPath, method = 'GET') {
  return new Promise((resolve, reject) => {
    if (!ocAgent) return reject(new Error('Agent not discovered'));

    const options = {
      hostname: ocAgent.ip,
      port: ocAgent.port,
      path: agentPath,
      method,
      timeout: 5000,
    };

    const req = http.request(options, (res) => {
      let body = '';
      res.on('data', (chunk) => { body += chunk; });
      res.on('end', () => {
        try { resolve(JSON.parse(body)); }
        catch (e) { resolve(body); }
      });
    });

    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    req.end();
  });
}

function broadcastOcState() {
  const isLocal = appSettings.ocMode === 'local';
  broadcast({
    type: 'oc_state',
    mode: appSettings.ocMode || 'network',
    agent: !isLocal && ocAgent ? { hostname: ocAgent.hostname, ip: ocAgent.ip, port: ocAgent.port } : null,
    status: isLocal ? ocStatus : (ocAgent ? ocStatus : 'searching'),
    statusMessage: isLocal ? ocStatusMessage : (ocAgent ? ocStatusMessage : 'Searching for agent on LAN...'),
    connectedSince: ocConnectedSince,
    uptime: ocConnectedSince ? Date.now() - ocConnectedSince : 0,
    tunDevice: isLocal ? ocTunDevice : null,
    routedHosts: [...ocRoutedHosts],
    vpnConfig: ocVpnConfig,
    upstreamRouting: buildUpstreamRoutingState(),
  });
}

// Initialize OC mode on startup
function initOcMode() {
  const mode = appSettings.ocMode || 'network';
  if (mode === 'network') {
    startNetworkDiscovery();
  } else {
    ocStatus = 'disconnected';
    ocStatusMessage = 'Idle';
    ocVpnConfig = appSettings.ocConfig ? {
      url: appSettings.ocConfig.url,
      authgroup: appSettings.ocConfig.authgroup,
      username: appSettings.ocConfig.username,
    } : null;
  }
}
initOcMode();

// Periodic state broadcast to WebSocket clients
setInterval(() => {
  broadcastOcState();
}, 2000);

// ─── Local OpenVPN with Split Tunneling ──────────────────────────────
const PROFILES_DIR = path.join(__dirname, 'profiles');
if (!fs.existsSync(PROFILES_DIR)) fs.mkdirSync(PROFILES_DIR);
const DEFAULT_OVPN_DATA_CIPHERS = ['AES-256-GCM', 'AES-128-GCM', 'CHACHA20-POLY1305'];
const OVPN_PROFILE_ROUTE_DIRECTIVES = new Set([
  'redirect-gateway',
  'redirect-private',
  'route',
  'route-ipv6',
  'route-gateway',
  'route-metric',
]);

const ovpnUpload = multer({ dest: PROFILES_DIR, limits: { fileSize: 1024 * 1024 } }); // 1MB max

let ovpnProcess = null;
let ovpnStatus = 'disconnected'; // connecting | connected | disconnected | error
let ovpnStatusMessage = 'Idle';
let ovpnConnectedSince = null;
let ovpnTunDevice = null;     // e.g. 'utun5' or 'tun0'
let ovpnGateway = null;       // VPN gateway IP
let ovpnLogs = [];
let ovpnRoutedHosts = new Set(); // SSH tunnel hosts currently routed through VPN
const MAX_OVPN_LOGS = 300;
let ovpnAutoReconnect = false;
let ovpnReconnectTimer = null;
let ovpnReconnectAttempt = 0;
let ovpnLastProfile = null;
let ovpnLastUsername = null;
let ovpnLastPassword = null;

function addOvpnLog(line) {
  ovpnLogs.push({ time: Date.now(), text: line });
  if (ovpnLogs.length > MAX_OVPN_LOGS) ovpnLogs.shift();
}

function getOvpnProfiles() {
  try {
    return fs.readdirSync(PROFILES_DIR)
      .filter(f => f.endsWith('.ovpn') || f.endsWith('.conf'))
      .map(f => ({ name: f, path: path.join(PROFILES_DIR, f) }));
  } catch { return []; }
}

function parseOvpnProfileMetadata(profilePath) {
  const meta = {
    cipher: null,
    dataCiphers: [],
    dataCiphersFallback: null,
    routingDirectives: [],
  };

  try {
    const content = fs.readFileSync(profilePath, 'utf8');
    for (const rawLine of content.split(/\r?\n/)) {
      const line = rawLine.trim();
      if (!line || line.startsWith('#') || line.startsWith(';')) continue;

      const parts = line.split(/\s+/);
      const directive = (parts.shift() || '').toLowerCase();
      const value = parts.join(' ').trim();
      if (!value) continue;

      if (directive === 'cipher' && !meta.cipher) {
        meta.cipher = value;
      } else if ((directive === 'data-ciphers' || directive === 'ncp-ciphers') && !meta.dataCiphers.length) {
        meta.dataCiphers = value.split(':').map(item => item.trim()).filter(Boolean);
      } else if (directive === 'data-ciphers-fallback' && !meta.dataCiphersFallback) {
        meta.dataCiphersFallback = value;
      } else if (OVPN_PROFILE_ROUTE_DIRECTIVES.has(directive)) {
        meta.routingDirectives.push(directive);
      }
    }
  } catch {
    // Ignore profile parsing issues; OpenVPN itself will report syntax problems.
  }

  return meta;
}

function mergeCipherSuites(...groups) {
  const merged = [];
  for (const group of groups) {
    for (const item of group || []) {
      const value = String(item || '').trim();
      if (value && !merged.includes(value)) merged.push(value);
    }
  }
  return merged;
}

function getPreferredUpstreamVpnMode() {
  return ['openvpn', 'openconnect', 'both'].includes(appSettings.upstreamVpnMode)
    ? appSettings.upstreamVpnMode
    : 'openvpn';
}

function isOpenVpnReadyForUpstreamRouting() {
  return ovpnStatus === 'connected' && !!ovpnTunDevice;
}

function isOpenConnectReadyForUpstreamRouting() {
  return appSettings.ocMode === 'local' && ocStatus === 'connected';
}

function getEffectiveUpstreamVpnMode() {
  const preferred = getPreferredUpstreamVpnMode();
  const openvpnReady = isOpenVpnReadyForUpstreamRouting();
  const openconnectReady = isOpenConnectReadyForUpstreamRouting();

  if (preferred === 'openvpn') {
    if (openvpnReady) return 'openvpn';
    if (openconnectReady) return 'openconnect';
    return 'direct';
  }

  if (preferred === 'openconnect') {
    if (openconnectReady) return 'openconnect';
    if (openvpnReady) return 'openvpn';
    return 'direct';
  }

  if (openvpnReady && openconnectReady) return 'both';
  if (openvpnReady) return 'openvpn';
  if (openconnectReady) return 'openconnect';
  return 'direct';
}

function buildUpstreamRoutingState() {
  return {
    preferred: getPreferredUpstreamVpnMode(),
    effective: getEffectiveUpstreamVpnMode(),
    openvpnReady: isOpenVpnReadyForUpstreamRouting(),
    openconnectReady: isOpenConnectReadyForUpstreamRouting(),
    openconnectMode: appSettings.ocMode || 'network',
  };
}

function shouldPinRouteViaOpenVpn(host) {
  const effective = getEffectiveUpstreamVpnMode();
  if (effective === 'openvpn') return true;
  if (effective !== 'both') return false;
  const digest = crypto.createHash('sha1').update(String(host)).digest();
  return (digest[0] & 1) === 0;
}

function buildOvpnStatePayload() {
  return {
    type: 'ovpn_state',
    status: ovpnStatus,
    statusMessage: ovpnStatusMessage,
    connectedSince: ovpnConnectedSince,
    uptime: ovpnConnectedSince ? Date.now() - ovpnConnectedSince : 0,
    tunDevice: ovpnTunDevice,
    routedHosts: [...ovpnRoutedHosts],
    profiles: getOvpnProfiles().map(p => p.name),
    upstreamRouting: buildUpstreamRoutingState(),
  };
}

function broadcastOvpnState() {
  broadcast(buildOvpnStatePayload());
}

function addVpnRoute(host) {
  if (!ovpnTunDevice || !host || ovpnRoutedHosts.has(host)) return;
  try {
    const isMac = process.platform === 'darwin';
    if (isMac) {
      // macOS: route add -host <ip> -interface <tun>
      if (ovpnGateway) {
        execSync(`route add -host ${host} ${ovpnGateway}`, { stdio: 'ignore' });
      } else {
        execSync(`route add -host ${host} -interface ${ovpnTunDevice}`, { stdio: 'ignore' });
      }
    } else {
      // Linux: ip route add <ip>/32 dev <tun>
      if (ovpnGateway) {
        execSync(`ip route add ${host}/32 via ${ovpnGateway} dev ${ovpnTunDevice}`, { stdio: 'ignore' });
      } else {
        execSync(`ip route add ${host}/32 dev ${ovpnTunDevice}`, { stdio: 'ignore' });
      }
    }
    ovpnRoutedHosts.add(host);
    addOvpnLog(`[route] Added route for ${host} via ${ovpnTunDevice}`);
    console.log(`[OpenVPN] Route added: ${host} -> ${ovpnTunDevice}`);
  } catch (e) {
    addOvpnLog(`[route] Failed to add route for ${host}: ${e.message}`);
    console.error(`[OpenVPN] Route add failed for ${host}:`, e.message);
  }
}

function removeVpnRoute(host) {
  if (!host || !ovpnRoutedHosts.has(host)) return;
  try {
    const isMac = process.platform === 'darwin';
    if (isMac) {
      execSync(`route delete -host ${host}`, { stdio: 'ignore' });
    } else {
      execSync(`ip route del ${host}/32`, { stdio: 'ignore' });
    }
    ovpnRoutedHosts.delete(host);
    addOvpnLog(`[route] Removed route for ${host}`);
  } catch (e) {
    ovpnRoutedHosts.delete(host);
  }
}

function removeAllVpnRoutes() {
  for (const host of [...ovpnRoutedHosts]) {
    removeVpnRoute(host);
  }
}

// Add routes for all current SSH tunnel hosts
function addRoutesForTunnels() {
  if (ovpnStatus !== 'connected' || !ovpnTunDevice) return;
  for (const t of tunnels) {
    if (t.host && net.isIP(t.host)) {
      addVpnRoute(t.host);
    }
  }
}

function isLocalRouteHost(host) {
  return !host || ['127.0.0.1', '0.0.0.0', '::1', 'localhost'].includes(String(host).trim().toLowerCase());
}

function extractOutboundRouteHosts(outbound) {
  const hosts = new Set();
  if (!outbound || typeof outbound !== 'object') return [];

  const addHost = (host) => {
    if (!host || isLocalRouteHost(host)) return;
    hosts.add(String(host).trim());
  };

  const settings = outbound.settings || {};
  if (Array.isArray(settings.vnext)) {
    settings.vnext.forEach(server => addHost(server && server.address));
  }
  if (Array.isArray(settings.servers)) {
    settings.servers.forEach(server => addHost(server && server.address));
  }

  return [...hosts];
}

function getLocalXrayOutboundRouteHosts() {
  const outbound = localXraySettings && localXraySettings.outbound;
  if (!outbound || typeof outbound !== 'string' || !outbound.startsWith('socks:')) return [];
  const parts = outbound.split(':');
  const host = parts[1];
  return isLocalRouteHost(host) ? [] : [host];
}

async function resolveVpnRouteIps(hosts) {
  const ips = new Set();

  for (const host of hosts || []) {
    if (!host) continue;
    const ipVersion = net.isIP(host);
    if (ipVersion === 4) {
      ips.add(host);
      continue;
    }
    if (ipVersion === 6) {
      addOvpnLog(`[route] Skipping IPv6 route target ${host} (only IPv4 route pinning is supported)`);
      continue;
    }

    try {
      const resolved = await dns.promises.lookup(host, { all: true });
      const v4 = resolved.filter(entry => entry.family === 4);
      if (!v4.length) {
        addOvpnLog(`[route] No IPv4 address found for ${host}`);
        continue;
      }
      v4.forEach(entry => ips.add(entry.address));
    } catch (e) {
      addOvpnLog(`[route] DNS lookup failed for ${host}: ${e.message}`);
    }
  }

  return [...ips];
}

async function ensureVpnRoutesForHosts(hosts, label = 'Route sync') {
  if (ovpnStatus !== 'connected' || !ovpnTunDevice) return [];
  const ips = await resolveVpnRouteIps(hosts);
  const pinnedIps = ips.filter(ip => shouldPinRouteViaOpenVpn(ip));
  pinnedIps.forEach(ip => addVpnRoute(ip));
  if (ips.length > 0) {
    if (pinnedIps.length > 0) {
      addOvpnLog(`[route] ${label}: pinned via OpenVPN -> ${pinnedIps.join(', ')}`);
    } else {
      addOvpnLog(`[route] ${label}: OpenVPN pinning not required (effective mode: ${getEffectiveUpstreamVpnMode()})`);
    }
    broadcastOvpnState();
  }
  return pinnedIps;
}

async function collectDesiredVpnRouteIps() {
  // Client traffic should now flow either:
  // 1. directly over local OpenConnect, or
  // 2. through an SSH SOCKS tunnel created on top of OpenVPN.
  // The panel no longer installs host routes for Xray/V2Ray client outbounds.
  return new Set();
}

async function refreshVpnRoutes() {
  const desired = ovpnStatus === 'connected' && ovpnTunDevice
    ? await collectDesiredVpnRouteIps()
    : new Set();

  for (const host of [...ovpnRoutedHosts]) {
    if (!desired.has(host)) removeVpnRoute(host);
  }
  for (const host of desired) {
    if (!ovpnRoutedHosts.has(host)) addVpnRoute(host);
  }

  broadcastOvpnState();
  await syncAllXrayConfigRoutes('Tunnel state change');
}

function stopOvpn(reason = '', userInitiated = false) {
  if (userInitiated) {
    ovpnAutoReconnect = false;
    clearTimeout(ovpnReconnectTimer);
    ovpnReconnectTimer = null;
  }
  if (ovpnProcess) {
    try { ovpnProcess.kill('SIGTERM'); } catch (e) { /* ignore */ }
    ovpnProcess = null;
  }
  removeAllVpnRoutes();
  ovpnStatus = 'disconnected';
  ovpnStatusMessage = reason || 'Stopped';
  ovpnConnectedSince = null;
  ovpnTunDevice = null;
  ovpnGateway = null;
  addOvpnLog(`[agent] Stopped: ${ovpnStatusMessage}`);
  console.log(`[OpenVPN] Stopped: ${ovpnStatusMessage}`);
  addAudit('vpn_disconnected', reason || 'Stopped');
  broadcastOvpnState();
  refreshConfiguredDnsEnforcement('OpenVPN stopped');
  refreshVpnRoutes().catch(() => {});
}

function startOvpn(profilePath, username, password) {
  if (ovpnProcess) stopOvpn('Restarting');
  ovpnLastProfile = profilePath;
  ovpnLastUsername = username;
  ovpnLastPassword = password;
  ovpnAutoReconnect = true;
  ovpnReconnectAttempt = 0;
  clearTimeout(ovpnReconnectTimer);

  if (!fs.existsSync(profilePath)) {
    ovpnStatus = 'error';
    ovpnStatusMessage = `Profile not found: ${profilePath}`;
    broadcastOvpnState();
    return;
  }

  ovpnStatus = 'connecting';
  ovpnStatusMessage = 'Connecting...';
  ovpnLogs = [];
  ovpnConnectedSince = null;
  ovpnTunDevice = null;
  ovpnGateway = null;
  refreshConfiguredDnsEnforcement('OpenVPN starting');
  broadcastOvpnState();

  const profileMeta = parseOvpnProfileMetadata(profilePath);
  const mergedDataCiphers = mergeCipherSuites(
    profileMeta.dataCiphers,
    DEFAULT_OVPN_DATA_CIPHERS,
    profileMeta.cipher ? [profileMeta.cipher] : []
  );

  const args = [
    '--config', profilePath,
    '--route-nopull',       // Don't accept pushed server routes
    '--route-noexec',       // Don't install profile-defined routes either
    '--pull-filter', 'ignore', 'redirect-gateway',
    '--pull-filter', 'ignore', 'redirect-private',
    '--pull-filter', 'ignore', 'dhcp-option',
    '--verb', '3',
    '--auth-nocache',
  ];

  if (mergedDataCiphers.length > 0) {
    args.push('--data-ciphers', mergedDataCiphers.join(':'));
  }
  if (profileMeta.cipher && !profileMeta.dataCiphersFallback) {
    args.push('--data-ciphers-fallback', profileMeta.cipher);
  }

  // Write auth file if credentials provided
  let authFile = null;
  if (username && password) {
    authFile = path.join(PROFILES_DIR, '.auth-tmp');
    fs.writeFileSync(authFile, `${username}\n${password}\n`, { mode: 0o600 });
    args.push('--auth-user-pass', authFile);
  }

  addOvpnLog(`[agent] Starting OpenVPN with profile: ${path.basename(profilePath)}`);
  if (profileMeta.cipher) {
    addOvpnLog(`[agent] OpenVPN 2.6 compatibility: merged cipher ${profileMeta.cipher} into data-ciphers${profileMeta.dataCiphersFallback ? '' : ` and fallback ${profileMeta.cipher}`}`);
  }
  if (profileMeta.routingDirectives.length > 0) {
    addOvpnLog(`[agent] Split tunnel enforcement: ignoring profile route directives (${[...new Set(profileMeta.routingDirectives)].join(', ')})`);
  }
  console.log(`[OpenVPN] Spawning: openvpn ${args.map(a => a === authFile ? '<auth-file>' : a).join(' ')}`);

  const ovpnBin = process.platform === 'darwin'
    ? '/opt/homebrew/opt/openvpn/sbin/openvpn'
    : 'openvpn';
  const proc = spawn(ovpnBin, args, { stdio: ['pipe', 'pipe', 'pipe'] });
  ovpnProcess = proc;

  const handleOutput = (source) => (data) => {
    const lines = data.toString().split('\n').filter(l => l.trim());
    for (const line of lines) {
      console.log(`[OVPN ${source}] ${line}`);
      addOvpnLog(line);

      // Detect tun device — macOS: "Opened utun5", Linux: "TUN/TAP device tun0 opened"
      const tunMatch = line.match(/(?:Opened|device)\s+((?:utun|tun|tap)\d+)/i);
      if (tunMatch) {
        ovpnTunDevice = tunMatch[1];
        addOvpnLog(`[agent] TUN device: ${ovpnTunDevice}`);
      }

      // Detect gateway — "route remote_host ... net_gateway ..."
      // or "peer info: IV_PLAT=..." won't help, look for route/ifconfig lines
      const gwMatch = line.match(/route.*via\s+(\d+\.\d+\.\d+\.\d+)/);
      if (gwMatch) ovpnGateway = gwMatch[1];

      // Also detect gateway from ifconfig push: "option 3 ifconfig <local> <remote/gw>"
      const ifcfgMatch = line.match(/ifconfig\s+(\d+\.\d+\.\d+\.\d+)\s+(\d+\.\d+\.\d+\.\d+)/);
      if (ifcfgMatch) {
        ovpnGateway = ifcfgMatch[2]; // peer/gateway IP
      }

      // Detect connected state
      if (line.includes('Initialization Sequence Completed')) {
        ovpnStatus = 'connected';
        ovpnStatusMessage = 'Connected';
        ovpnConnectedSince = Date.now();
        ovpnReconnectAttempt = 0;
        refreshConfiguredDnsEnforcement('OpenVPN connected');
        addOvpnLog('[agent] Automatic OpenVPN routes are disabled; Xray config routes will be added only when selected in the Xray tab');
        addOvpnLog(`[agent] Connected via ${ovpnTunDevice || 'unknown'}, gateway ${ovpnGateway || 'unknown'}`);
        setTimeout(() => {
          refreshVpnRoutes().catch(err => {
            addOvpnLog(`[route] Failed to refresh routes: ${err.message}`);
          });
        }, 500);
        broadcastOvpnState();
        broadcastNotification('vpn_connected', 'OpenVPN connected');
        addAudit('vpn_connected', `via ${ovpnTunDevice || 'unknown'}`);
      }
    }
    broadcastOvpnState();
  };

  proc.stdout.on('data', handleOutput('stdout'));
  proc.stderr.on('data', handleOutput('stderr'));

  proc.on('error', (err) => {
    console.error('[OpenVPN] Process error:', err.message);
    addOvpnLog(`[agent] Error: ${err.message}`);
    ovpnStatus = 'error';
    ovpnStatusMessage = err.message;
    broadcastOvpnState();
    refreshConfiguredDnsEnforcement('OpenVPN error');
  });

  proc.on('close', (code) => {
    console.log(`[OpenVPN] Exited with code ${code}`);
    addOvpnLog(`[agent] Process exited with code ${code}`);
    ovpnProcess = null;
    removeAllVpnRoutes();
    ovpnConnectedSince = null;
    ovpnTunDevice = null;
    ovpnGateway = null;
    // Clean up auth file
    if (authFile && fs.existsSync(authFile)) {
      try { fs.unlinkSync(authFile); } catch {}
    }
    // Auto-reconnect on unexpected close
    if (ovpnAutoReconnect && ovpnLastProfile) {
      ovpnReconnectAttempt++;
      const delay = Math.min(1000 * Math.pow(2, ovpnReconnectAttempt - 1), 30000);
      ovpnStatus = 'reconnecting';
      ovpnStatusMessage = `Reconnecting in ${Math.round(delay / 1000)}s (attempt ${ovpnReconnectAttempt})...`;
      broadcastOvpnState();
      broadcastNotification('vpn_reconnecting', ovpnStatusMessage);
      ovpnReconnectTimer = setTimeout(() => {
        if (ovpnAutoReconnect && ovpnLastProfile) {
          startOvpn(ovpnLastProfile, ovpnLastUsername, ovpnLastPassword);
        }
      }, delay);
    } else {
      ovpnStatus = 'disconnected';
      ovpnStatusMessage = `Exited with code ${code}`;
      broadcastOvpnState();
    }
    refreshConfiguredDnsEnforcement('OpenVPN exited');
  });
}

// Periodic OpenVPN state broadcast
setInterval(() => {
  if (ovpnStatus !== 'disconnected') broadcastOvpnState();
}, 2000);

// ─── Health Checks ──────────────────────────────────────────────────
function runHealthCheck() {
  if (!activeTunnelId || connectionStatus !== 'connected') {
    if (healthStatus.lastCheck) healthStatus = { latency: null, lastCheck: null, healthy: true, history: [] };
    healthFailureStreak = 0;
    return;
  }
  const tunnel = tunnels.find(t => t.id === activeTunnelId);
  if (!tunnel) return;
  const start = Date.now();
  const sock = net.createConnection({ host: tunnel.host, port: tunnel.sshPort || 22, timeout: 5000 });
  sock.on('connect', () => {
    const latency = Date.now() - start;
    sock.destroy();
    healthStatus.latency = latency;
    healthStatus.lastCheck = Date.now();
    healthStatus.healthy = true;
    healthStatus.history.push({ time: Date.now(), latency });
    if (healthStatus.history.length > MAX_HEALTH_HISTORY) healthStatus.history.shift();
    healthFailureStreak = 0;
  });
  sock.on('error', () => {
    healthStatus.latency = null;
    healthStatus.lastCheck = Date.now();
    healthStatus.healthy = false;
    healthStatus.history.push({ time: Date.now(), latency: null });
    if (healthStatus.history.length > MAX_HEALTH_HISTORY) healthStatus.history.shift();
    broadcastNotification('health_warning', `Health check failed for ${tunnel.host}`);
    maybeReconnectTunnelFromHealthFailure(tunnel, 'connect error');
  });
  sock.on('timeout', () => {
    sock.destroy();
    healthStatus.latency = null;
    healthStatus.lastCheck = Date.now();
    healthStatus.healthy = false;
    healthStatus.history.push({ time: Date.now(), latency: null });
    if (healthStatus.history.length > MAX_HEALTH_HISTORY) healthStatus.history.shift();
    broadcastNotification('health_warning', `Health check timeout for ${tunnel.host}`);
    maybeReconnectTunnelFromHealthFailure(tunnel, 'timeout');
  });
}

setInterval(runHealthCheck, 30000);

// ─── REST API ───────────────────────────────────────────────────────

app.get('/api/tunnels', (req, res) => {
  const result = tunnels.map(t => ({
    ...t,
    hasPassword: !!passwords[t.id],
    isActive: activeTunnelId === t.id,
  }));
  res.json(result);
});

app.get('/api/tunnels/interfaces', (req, res) => {
  const ifaces = [];
  try {
    if (process.platform === 'linux') {
      const output = execSync('ip -o link show up', { encoding: 'utf8', timeout: 3000 }).trim();
      for (const line of output.split('\n')) {
        const match = line.match(/^\d+:\s+(\S+?)(?:@\S+)?:/);
        if (match && match[1] !== 'lo') {
          ifaces.push(match[1]);
        }
      }
    } else {
      // macOS / other — use os.networkInterfaces
      const osIfaces = require('os').networkInterfaces();
      for (const name of Object.keys(osIfaces)) {
        if (name === 'lo' || name === 'lo0') continue;
        ifaces.push(name);
      }
    }
  } catch (e) {
    console.error('[Interfaces] Error:', e.message);
  }
  // Always include 'direct' option
  const defaultInterface = ovpnTunDevice && ifaces.includes(ovpnTunDevice)
    ? ovpnTunDevice
    : (ifaces.includes('tun0') ? 'tun0' : 'direct');
  res.json({ interfaces: ifaces, default: defaultInterface });
});

app.get('/api/tunnel/logs', (req, res) => {
  res.json(sshTunnelLogs);
});

app.post('/api/tunnel/logs/clear', (req, res) => {
  sshTunnelLogs = [];
  res.json({ ok: true });
});

app.post('/api/tunnels', (req, res) => {
  const { type, name, host, sshPort, username, remotePort, localPort, localHost, socksPort, routeInterface } = req.body;
  if (!name || !host || !username) {
    return res.status(400).json({ error: 'Missing required fields' });
  }
  if (type === 'socks' && !socksPort) {
    return res.status(400).json({ error: 'SOCKS port is required' });
  }
  if (type !== 'socks' && (!remotePort || !localPort)) {
    return res.status(400).json({ error: 'Remote and local ports are required' });
  }
  const tunnel = {
    id: genId(),
    type: type || 'reverse',
    name,
    host,
    sshPort: parseInt(sshPort) || 22,
    username,
    remotePort: remotePort ? parseInt(remotePort) : undefined,
    localPort: localPort ? parseInt(localPort) : undefined,
    localHost: localHost || 'localhost',
    socksPort: socksPort ? parseInt(socksPort) : undefined,
    routeInterface: routeInterface || 'tun0',
    createdAt: Date.now(),
  };
  tunnels.push(tunnel);
  saveTunnels();
  res.json(tunnel);
});

app.put('/api/tunnels/:id', (req, res) => {
  const idx = tunnels.findIndex(t => t.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Tunnel not found' });
  if (activeTunnelId === req.params.id) {
    return res.status(400).json({ error: 'Cannot edit active tunnel' });
  }
  const { type, name, host, sshPort, username, remotePort, localPort, localHost, socksPort, routeInterface } = req.body;
  tunnels[idx] = {
    ...tunnels[idx],
    type: type || tunnels[idx].type || 'reverse',
    name, host,
    sshPort: parseInt(sshPort) || 22,
    username,
    remotePort: remotePort ? parseInt(remotePort) : tunnels[idx].remotePort,
    localPort: localPort ? parseInt(localPort) : tunnels[idx].localPort,
    localHost: localHost || 'localhost',
    socksPort: socksPort ? parseInt(socksPort) : tunnels[idx].socksPort,
    routeInterface: routeInterface || tunnels[idx].routeInterface || 'tun0',
  };
  saveTunnels();
  res.json(tunnels[idx]);
});

app.delete('/api/tunnels/:id', (req, res) => {
  if (activeTunnelId === req.params.id) stopTunnel('Tunnel deleted');
  tunnels = tunnels.filter(t => t.id !== req.params.id);
  delete passwords[req.params.id];
  saveTunnels();
  res.json({ ok: true });
});

app.post('/api/tunnels/:id/start', (req, res) => {
  try {
    const { password } = req.body;
    if (password) passwords[req.params.id] = password;
    startTunnel(req.params.id);
    res.json({ ok: true });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.post('/api/tunnels/:id/stop', (req, res) => {
  if (activeTunnelId !== req.params.id) {
    return res.status(400).json({ error: 'This tunnel is not active' });
  }
  stopTunnel();
  res.json({ ok: true });
});

app.get('/api/status', (req, res) => {
  res.json({
    activeTunnelId,
    status: connectionStatus,
    statusMessage,
    stats: {
      ...sessionStats,
      uptime: sessionStats.connectedSince ? Date.now() - sessionStats.connectedSince : 0,
    },
  });
});

// ─── Connections API ────────────────────────────────────────────────
app.get('/api/connections', (req, res) => {
  const conns = [];
  for (const [id, c] of localConnections) {
    conns.push({
      id,
      srcIP: c.srcIP,
      srcPort: c.srcPort,
      destPort: c.destPort,
      bytesIn: c.bytesIn,
      bytesOut: c.bytesOut,
      connectedAt: c.connectedAt,
      duration: Date.now() - c.connectedAt,
    });
  }
  res.json(conns);
});

app.post('/api/connections/:id/drop', (req, res) => {
  const id = parseInt(req.params.id);
  const conn = localConnections.get(id);
  if (!conn) return res.status(404).json({ error: 'Connection not found' });
  conn.stream.destroy();
  conn.socket.destroy();
  localConnections.delete(id);
  sessionStats.activeConnections = Math.max(0, sessionStats.activeConnections - 1);
  res.json({ ok: true });
});

// Drop all connections from an IP
app.post('/api/connections/drop-ip/:ip', (req, res) => {
  const ip = req.params.ip;
  let dropped = 0;
  for (const [id, c] of localConnections) {
    if (c.srcIP === ip) {
      c.stream.destroy();
      c.socket.destroy();
      localConnections.delete(id);
      sessionStats.activeConnections = Math.max(0, sessionStats.activeConnections - 1);
      dropped++;
    }
  }
  res.json({ ok: true, dropped });
});

// Set speed limit for an IP (bytes/sec, 0 = unlimited)
app.post('/api/connections/limit/:ip', (req, res) => {
  const ip = req.params.ip;
  const { download, upload } = req.body; // bytes per second
  if (download === 0 && upload === 0) {
    delete ipSpeedLimits[ip];
    delete ipThrottles[ip];
  } else {
    ipSpeedLimits[ip] = {
      download: Math.max(0, parseInt(download) || 0),
      upload: Math.max(0, parseInt(upload) || 0),
    };
  }
  res.json({ ok: true, ip, limit: ipSpeedLimits[ip] || null });
});

// ─── OpenConnect API (local or proxied to remote agent) ─────────────
app.get('/api/openconnect/status', (req, res) => {
  const isLocal = appSettings.ocMode === 'local';
  res.json({
    mode: appSettings.ocMode || 'network',
    agent: !isLocal && ocAgent ? { hostname: ocAgent.hostname, ip: ocAgent.ip, port: ocAgent.port } : null,
    status: isLocal ? ocStatus : (ocAgent ? ocStatus : 'searching'),
    statusMessage: isLocal ? ocStatusMessage : (ocAgent ? ocStatusMessage : 'Searching for agent on LAN...'),
    connectedSince: ocConnectedSince,
    uptime: ocConnectedSince ? Date.now() - ocConnectedSince : 0,
    tunDevice: isLocal ? ocTunDevice : null,
    routedHosts: [...ocRoutedHosts],
    vpnConfig: ocVpnConfig,
    upstreamRouting: buildUpstreamRoutingState(),
  });
});

app.post('/api/openconnect/start', async (req, res) => {
  if (appSettings.ocMode === 'local') {
    ocLocalStart();
    return res.json({ ok: true });
  }
  try {
    const result = await agentRequest('/start', 'POST');
    res.json(result);
  } catch (e) {
    res.status(502).json({ error: `Agent unreachable: ${e.message}` });
  }
});

app.post('/api/openconnect/stop', async (req, res) => {
  if (appSettings.ocMode === 'local') {
    ocLocalStop();
    return res.json({ ok: true });
  }
  try {
    const result = await agentRequest('/stop', 'POST');
    res.json(result);
  } catch (e) {
    res.status(502).json({ error: `Agent unreachable: ${e.message}` });
  }
});

app.post('/api/openconnect/retry', async (req, res) => {
  if (appSettings.ocMode === 'local') {
    if (ocStatus === 'connected' || ocStatus === 'connecting') {
      return res.json({ ok: true, message: 'Already running' });
    }
    ocLocalStart();
    return res.json({ ok: true });
  }
  try {
    const result = await agentRequest('/retry', 'POST');
    res.json(result);
  } catch (e) {
    res.status(502).json({ error: `Agent unreachable: ${e.message}` });
  }
});

app.get('/api/openconnect/logs', async (req, res) => {
  if (appSettings.ocMode === 'local') {
    return res.json(ocLocalLogs);
  }
  try {
    const logs = await agentRequest('/logs');
    res.json(logs);
  } catch (e) {
    res.status(502).json({ error: `Agent unreachable: ${e.message}` });
  }
});

function getDnsSettingsPayload() {
  return {
    servers: getConfiguredDnsServers(),
    updateUrl: String((appSettings.dnsConfig || {}).updateUrl || '').trim(),
  };
}

function runDnsUpdateCurl(url) {
  return new Promise((resolve, reject) => {
    let stdout = '';
    let stderr = '';
    let settled = false;
    const proc = spawn('curl', ['-fsSL', '--max-time', '20', url], {
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    const finish = (err, result = null) => {
      if (settled) return;
      settled = true;
      if (err) reject(err);
      else resolve(result);
    };

    proc.stdout.on('data', chunk => {
      stdout += chunk.toString();
      if (stdout.length > 8192) stdout = stdout.slice(-8192);
    });
    proc.stderr.on('data', chunk => {
      stderr += chunk.toString();
      if (stderr.length > 8192) stderr = stderr.slice(-8192);
    });
    proc.on('error', err => finish(err));
    proc.on('close', code => {
      if (code !== 0) {
        finish(new Error(stderr.trim() || `curl exited with code ${code}`));
        return;
      }
      finish(null, {
        stdout: stdout.trim(),
        stderr: stderr.trim(),
      });
    });
  });
}

// OC mode settings API
app.get('/api/openconnect/settings', (req, res) => {
  res.json({
    mode: appSettings.ocMode || 'network',
    config: {
      url: (appSettings.ocConfig || {}).url || '',
      authgroup: (appSettings.ocConfig || {}).authgroup || '',
      username: (appSettings.ocConfig || {}).username || '',
      password: (appSettings.ocConfig || {}).password ? '••••••••' : '',
      hasPassword: !!(appSettings.ocConfig || {}).password,
    },
  });
});

app.post('/api/openconnect/settings', (req, res) => {
  const { mode, config } = req.body;
  const oldMode = appSettings.ocMode || 'network';
  const newMode = mode || oldMode;

  // Update config if provided
  if (config) {
    if (!appSettings.ocConfig) appSettings.ocConfig = {};
    if (config.url !== undefined) appSettings.ocConfig.url = config.url;
    if (config.authgroup !== undefined) appSettings.ocConfig.authgroup = config.authgroup;
    if (config.username !== undefined) appSettings.ocConfig.username = config.username;
    if (config.password !== undefined && config.password !== '••••••••') {
      appSettings.ocConfig.password = config.password;
    }
    ocVpnConfig = {
      url: appSettings.ocConfig.url,
      authgroup: appSettings.ocConfig.authgroup,
      username: appSettings.ocConfig.username,
    };
  }

  // Switch mode if changed
  if (newMode !== oldMode) {
    // Stop whatever is running in old mode
    if (oldMode === 'local' && ocLocalProcess) ocLocalStop('Mode switched to network');
    if (oldMode === 'network') stopNetworkDiscovery();

    appSettings.ocMode = newMode;

    if (newMode === 'network') {
      startNetworkDiscovery();
    } else {
      ocStatus = 'disconnected';
      ocStatusMessage = 'Idle';
      ocConnectedSince = null;
    }
  }

  appSettings.ocMode = newMode;
  saveAppSettings();
  broadcastOcState();
  refreshVpnRoutes().catch(() => {});
  res.json({ ok: true });
});

app.get('/api/dns/settings', (req, res) => {
  res.json(getDnsSettingsPayload());
});

app.post('/api/dns/settings', (req, res) => {
  try {
    const servers = normalizeDnsServers(req.body && req.body.servers);
    const updateUrl = normalizeDnsUpdateUrl(req.body && req.body.updateUrl);
    appSettings.dnsConfig = { servers, updateUrl };
    saveAppSettings();
    refreshConfiguredDnsEnforcement('DNS settings updated');
    res.json({ ok: true, settings: getDnsSettingsPayload() });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.post('/api/dns/update-ip', async (req, res) => {
  try {
    const configuredUrl = (appSettings.dnsConfig || {}).updateUrl;
    const url = normalizeDnsUpdateUrl(req.body && req.body.url !== undefined ? req.body.url : configuredUrl);
    if (!url) {
      return res.status(400).json({ error: 'DNS update URL is empty' });
    }
    const result = await runDnsUpdateCurl(url);
    const output = result.stdout || result.stderr || 'curl completed successfully';
    res.json({ ok: true, url, output: output.slice(0, 1000) });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.get('/api/spoofdpi/settings', (req, res) => {
  res.json(getSpoofDpiStatusPayload());
});

app.post('/api/spoofdpi/settings', async (req, res) => {
  try {
    const previousPort = getConfiguredSpoofDpiPort();
    const port = normalizeSpoofDpiPort(req.body && req.body.port, previousPort);
    appSettings.spoofDpi = {
      ...(appSettings.spoofDpi || {}),
      port,
    };
    saveAppSettings();

    let restarted = false;
    if (isSpoofDpiRunning() && port !== previousPort) {
      await restartSpoofDpiProcess('Port updated');
      restarted = true;
    }

    res.json({ ok: true, restarted, ...getSpoofDpiStatusPayload() });
  } catch (e) {
    res.status(400).json({ error: e.message, ...getSpoofDpiStatusPayload() });
  }
});

app.post('/api/spoofdpi/start', async (req, res) => {
  try {
    const port = normalizeSpoofDpiPort(req.body && req.body.port, getConfiguredSpoofDpiPort());
    appSettings.spoofDpi = {
      ...(appSettings.spoofDpi || {}),
      port,
    };
    saveAppSettings();
    await startSpoofDpiProcess(port);
    res.json({ ok: true, ...getSpoofDpiStatusPayload() });
  } catch (e) {
    res.status(400).json({ error: e.message, ...getSpoofDpiStatusPayload() });
  }
});

app.post('/api/spoofdpi/stop', async (req, res) => {
  stopSpoofDpiProcess('Stopped by user');
  await waitForSpoofDpiStop();
  res.json({ ok: true, ...getSpoofDpiStatusPayload() });
});

app.get('/api/vpn/upstream-mode', (req, res) => {
  res.json(buildUpstreamRoutingState());
});

app.post('/api/vpn/upstream-mode', (req, res) => {
  const mode = req.body && req.body.mode;
  if (!['openvpn', 'openconnect', 'both'].includes(mode)) {
    return res.status(400).json({ error: 'mode must be openvpn, openconnect, or both' });
  }

  appSettings.upstreamVpnMode = mode;
  saveAppSettings();
  broadcastOcState();
  broadcastOvpnState();
  refreshVpnRoutes().catch(() => {});
  res.json({ ok: true, ...buildUpstreamRoutingState() });
});

// ─── OpenVPN API ────────────────────────────────────────────────────
app.get('/api/openvpn/status', (req, res) => {
  res.json({
    status: ovpnStatus,
    statusMessage: ovpnStatusMessage,
    connectedSince: ovpnConnectedSince,
    uptime: ovpnConnectedSince ? Date.now() - ovpnConnectedSince : 0,
    tunDevice: ovpnTunDevice,
    routedHosts: [...ovpnRoutedHosts],
    profiles: getOvpnProfiles().map(p => p.name),
    upstreamRouting: buildUpstreamRoutingState(),
  });
});

app.get('/api/openvpn/profiles', (req, res) => {
  res.json(getOvpnProfiles().map(p => p.name));
});

app.post('/api/openvpn/upload', ovpnUpload.single('profile'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
  const origName = req.file.originalname || 'profile.ovpn';
  const dest = path.join(PROFILES_DIR, origName);
  fs.renameSync(req.file.path, dest);
  res.json({ ok: true, name: origName });
});

app.post('/api/openvpn/start', (req, res) => {
  const { profile, profilePath, username, password, saveCredentials } = req.body;
  let resolvedPath;
  if (profilePath) {
    // Absolute or relative path provided directly
    resolvedPath = path.isAbsolute(profilePath) ? profilePath : path.resolve(profilePath);
  } else if (profile) {
    // Profile name from uploaded profiles
    resolvedPath = path.join(PROFILES_DIR, profile);
  } else {
    return res.status(400).json({ error: 'Provide profile name or profilePath' });
  }
  if (!fs.existsSync(resolvedPath)) {
    return res.status(400).json({ error: `Profile not found: ${resolvedPath}` });
  }
  // Save credentials if requested
  if (saveCredentials) {
    appSettings.ovpnCredentials = { username: username || '', password: password || '' };
    saveAppSettings();
  }
  startOvpn(resolvedPath, username, password);
  res.json({ ok: true });
});

app.get('/api/openvpn/saved-credentials', (req, res) => {
  const creds = appSettings.ovpnCredentials || {};
  res.json({ username: creds.username || '', password: creds.password || '', saved: !!(creds.username || creds.password) });
});

app.delete('/api/openvpn/saved-credentials', (req, res) => {
  delete appSettings.ovpnCredentials;
  saveAppSettings();
  res.json({ ok: true });
});

app.post('/api/openvpn/stop', (req, res) => {
  stopOvpn('Stopped by user', true);
  res.json({ ok: true });
});

app.get('/api/openvpn/logs', (req, res) => {
  res.json(ovpnLogs);
});

app.delete('/api/openvpn/profiles/:name', (req, res) => {
  const filePath = path.join(PROFILES_DIR, req.params.name);
  if (!fs.existsSync(filePath)) return res.status(404).json({ error: 'Profile not found' });
  if (ovpnStatus !== 'disconnected') {
    return res.status(400).json({ error: 'Cannot delete profile while VPN is active' });
  }
  fs.unlinkSync(filePath);
  res.json({ ok: true });
});

// ─── Audit Log API ──────────────────────────────────────────────────
app.get('/api/audit', (req, res) => {
  res.json(auditLog);
});

// ─── Config Export/Import (full backup) ─────────────────────────────
app.get('/api/config/export', (req, res) => {
  const config = {
    version: 2,
    exportedAt: new Date().toISOString(),
    tunnels,
    profiles: {},
    localAccounts,
    localXraySettings,
    xrayLocalConfigs,
    appSettings: { ...appSettings, adminPassword: undefined }, // don't export password
  };
  for (const p of getOvpnProfiles()) {
    try { config.profiles[p.name] = fs.readFileSync(p.path, 'base64'); } catch {}
  }
  res.setHeader('Content-Disposition', 'attachment; filename="vpn-panel-backup.json"');
  res.json(config);
});

app.post('/api/config/import', (req, res) => {
  const data = req.body;
  if (!data || typeof data !== 'object') {
    return res.status(400).json({ error: 'Invalid backup file' });
  }

  const counts = {};

  // Tunnels
  if (Array.isArray(data.tunnels)) {
    if (activeTunnelId) stopTunnel('Importing config');
    tunnels = data.tunnels;
    saveTunnels();
    counts.tunnels = tunnels.length;
  }

  // OpenVPN profiles
  let profileCount = 0;
  if (data.profiles && typeof data.profiles === 'object') {
    for (const [name, base64] of Object.entries(data.profiles)) {
      try {
        fs.writeFileSync(path.join(PROFILES_DIR, name), Buffer.from(base64, 'base64'));
        profileCount++;
      } catch {}
    }
    counts.profiles = profileCount;
  }

  // Local accounts
  if (Array.isArray(data.localAccounts)) {
    localAccounts = data.localAccounts;
    saveLocalAccounts();
    if (localXrayProc) restartLocalXray();
    counts.accounts = localAccounts.length;
  }

  // Xray local settings (routes, outbound, etc.)
  if (data.localXraySettings && typeof data.localXraySettings === 'object') {
    localXraySettings = { ...localXraySettings, ...data.localXraySettings };
    saveLocalXraySettings();
    counts.routingRules = (localXraySettings.routes || []).length;
  }

  // Xray client configs
  if (Array.isArray(data.xrayLocalConfigs)) {
    xrayLocalConfigs = data.xrayLocalConfigs;
    saveXrayLocalConfigs();
    counts.xrayConfigs = xrayLocalConfigs.length;
  }

  // App settings (merge, don't overwrite admin creds)
  if (data.appSettings && typeof data.appSettings === 'object') {
    const { adminUsername, adminPassword, ...rest } = data.appSettings;
    appSettings = { ...appSettings, ...rest };
    saveAppSettings();
  }

  addAudit('config_imported', JSON.stringify(counts));
  res.json({ ok: true, counts });
});

// ─── Xray Binary Settings ───────────────────────────────────────────
app.get('/api/xray/binary-info', (req, res) => {
  const detected = autoDetectXrayBinary();
  const current = findXrayBinary();
  // List all bundled binaries
  const bundled = [];
  const binDir = path.join(__dirname, 'bin');
  const scan = (dir, label) => {
    const xp = path.join(dir, 'xray');
    if (fs.existsSync(xp)) bundled.push({ path: xp, label });
  };
  try {
    if (fs.existsSync(path.join(binDir, 'macos', 'arm'))) scan(path.join(binDir, 'macos', 'arm'), 'macOS ARM64');
    if (fs.existsSync(path.join(binDir, 'macos', 'x64'))) scan(path.join(binDir, 'macos', 'x64'), 'macOS x64');
    if (fs.existsSync(path.join(binDir, 'linux', 'x64'))) scan(path.join(binDir, 'linux', 'x64'), 'Linux x64');
    if (fs.existsSync(path.join(binDir, 'linux', 'arm64'))) scan(path.join(binDir, 'linux', 'arm64'), 'Linux ARM64');
  } catch {}
  res.json({
    current,
    configured: appSettings.xrayBinaryPath || '',
    detected,
    bundled,
    platform: os.platform(),
    arch: os.arch(),
  });
});

app.post('/api/xray/binary-info', (req, res) => {
  const { xrayBinaryPath } = req.body;
  appSettings.xrayBinaryPath = xrayBinaryPath || '';
  saveAppSettings();
  const current = findXrayBinary();
  res.json({ ok: true, current });
});

// ─── Admin Credentials ──────────────────────────────────────────────
app.get('/api/admin', (req, res) => {
  res.json({ username: getAuthUser() });
});

app.post('/api/admin', (req, res) => {
  const { currentPassword, newUsername, newPassword } = req.body;
  if (!currentPassword || currentPassword !== getAuthPass()) {
    return res.status(403).json({ error: 'Current password is incorrect' });
  }
  if (newUsername) appSettings.adminUsername = newUsername.trim();
  if (newPassword) appSettings.adminPassword = newPassword;
  saveAppSettings();
  // Invalidate all sessions so user must re-login with new credentials
  activeSessions.clear();
  res.json({ ok: true, message: 'Credentials updated. Please log in again.' });
});

// ─── 3X-UI Integration ──────────────────────────────────────────────
const XUI_CONFIG_FILE = path.join(__dirname, 'xui-config.json');
let xuiConfig = { url: '', username: '', password: '' };
let xuiSessionCookie = '';

function loadXuiConfig() {
  try {
    if (fs.existsSync(XUI_CONFIG_FILE)) {
      xuiConfig = JSON.parse(fs.readFileSync(XUI_CONFIG_FILE, 'utf8'));
    }
  } catch {}
}

function saveXuiConfig() {
  fs.writeFileSync(XUI_CONFIG_FILE, JSON.stringify(xuiConfig, null, 2));
}

loadXuiConfig();

async function xuiRequest(apiPath, method = 'GET', body = null, form = false) {
  const baseUrl = xuiConfig.url.replace(/\/+$/, '');
  if (!baseUrl) throw new Error('3X-UI not configured');

  const url = baseUrl + apiPath;
  console.log(`[3X-UI] ${method} ${url}${form ? ' (form)' : ''}`);
  const headers = {};
  if (xuiSessionCookie) headers['Cookie'] = xuiSessionCookie;

  const opts = { method, headers };
  if (body && form) {
    headers['Content-Type'] = 'application/x-www-form-urlencoded';
    opts.body = new URLSearchParams(body).toString();
  } else if (body) {
    headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }

  const resp = await fetch(url, opts);
  console.log(`[3X-UI] Response: ${resp.status} ${resp.statusText}`);

  // Capture set-cookie
  const setCookie = resp.headers.get('set-cookie');
  if (setCookie) {
    const match = setCookie.match(/3x-ui=([^;]+)/);
    if (match) xuiSessionCookie = '3x-ui=' + match[1];
  }

  if (resp.status === 401) {
    return { success: false, msg: 'unauthorized' };
  }

  const text = await resp.text();
  try {
    return JSON.parse(text);
  } catch {
    console.log(`[3X-UI] Non-JSON response: ${text.substring(0, 200)}`);
    return { success: false, msg: `HTTP ${resp.status}: non-JSON response` };
  }
}

async function xuiLogin() {
  if (!xuiConfig.url || !xuiConfig.username) throw new Error('3X-UI not configured');
  console.log(`[3X-UI] Logging in as ${xuiConfig.username} to ${xuiConfig.url}`);
  xuiSessionCookie = '';
  const result = await xuiRequest('/login', 'POST', {
    username: xuiConfig.username,
    password: xuiConfig.password,
  });
  console.log(`[3X-UI] Login result: ${JSON.stringify(result)}`);
  if (!result.success) throw new Error('3X-UI login failed: ' + (result.msg || 'unknown error'));
  console.log(`[3X-UI] Session cookie: ${xuiSessionCookie ? 'obtained' : 'MISSING'}`);
  return result;
}

async function xuiApi(apiPath, method = 'POST', body = null, form = false) {
  let result = await xuiRequest(apiPath, method, body, form);
  // If unauthorized, re-login and retry
  if (!result.success && /login|session|unauthorized/i.test(result.msg || '')) {
    await xuiLogin();
    result = await xuiRequest(apiPath, method, body, form);
  }
  return result;
}

// Config endpoints
app.get('/api/xui/config', (req, res) => {
  res.json({ url: xuiConfig.url, username: xuiConfig.username, configured: !!xuiConfig.url });
});

app.post('/api/xui/config', (req, res) => {
  const { url, username, password } = req.body;
  xuiConfig = { url: url || '', username: username || '', password: password || xuiConfig.password || '' };
  xuiSessionCookie = '';
  saveXuiConfig();
  res.json({ ok: true });
});

app.post('/api/xui/test', async (req, res) => {
  try {
    await xuiLogin();
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, error: e.message });
  }
});

// Proxy endpoints
app.get('/api/xui/inbounds', async (req, res) => {
  try {
    const data = await xuiApi('/panel/api/inbounds/list', 'GET');
    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, msg: e.message });
  }
});

app.get('/api/xui/inbound/:id', async (req, res) => {
  try {
    const data = await xuiApi(`/panel/api/inbounds/get/${req.params.id}`, 'GET');
    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, msg: e.message });
  }
});

app.post('/api/xui/onlines', async (req, res) => {
  try {
    const data = await xuiApi('/panel/api/inbounds/onlines', 'POST');
    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, msg: e.message });
  }
});

app.get('/api/xui/client-traffic/:email', async (req, res) => {
  try {
    const data = await xuiApi(`/panel/api/inbounds/getClientTraffics/${req.params.email}`, 'GET');
    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, msg: e.message });
  }
});

app.post('/api/xui/client/add', async (req, res) => {
  try {
    const data = await xuiApi('/panel/api/inbounds/addClient', 'POST', req.body);
    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, msg: e.message });
  }
});

app.post('/api/xui/client/update/:clientId', async (req, res) => {
  try {
    const data = await xuiApi(`/panel/api/inbounds/updateClient/${req.params.clientId}`, 'POST', req.body);
    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, msg: e.message });
  }
});

app.post('/api/xui/client/delete/:inboundId/:clientId', async (req, res) => {
  try {
    const data = await xuiApi(`/panel/api/inbounds/${req.params.inboundId}/delClient/${req.params.clientId}`, 'POST');
    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, msg: e.message });
  }
});

app.post('/api/xui/client/reset-traffic/:inboundId/:email', async (req, res) => {
  try {
    const data = await xuiApi(`/panel/api/inbounds/${req.params.inboundId}/resetClientTraffic/${req.params.email}`, 'POST');
    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, msg: e.message });
  }
});

app.post('/api/xui/inbound/enable/:id', async (req, res) => {
  try {
    // Get current inbound, toggle enable
    const getRes = await xuiApi(`/panel/api/inbounds/get/${req.params.id}`, 'GET');
    if (!getRes.success) return res.json(getRes);
    const inbound = getRes.obj;
    inbound.enable = req.body.enable;
    const data = await xuiApi(`/panel/api/inbounds/update/${req.params.id}`, 'POST', inbound);
    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, msg: e.message });
  }
});

app.get('/api/xui/server-status', async (req, res) => {
  try {
    const data = await xuiApi('/panel/api/server/status', 'GET');
    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, msg: e.message });
  }
});

// Xray settings endpoints
app.get('/api/xui/xray-settings', async (req, res) => {
  try {
    const data = await xuiApi('/panel/xray/', 'POST');
    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, msg: e.message });
  }
});

app.post('/api/xui/xray-settings', async (req, res) => {
  try {
    const data = await xuiApi('/panel/xray/update', 'POST', req.body);
    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, msg: e.message });
  }
});

app.post('/api/xui/restart-xray', async (req, res) => {
  try {
    const data = await xuiApi('/panel/api/server/restartXrayService', 'POST');
    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, msg: e.message });
  }
});

app.get('/api/xui/panel-settings', async (req, res) => {
  try {
    const data = await xuiApi('/panel/setting/all', 'POST');
    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, msg: e.message });
  }
});

app.post('/api/xui/panel-settings', async (req, res) => {
  try {
    const data = await xuiApi('/panel/setting/update', 'POST', req.body);
    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, msg: e.message });
  }
});

app.post('/api/xui/restart-panel', async (req, res) => {
  try {
    const data = await xuiApi('/panel/setting/restartPanel', 'POST');
    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, msg: e.message });
  }
});

app.get('/api/xui/xray-config', async (req, res) => {
  try {
    const data = await xuiApi('/panel/api/server/getConfigJson', 'GET');
    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, msg: e.message });
  }
});

// ─── System Statistics (on-demand only) ─────────────────────────────
const serverStartTime = Date.now();
let lastCpuIdle = 0;
let lastCpuTotal = 0;
let lastCpuPercent = 0;

// Snapshot initial CPU counters so the first request has a baseline
(() => {
  const cpus = os.cpus();
  for (const cpu of cpus) {
    for (const type in cpu.times) lastCpuTotal += cpu.times[type];
    lastCpuIdle += cpu.times.idle;
  }
})();

function getCpuPercent() {
  const cpus = os.cpus();
  let idle = 0, total = 0;
  for (const cpu of cpus) {
    for (const type in cpu.times) total += cpu.times[type];
    idle += cpu.times.idle;
  }
  const dTotal = total - lastCpuTotal;
  const dIdle = idle - lastCpuIdle;
  if (dTotal > 0) lastCpuPercent = Math.round(100 * (1 - dIdle / dTotal));
  lastCpuIdle = idle;
  lastCpuTotal = total;
  return lastCpuPercent;
}

function getBattery() {
  try {
    if (process.platform === 'darwin') {
      const out = execSync('pmset -g batt', { timeout: 3000 }).toString();
      const pctMatch = out.match(/(\d+)%/);
      const charging = /AC Power|charging|charged/i.test(out);
      return pctMatch ? { percent: parseInt(pctMatch[1]), charging } : null;
    } else if (process.platform === 'linux') {
      const cap = fs.readFileSync('/sys/class/power_supply/BAT0/capacity', 'utf8').trim();
      let charging = false;
      try {
        const st = fs.readFileSync('/sys/class/power_supply/BAT0/status', 'utf8').trim();
        charging = st === 'Charging' || st === 'Full';
      } catch {}
      return { percent: parseInt(cap), charging };
    }
  } catch {}
  return null;
}

function getNetworkTotals() {
  let rx = 0, tx = 0;
  try {
    if (process.platform === 'darwin') {
      const out = execSync("netstat -ib | awk 'NR>1 && $1!~/lo/ {rx+=$7; tx+=$10} END{print rx, tx}'", { timeout: 3000 }).toString().trim();
      const parts = out.split(/\s+/);
      rx = parseInt(parts[0]) || 0;
      tx = parseInt(parts[1]) || 0;
    } else if (process.platform === 'linux') {
      const lines = fs.readFileSync('/proc/net/dev', 'utf8').split('\n');
      for (const line of lines) {
        const m = line.match(/^\s*(\w+):\s*(\d+)(?:\s+\d+){7}\s+(\d+)/);
        if (m && m[1] !== 'lo') {
          rx += parseInt(m[2]) || 0;
          tx += parseInt(m[3]) || 0;
        }
      }
    }
  } catch {}
  return { rx, tx };
}

app.get('/api/system-stats', (req, res) => {
  const totalMem = os.totalmem();
  const freeMem = os.freemem();
  const usedMem = totalMem - freeMem;

  const conns = getConnectionsList();
  const rClients = getRemoteClientsList();
  const hasRemote = rClients.length > 0;
  const activeConns = hasRemote ? rClients.filter(c => c.clientIP !== '127.0.0.1') : conns.filter(c => c.srcIP !== '127.0.0.1');
  const uniqueIPs = [...new Set(activeConns.map(c => c.clientIP || c.srcIP))].length;

  res.json({
    cpu: {
      loadPercent: getCpuPercent(),
      cores: os.cpus().length,
    },
    memory: {
      total: totalMem,
      used: usedMem,
      free: freeMem,
      usedPercent: (usedMem / totalMem) * 100,
    },
    battery: getBattery(),
    network: getNetworkTotals(),
    connections: {
      total: activeConns.length,
      uniqueIPs,
    },
    platform: os.platform() + ' ' + os.arch(),
    hostname: os.hostname(),
    osUptime: os.uptime(),
    serverUptime: Math.round((Date.now() - serverStartTime) / 1000),
  });
});

app.post('/api/geoip/batch', async (req, res) => {
  try {
    const ips = Array.isArray(req.body && req.body.ips) ? req.body.ips : [];
    const result = await lookupGeoIpBatch(ips);
    res.json(result);
  } catch (e) {
    res.status(500).json({ error: e.message || 'Geo lookup failed' });
  }
});

// ─── Deploy (source update) ─────────────────────────────────────────
const deployUpload = multer({ dest: os.tmpdir(), limits: { fileSize: 10 * 1024 * 1024 } });

app.post('/api/deploy', deployUpload.single('archive'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No archive uploaded' });
  const tmpFile = req.file.path;
  const extractDir = path.join(os.tmpdir(), 'tunnel-deploy-' + Date.now());
  try {
    fs.mkdirSync(extractDir, { recursive: true });
    const origName = (req.file.originalname || '').toLowerCase();
    if (origName.endsWith('.zip')) {
      execSync(`unzip -o "${tmpFile}" -d "${extractDir}"`, { timeout: 30000 });
    } else {
      execSync(`tar xzf "${tmpFile}" -C "${extractDir}"`, { timeout: 30000 });
    }
    const allowList = [
      'server.js', 'oc-agent.js', 'package.json', 'install-vpn-servers.sh',
      'public/app.js', 'public/index.html', 'public/style.css',
      'public/login.html', 'public/sw.js', 'public/manifest.json',
    ];
    const updated = [];
    for (const relPath of allowList) {
      const src = path.join(extractDir, relPath);
      if (fs.existsSync(src)) {
        const dest = path.join(__dirname, relPath);
        fs.mkdirSync(path.dirname(dest), { recursive: true });
        fs.copyFileSync(src, dest);
        updated.push(relPath);
      }
    }
    try { execSync(`rm -rf "${extractDir}" "${tmpFile}"`); } catch {}
    if (updated.length === 0) return res.json({ ok: false, error: 'No recognized source files in archive' });
    if (updated.includes('package.json')) {
      try { execSync('npm install --production', { cwd: __dirname, timeout: 60000 }); } catch {}
    }
    addAudit('deploy', `Updated: ${updated.join(', ')}`);
    res.json({ ok: true, updated, message: 'Deploy successful, restarting in 2s...' });
    setTimeout(() => process.exit(0), 2000);
  } catch (e) {
    try { execSync(`rm -rf "${extractDir}" "${tmpFile}"`); } catch {}
    res.status(500).json({ error: `Deploy failed: ${e.message}` });
  }
});

// ─── WebSocket connections ──────────────────────────────────────────
wss.on('connection', (ws, req) => {
  // Authenticate WebSocket via cookie
  const cookies = req.headers.cookie || '';
  const match = cookies.match(/(?:^|;\s*)token=([^;]+)/);
  const token = match ? match[1] : null;
  if (!token || !activeSessions.has(token)) {
    ws.close(4001, 'Unauthorized');
    return;
  }

  // Send current SSH tunnel state immediately
  const activeTunnel = tunnels.find(t => t.id === activeTunnelId);
  ws.send(JSON.stringify({
    type: 'state',
    activeTunnelId,
    status: connectionStatus,
    statusMessage,
    panelUrl: panelExposed && activeTunnel ? `http://${activeTunnel.host}:${PANEL_REMOTE_PORT}` : null,
    stats: {
      ...sessionStats,
      uptime: sessionStats.connectedSince ? Date.now() - sessionStats.connectedSince : 0,
      bandwidthIn,
      bandwidthOut,
    },
    connections: getConnectionsList(),
    remoteClients: getRemoteClientsList(),
    health: healthStatus,
  }));

  // Send current OpenConnect state
  const isOcLocal = appSettings.ocMode === 'local';
  ws.send(JSON.stringify({
    type: 'oc_state',
    mode: appSettings.ocMode || 'network',
    agent: !isOcLocal && ocAgent ? { hostname: ocAgent.hostname, ip: ocAgent.ip, port: ocAgent.port } : null,
    status: isOcLocal ? ocStatus : (ocAgent ? ocStatus : 'searching'),
    statusMessage: isOcLocal ? ocStatusMessage : (ocAgent ? ocStatusMessage : 'Searching for agent on LAN...'),
    connectedSince: ocConnectedSince,
    uptime: ocConnectedSince ? Date.now() - ocConnectedSince : 0,
    vpnConfig: ocVpnConfig,
    upstreamRouting: buildUpstreamRoutingState(),
  }));

  ws.send(JSON.stringify(buildOvpnStatePayload()));

  ws.send(JSON.stringify({
    type: 'ssh_tunnel_logs',
    entries: sshTunnelLogs,
  }));

});

// ─── Local V2Ray Account Manager ────────────────────────────────────
// Xray instance #1: local server — accepts inbound connections from others
const LOCAL_ACCOUNTS_FILE = path.join(__dirname, 'local-accounts.json');
const LOCAL_XRAY_SETTINGS_FILE = path.join(__dirname, 'local-xray-settings.json');
let localAccounts = [];
let localXrayProc = null;
let localXrayLogs = [];
let localXraySettings = { outbound: 'direct', routes: [] }; // routes: [{ id, type, pattern, outboundTag }]

// Per-IP access history populated from xray access log
let localClientHistory = new Map(); // ip -> { accountId, firstSeen, lastSeen, totalAccesses, accesses:[{t,dest,outbound}] }
let localAccessLog = []; // flat array, last 2000 entries for display
let localXrayAccessLogPath = null; // temp file path for xray access log
let localXrayAccessLogPos = 0;  // byte position read so far
let localXrayAccessLogWatcher = null;
let localXrayAccessLogTail = '';

function parseLocalXrayTimestamp(datePart, timePart) {
  const safeTime = String(timePart || '00:00:00')
    .replace(/(\.\d{3})\d+$/, '$1');
  const parsed = new Date(`${String(datePart || '').replace(/\//g, '-') }T${safeTime}`);
  return Number.isNaN(parsed.getTime()) ? Date.now() : parsed.getTime();
}

function getLocalAccountIdFromInboundTag(inboundTag) {
  const tag = String(inboundTag || '').trim();
  if (!tag) return null;
  for (const acc of localAccounts) {
    const tags = [`in-${acc.id}`, ...getLocalAccountPorts(acc).map(port => `in-${acc.id}-${port}`)];
    if (tags.includes(tag)) return acc.id;
  }
  return null;
}

function getLocalAccountName(accountId) {
  if (!accountId) return null;
  const acc = localAccounts.find(item => item.id === accountId);
  return acc ? acc.name : null;
}

function parseLocalXrayAccessLine(line) {
  // xray access log example:
  // 2026/03/21 13:07:17.701674 from 192.168.1.173:57538 accepted tcp:example.com:443 [in-<id> -> outbound] email: <id>@local
  const m = line.match(/^(\d{4}\/\d{2}\/\d{2})\s+([\d:.]+)\s+(?:from\s+)?(\[[^\]]+\]|[^\s]+):(\d+)\s+(accepted|rejected)\s+([a-z0-9_-]+):(.+?)(?:\s+\[([^\]]*)\])?(?:\s+email:\s+(\S+))?$/i);
  if (!m) return null;
  const [, datePart, timePart, srcIpRaw, srcPort, status, network, rawDest, routeInfo, email] = m;
  const srcIp = String(srcIpRaw || '').replace(/^\[|\]$/g, '');
  const routeParts = String(routeInfo || '')
    .split('->')
    .map(part => part.trim())
    .filter(Boolean);
  const inboundTag = routeParts[0] || null;
  const outboundTag = routeParts.length > 1 ? routeParts[routeParts.length - 1] : (routeParts[0] || 'direct');
  const accountId = email
    ? email.replace(/@.*$/, '')
    : getLocalAccountIdFromInboundTag(inboundTag);
  return {
    t: parseLocalXrayTimestamp(datePart, timePart),
    srcIp,
    srcPort: parseInt(srcPort, 10) || 0,
    status,
    network: String(network || '').toLowerCase(),
    dest: String(rawDest || '').trim(),
    outbound: outboundTag || 'direct',
    inbound: inboundTag,
    email: email || null,
    accountId,
    accountName: getLocalAccountName(accountId),
  };
}

function recordLocalXrayAccess(entry) {
  if (!localClientHistory.has(entry.srcIp)) {
    localClientHistory.set(entry.srcIp, {
      accountId: entry.accountId,
      accountName: entry.accountName || null,
      firstSeen: entry.t,
      lastSeen: entry.t,
      totalAccesses: 0,
      accesses: [],
    });
  }
  const hist = localClientHistory.get(entry.srcIp);
  hist.lastSeen = entry.t;
  hist.totalAccesses++;
  if (entry.accountId) hist.accountId = entry.accountId;
  if (entry.accountName) hist.accountName = entry.accountName;
  hist.accesses.push({ t: entry.t, dest: entry.dest, outbound: entry.outbound, network: entry.network });
  if (hist.accesses.length > 200) hist.accesses.shift();

  localAccessLog.push({
    t: entry.t,
    srcIp: entry.srcIp,
    dest: entry.dest,
    outbound: entry.outbound,
    network: entry.network,
    email: entry.email,
    accountId: entry.accountId,
    accountName: entry.accountName || null,
    status: entry.status,
  });
  if (localAccessLog.length > 2000) localAccessLog.shift();
}

function stopXrayAccessLogWatcher() {
  if (localXrayAccessLogWatcher) {
    try { localXrayAccessLogWatcher.close(); } catch {}
    localXrayAccessLogWatcher = null;
  }
  if (localXrayAccessLogPath) {
    try { fs.unlinkSync(localXrayAccessLogPath); } catch {}
    localXrayAccessLogPath = null;
  }
  localXrayAccessLogPos = 0;
  localXrayAccessLogTail = '';
}

function startXrayAccessLogWatcher(logPath) {
  stopXrayAccessLogWatcher();
  localXrayAccessLogPath = logPath;
  localXrayAccessLogPos = 0;

  function readNewLines() {
    try {
      const stat = fs.statSync(logPath);
      if (stat.size <= localXrayAccessLogPos) return;
      const fd = fs.openSync(logPath, 'r');
      const toRead = stat.size - localXrayAccessLogPos;
      const buf = Buffer.alloc(toRead);
      const bytesRead = fs.readSync(fd, buf, 0, toRead, localXrayAccessLogPos);
      fs.closeSync(fd);
      if (bytesRead > 0) {
        localXrayAccessLogPos += bytesRead;
        const content = localXrayAccessLogTail + buf.subarray(0, bytesRead).toString();
        const lines = content.split(/\r?\n/);
        localXrayAccessLogTail = lines.pop() || '';
        lines.forEach(l => {
          if (!l.trim()) return;
          const entry = parseLocalXrayAccessLine(l.trim());
          if (entry) recordLocalXrayAccess(entry);
        });
      }
    } catch {}
  }

  try {
    localXrayAccessLogWatcher = fs.watch(logPath, () => readNewLines());
  } catch {
    // fs.watch may not be available or reliable; fall back to polling
    const timer = setInterval(() => {
      if (!localXrayAccessLogPath) { clearInterval(timer); return; }
      readNewLines();
    }, 1000);
    localXrayAccessLogWatcher = { close: () => clearInterval(timer) };
  }

  readNewLines();
}

function loadLocalXraySettings() {
  try {
    if (fs.existsSync(LOCAL_XRAY_SETTINGS_FILE)) {
      localXraySettings = JSON.parse(fs.readFileSync(LOCAL_XRAY_SETTINGS_FILE, 'utf8'));
    }
  } catch {}
}
function saveLocalXraySettings() {
  fs.writeFileSync(LOCAL_XRAY_SETTINGS_FILE, JSON.stringify(localXraySettings, null, 2));
}
loadLocalXraySettings();

function loadLocalAccounts() {
  try {
    if (fs.existsSync(LOCAL_ACCOUNTS_FILE)) {
      localAccounts = JSON.parse(fs.readFileSync(LOCAL_ACCOUNTS_FILE, 'utf8'));
    }
  } catch {}
}
function saveLocalAccounts() {
  ensureAdditiveJsonBackup(
    LOCAL_ACCOUNTS_FILE,
    'local-accounts-assigned-xray',
    localAccounts.some(acc => Object.prototype.hasOwnProperty.call(acc || {}, 'assignedXrayConfigId'))
  );
  fs.writeFileSync(LOCAL_ACCOUNTS_FILE, JSON.stringify(localAccounts, null, 2));
}
loadLocalAccounts();


const LOCAL_XRAY_API_PORT = 10085; // xray gRPC stats API port
const LOCAL_ACCOUNT_LIMIT_CHAIN_PREFIX = 'VPNACC_';
const availableCommandCache = new Map();
let localAccountLimitState = new Map();

function hasCommand(cmd) {
  if (availableCommandCache.has(cmd)) return availableCommandCache.get(cmd);
  let exists = false;
  try {
    exists = !!execSync(`command -v ${cmd} 2>/dev/null`, { encoding: 'utf8', timeout: 2000 }).trim();
  } catch {}
  availableCommandCache.set(cmd, exists);
  return exists;
}

function isIgnoredClientIp(ip) {
  const value = String(ip || '').trim();
  return !value || value === '127.0.0.1' || value === '0.0.0.0' || value === '::1';
}

function splitHostPort(value) {
  const raw = String(value || '').trim();
  if (!raw) return { host: '', port: 0 };
  const bracket = raw.match(/^\[([^\]]+)\]:(\d+)$/);
  if (bracket) return { host: bracket[1], port: parseInt(bracket[2], 10) || 0 };
  const idx = raw.lastIndexOf(':');
  if (idx === -1) return { host: raw, port: 0 };
  return {
    host: raw.slice(0, idx),
    port: parseInt(raw.slice(idx + 1), 10) || 0,
  };
}

function normalizeTcpPort(value) {
  const port = parseInt(value, 10);
  return Number.isInteger(port) && port > 0 && port <= 65535 ? port : 0;
}

function appendNormalizedPorts(target, seen, value) {
  if (value == null || value === '') return;

  if (Array.isArray(value)) {
    value.forEach(item => appendNormalizedPorts(target, seen, item));
    return;
  }

  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return;

    if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
      try {
        appendNormalizedPorts(target, seen, JSON.parse(trimmed));
        return;
      } catch {}
    }

    if (/[,\s;]+/.test(trimmed)) {
      trimmed.split(/[\s,;]+/).forEach(part => appendNormalizedPorts(target, seen, part));
      return;
    }
  }

  const port = normalizeTcpPort(value);
  if (!port || seen.has(port)) return;
  seen.add(port);
  target.push(port);
}

function normalizePortList(...values) {
  const ports = [];
  const seen = new Set();
  values.forEach(value => appendNormalizedPorts(ports, seen, value));
  return ports;
}

function getLocalAccountPorts(acc) {
  if (!acc) return [];
  return normalizePortList(
    acc.port,
    acc.ports,
    acc.extraPorts,
    acc.additionalPorts,
    acc.listenPorts,
    acc.portList,
    acc.port_list
  );
}

function getLocalAccountPrimaryPort(acc) {
  return getLocalAccountPorts(acc)[0] || normalizeTcpPort(acc && acc.port) || 0;
}

function getLocalAccountPortDisplay(acc) {
  const ports = getLocalAccountPorts(acc);
  return ports.length ? ports.join(', ') : '--';
}

function hasExplicitLocalAccountPortList(body) {
  return ['ports', 'extraPorts', 'additionalPorts', 'listenPorts', 'portList', 'port_list']
    .some(key => body && body[key] !== undefined);
}

function buildRequestedLocalAccountPorts(body) {
  return normalizePortList(
    body && body.port,
    body && body.ports,
    body && body.extraPorts,
    body && body.additionalPorts,
    body && body.listenPorts,
    body && body.portList,
    body && body.port_list
  );
}

function getLocalAccountLimitStatePorts(state) {
  return normalizePortList(state && state.ports, state && state.port);
}

function parseConntrackTcpSessions(output, trackedPorts) {
  const sessionsByPort = new Map();
  for (const line of String(output || '').split('\n')) {
    const srcMatch = line.match(/src=([^\s]+)/);
    const sportMatch = line.match(/sport=(\d+)/);
    const dportMatch = line.match(/dport=(\d+)/);
    if (!srcMatch || !sportMatch || !dportMatch) continue;

    const remoteIp = srcMatch[1];
    const remotePort = parseInt(sportMatch[1], 10) || 0;
    const localPort = parseInt(dportMatch[1], 10) || 0;
    if (!trackedPorts.has(localPort) || isIgnoredClientIp(remoteIp)) continue;

    if (!sessionsByPort.has(localPort)) sessionsByPort.set(localPort, []);
    sessionsByPort.get(localPort).push({ ip: remoteIp, port: remotePort, source: 'conntrack' });
  }
  return sessionsByPort;
}

function parseLsofTcpSessions(output, trackedPorts) {
  const sessionsByPort = new Map();
  const lines = String(output || '').split('\n').filter(Boolean);
  for (const line of lines.slice(1)) {
    const tcpIdx = line.indexOf(' TCP ');
    if (tcpIdx === -1) continue;
    const descriptor = line.slice(tcpIdx + 5).trim();
    const match = descriptor.match(/^(.+?)->(.+?) \(([A-Z_]+)\)$/);
    if (!match) continue;

    const left = splitHostPort(match[1]);
    const right = splitHostPort(match[2]);
    let localPort = 0;
    let remoteIp = '';
    let remotePort = 0;

    if (left.port && trackedPorts.has(left.port)) {
      localPort = left.port;
      remoteIp = right.host;
      remotePort = right.port;
    } else if (right.port && trackedPorts.has(right.port)) {
      localPort = right.port;
      remoteIp = left.host;
      remotePort = left.port;
    } else {
      continue;
    }

    if (isIgnoredClientIp(remoteIp)) continue;
    if (!sessionsByPort.has(localPort)) sessionsByPort.set(localPort, []);
    sessionsByPort.get(localPort).push({ ip: remoteIp, port: remotePort, source: 'lsof' });
  }
  return sessionsByPort;
}

// ss -tnH state established output: [State] Recv-Q Send-Q Local:Port Peer:Port
// Search each line for a column pair where left side port matches a tracked port
function parseSsTcpSessions(output, trackedPorts) {
  const sessionsByPort = new Map();
  for (const line of String(output || '').split('\n')) {
    const parts = line.trim().split(/\s+/);
    if (parts.length < 4) continue;
    for (let i = 0; i < parts.length - 1; i++) {
      const local = splitHostPort(parts[i]);
      if (!local.port || !trackedPorts.has(local.port)) continue;
      const peer = splitHostPort(parts[i + 1]);
      if (!peer.host || !peer.port || isIgnoredClientIp(peer.host)) break;
      if (!sessionsByPort.has(local.port)) sessionsByPort.set(local.port, []);
      sessionsByPort.get(local.port).push({ ip: peer.host, port: peer.port, source: 'ss' });
      break;
    }
  }
  return sessionsByPort;
}

function buildLocalAccountConnectionSnapshot(accounts) {
  const enabledAccounts = (accounts || localAccounts).filter(acc => acc && acc.enabled);
  const byAccountId = new Map();
  const trackedPorts = new Set();
  enabledAccounts.forEach(acc => {
    getLocalAccountPorts(acc).forEach(port => trackedPorts.add(port));
  });

  let sessionsByPort = new Map();
  let source = 'none';
  let limitSupported = false;

  if (localXrayProc && trackedPorts.size > 0) {
    if (process.platform === 'linux' && hasCommand('conntrack')) {
      try {
        const output = execSync('conntrack -L -p tcp 2>/dev/null', {
          encoding: 'utf8',
          timeout: 5000,
          stdio: ['ignore', 'pipe', 'pipe'],
        });
        sessionsByPort = parseConntrackTcpSessions(output, trackedPorts);
        source = 'conntrack';
        limitSupported = true;
      } catch {}
    }

    if (source === 'none' && hasCommand('lsof')) {
      try {
        const output = execSync('lsof -nP -iTCP -sTCP:ESTABLISHED 2>/dev/null', {
          encoding: 'utf8',
          timeout: 5000,
          stdio: ['ignore', 'pipe', 'pipe'],
        });
        sessionsByPort = parseLsofTcpSessions(output, trackedPorts);
        source = 'lsof';
        limitSupported = true;
      } catch {}
    }

    if (source === 'none' && process.platform === 'linux' && hasCommand('ss')) {
      try {
        const output = execSync('ss -tnH state established', {
          encoding: 'utf8',
          timeout: 5000,
          stdio: ['ignore', 'pipe', 'pipe'],
        });
        sessionsByPort = parseSsTcpSessions(output, trackedPorts);
        source = 'ss';
        limitSupported = true;
      } catch {}
    }
  }

  for (const acc of enabledAccounts) {
    const sessions = [];
    for (const port of getLocalAccountPorts(acc)) {
      sessions.push(...(sessionsByPort.get(port) || []).map(session => ({ ...session, localPort: port })));
    }
    const uniqueIps = [...new Set(sessions.map(session => session.ip))];
    byAccountId.set(acc.id, {
      source,
      limitSupported,
      activeSessions: sessions.length,
      activeUniqueIps: uniqueIps.length,
      activeIps: uniqueIps,
      blockedIps: [],
      atCapacity: false,
    });
  }

  return { source, limitSupported, byAccountId };
}

function getLocalAccountLimitChainName(accountId) {
  const suffix = String(accountId || '').replace(/[^a-zA-Z0-9]/g, '').slice(0, 21);
  return `${LOCAL_ACCOUNT_LIMIT_CHAIN_PREFIX}${suffix}`;
}

function runIptablesCommand(cmd) {
  return execSync(cmd, {
    encoding: 'utf8',
    timeout: 5000,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
}

function deleteLocalAccountInputJumps(ports, chain) {
  if (!chain || process.platform !== 'linux' || !hasCommand('iptables')) return;
  for (const port of normalizePortList(ports)) {
    while (true) {
      try {
        runIptablesCommand(`iptables -D INPUT -p tcp --dport ${port} -j ${chain}`);
      } catch {
        break;
      }
    }
  }
}

function clearLocalAccountLimitStateEntry(accountId, stateOverride = null) {
  const state = stateOverride || localAccountLimitState.get(accountId);
  if (!state) return;

  if (process.platform === 'linux' && hasCommand('iptables')) {
    deleteLocalAccountInputJumps(getLocalAccountLimitStatePorts(state), state.chain);
    try { runIptablesCommand(`iptables -F ${state.chain}`); } catch {}
    try { runIptablesCommand(`iptables -X ${state.chain}`); } catch {}
  }

  localAccountLimitState.delete(accountId);
}

function clearAllLocalAccountLimitState() {
  for (const [accountId, state] of [...localAccountLimitState.entries()]) {
    clearLocalAccountLimitStateEntry(accountId, state);
  }
}

function applyLocalAccountLimitChain(acc, chain, allowedIps, blockNewIps) {
  if (process.platform !== 'linux' || !hasCommand('iptables')) return;
  const ports = getLocalAccountPorts(acc);
  if (!ports.length) return;
  try { runIptablesCommand(`iptables -N ${chain}`); } catch {}
  for (const port of ports) {
    try {
      runIptablesCommand(`iptables -C INPUT -p tcp --dport ${port} -j ${chain}`);
    } catch {
      try { runIptablesCommand(`iptables -I INPUT -p tcp --dport ${port} -j ${chain}`); } catch {}
    }
  }

  try { runIptablesCommand(`iptables -F ${chain}`); } catch {}
  for (const ip of allowedIps) {
    try { runIptablesCommand(`iptables -A ${chain} -s ${ip} -j ACCEPT`); } catch {}
  }
  if (blockNewIps) {
    try { runIptablesCommand(`iptables -A ${chain} -j REJECT --reject-with tcp-reset`); }
    catch {
      try { runIptablesCommand(`iptables -A ${chain} -j REJECT`); } catch {}
    }
  }
  try { runIptablesCommand(`iptables -A ${chain} -j RETURN`); } catch {}
}

function disconnectLocalAccountIps(ports, ips) {
  const portList = normalizePortList(ports);
  if (!portList.length || process.platform !== 'linux' || !hasCommand('conntrack')) return;
  for (const port of portList) {
    for (const ip of ips || []) {
      if (isIgnoredClientIp(ip)) continue;
      try {
        execSync(`conntrack -D -p tcp -s ${ip} --dport ${port} 2>/dev/null`, {
          timeout: 5000,
          stdio: ['ignore', 'pipe', 'pipe'],
        });
      } catch {}
    }
  }
}

function syncLocalAccountIpLimits(snapshotByAccountId) {
  const activeAccountIds = new Set();
  const accountsById = new Map(localAccounts.map(acc => [acc.id, acc]));

  if (process.platform !== 'linux' || !hasCommand('iptables')) {
    clearAllLocalAccountLimitState();
    return;
  }

  for (const acc of localAccounts) {
    const snapshot = snapshotByAccountId.get(acc.id) || {
      activeSessions: 0,
      activeUniqueIps: 0,
      activeIps: [],
      blockedIps: [],
      atCapacity: false,
      limitSupported: false,
    };
    const accountPorts = getLocalAccountPorts(acc);

    if (!localXrayProc || !acc.enabled || !(acc.maxConnections > 0) || !accountPorts.length) {
      clearLocalAccountLimitStateEntry(acc.id);
      continue;
    }

    activeAccountIds.add(acc.id);

    const previous = localAccountLimitState.get(acc.id) || {
      chain: getLocalAccountLimitChainName(acc.id),
      ports: accountPorts,
      seenAt: new Map(),
      allowedIps: [],
      blockedIps: [],
    };

    const previousPorts = getLocalAccountLimitStatePorts(previous);
    const removedPorts = previousPorts.filter(port => !accountPorts.includes(port));
    if (removedPorts.length > 0) {
      deleteLocalAccountInputJumps(removedPorts, previous.chain);
    }

    previous.ports = accountPorts;
    const activeIps = [...new Set((snapshot.activeIps || []).filter(ip => !isIgnoredClientIp(ip)))];
    const activeIpSet = new Set(activeIps);

    for (const ip of [...previous.seenAt.keys()]) {
      if (!activeIpSet.has(ip)) previous.seenAt.delete(ip);
    }
    for (const ip of activeIps) {
      if (!previous.seenAt.has(ip)) previous.seenAt.set(ip, Date.now());
    }

    activeIps.sort((a, b) => {
      const timeDiff = (previous.seenAt.get(a) || 0) - (previous.seenAt.get(b) || 0);
      return timeDiff !== 0 ? timeDiff : a.localeCompare(b);
    });

    const allowedIps = activeIps.slice(0, acc.maxConnections);
    const allowedIpSet = new Set(allowedIps);
    const blockedIps = activeIps.filter(ip => !allowedIpSet.has(ip));
    const atCapacity = allowedIps.length >= acc.maxConnections;

    previous.allowedIps = allowedIps;
    previous.blockedIps = blockedIps;
    previous.atCapacity = atCapacity;
    localAccountLimitState.set(acc.id, previous);

    snapshot.blockedIps = blockedIps;
    snapshot.atCapacity = atCapacity;
    snapshot.allowedIps = allowedIps;
    snapshot.limitSupported = true;
    snapshotByAccountId.set(acc.id, snapshot);

    applyLocalAccountLimitChain(acc, previous.chain, allowedIps, atCapacity);
    if (blockedIps.length > 0) {
      disconnectLocalAccountIps(accountPorts, blockedIps);
    }
  }

  for (const [accountId, state] of [...localAccountLimitState.entries()]) {
    if (!activeAccountIds.has(accountId) || !accountsById.has(accountId)) {
      clearLocalAccountLimitStateEntry(accountId, state);
    }
  }
}

function getLocalAccountTunnelState(acc) {
  const assignedConfigId = getAssignedXrayConfigId(acc);
  if (!assignedConfigId) {
    return {
      assignedConfigId: null,
      running: null,
      outboundTag: null,
      label: localXraySettings.outbound === 'direct'
        ? 'Legacy default (direct)'
        : 'Legacy default (global proxy)',
    };
  }

  const cfg = getXrayConfigById(assignedConfigId);
  if (!cfg) {
    return {
      assignedConfigId,
      running: false,
      outboundTag: 'block',
      label: 'Missing assigned Xray tunnel',
    };
  }

  const session = xraySessions.get(assignedConfigId);
  if (!isManagedXraySessionActive(session)) {
    return {
      assignedConfigId,
      running: false,
      outboundTag: 'block',
      label: `${cfg.remark || assignedConfigId} (stopped)`,
    };
  }

  return {
    assignedConfigId,
    running: true,
    outboundTag: getXraySessionTag(assignedConfigId),
    label: cfg.remark || assignedConfigId,
  };
}

function splitLocalRoutePatterns(route) {
  const patterns = String(route && route.pattern || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
  const domainPatterns = [];
  const ipPatterns = [];

  if (route && route.type === 'ip') return { domainPatterns, ipPatterns: patterns };
  if (route && route.type === 'domain') return { domainPatterns: patterns, ipPatterns };

  for (const pattern of patterns) {
    if (pattern.startsWith('geoip:') || /^\d+\.\d+\.\d+/.test(pattern) || (pattern.includes('/') && /^\d/.test(pattern))) {
      ipPatterns.push(pattern);
    } else {
      domainPatterns.push(pattern);
    }
  }
  return { domainPatterns, ipPatterns };
}

function buildLocalXrayConfig() {
  const inbounds = [];
  const enabledAccounts = localAccounts.filter(acc => acc && acc.enabled);
  const accountStates = [];

  for (const acc of enabledAccounts) {
    const ports = getLocalAccountPorts(acc);
    if (!ports.length) continue;
    const email = `${acc.id}@local`;
    const inboundTags = [];
    for (const port of ports) {
      const inboundTag = `in-${acc.id}-${port}`;
      const inbound = {
        tag: inboundTag,
        listen: acc.listen || '0.0.0.0',
        port,
        protocol: acc.protocol,
        settings: {},
        streamSettings: { network: 'tcp', security: 'none' },
        sniffing: {
          enabled: true,
          destOverride: ['http', 'tls', 'quic'],
        },
      };
      if (acc.protocol === 'vless') {
        inbound.settings = { clients: [{ id: acc.uuid, flow: '', email }], decryption: 'none' };
      } else if (acc.protocol === 'vmess') {
        inbound.settings = { clients: [{ id: acc.uuid, alterId: 0, email }] };
      }
      inbounds.push(inbound);
      inboundTags.push(inboundTag);
    }
    accountStates.push({
      acc,
      inboundTags,
      tunnel: getLocalAccountTunnelState(acc),
    });
  }

  inbounds.push({
    tag: 'api-in',
    listen: '127.0.0.1',
    port: LOCAL_XRAY_API_PORT,
    protocol: 'dokodemo-door',
    settings: { address: '127.0.0.1' },
  });

  const outbounds = [];
  const ob = localXraySettings.outbound || 'direct';
  const hasSocksOutbound = ob !== 'direct' && ob.startsWith('socks:');
  const routesNeedProxy = (localXraySettings.routes || []).some(r => r.outboundTag === 'proxy');
  if (hasSocksOutbound) {
    const parts = ob.split(':');
    const socksHost = parts[1] || '127.0.0.1';
    const socksPort = parseInt(parts[2]) || 10808;
    outbounds.push({
      protocol: 'socks',
      tag: 'proxy',
      settings: { servers: [{ address: socksHost, port: socksPort }] },
    });
    outbounds.push({ protocol: 'freedom', tag: 'direct' });
  } else {
    outbounds.push({ protocol: 'freedom', tag: 'direct' });
    if (routesNeedProxy) {
      const socksHost = localXraySettings.socksFallbackHost || '127.0.0.1';
      const socksPort = localXraySettings.socksFallbackPort || 10808;
      outbounds.push({
        protocol: 'socks',
        tag: 'proxy',
        settings: { servers: [{ address: socksHost, port: socksPort }] },
      });
    }
  }
  for (const session of xraySessions.values()) {
    if (!isManagedXraySessionActive(session)) continue;
    outbounds.push({
      protocol: 'socks',
      tag: getXraySessionTag(session.configId),
      settings: {
        servers: [{
          address: getXraySessionConnectHost(session.localBindings.listenAddr),
          port: session.localBindings.socksPort,
        }],
      },
    });
  }
  outbounds.push({ protocol: 'blackhole', tag: 'block' });

  const config = {
    log: { loglevel: 'warning', ...(localXrayAccessLogPath ? { access: localXrayAccessLogPath } : {}) },
    stats: {},
    api: {
      tag: 'api',
      services: ['HandlerService', 'StatsService'],
    },
    policy: {
      levels: { 0: { statsUserUplink: true, statsUserDownlink: true } },
      system: { statsInboundUplink: true, statsInboundDownlink: true },
    },
    inbounds,
    outbounds,
  };

  const rules = [
    { inboundTag: ['api-in'], outboundTag: 'api', type: 'field' },
  ];

  const legacyInboundTags = accountStates
    .filter(state => !state.tunnel.assignedConfigId)
    .flatMap(state => state.inboundTags);

  const pushRouteRule = (outboundTag, inboundTags, domainPatterns, ipPatterns) => {
    if (domainPatterns.length > 0) {
      const rule = { outboundTag, domain: domainPatterns, type: 'field' };
      if (inboundTags && inboundTags.length > 0) rule.inboundTag = inboundTags;
      rules.push(rule);
    }
    if (ipPatterns.length > 0) {
      const rule = { outboundTag, ip: ipPatterns, type: 'field' };
      if (inboundTags && inboundTags.length > 0) rule.inboundTag = inboundTags;
      rules.push(rule);
    }
  };

  for (const route of localXraySettings.routes || []) {
    if (!route.pattern || !route.outboundTag) continue;
    const { domainPatterns, ipPatterns } = splitLocalRoutePatterns(route);
    if (domainPatterns.length === 0 && ipPatterns.length === 0) continue;

    if (route.outboundTag === 'proxy') {
      for (const state of accountStates) {
        if (!state.tunnel.assignedConfigId) continue;
        pushRouteRule(state.tunnel.outboundTag || 'block', state.inboundTags, domainPatterns, ipPatterns);
      }
      if (legacyInboundTags.length > 0) {
        pushRouteRule('proxy', legacyInboundTags, domainPatterns, ipPatterns);
      }
      continue;
    }

    pushRouteRule(route.outboundTag, null, domainPatterns, ipPatterns);
  }

  for (const state of accountStates) {
    if (!state.tunnel.assignedConfigId) continue;
    rules.push({
      inboundTag: state.inboundTags,
      outboundTag: state.tunnel.outboundTag || 'block',
      type: 'field',
    });
  }

  const domainStrategy = localXraySettings.domainStrategy || 'AsIs';
  config.routing = { domainStrategy, rules };

  return config;
}

function generateLink(acc) {
  const ip = acc.listen === '0.0.0.0' ? getPublicIp() : acc.listen;
  const port = getLocalAccountPrimaryPort(acc);
  if (acc.protocol === 'vless') {
    return `vless://${acc.uuid}@${ip}:${port}?encryption=none&type=tcp&security=none#${encodeURIComponent(acc.name)}`;
  } else if (acc.protocol === 'vmess') {
    const vmessObj = {
      v: '2', ps: acc.name, add: ip, port, id: acc.uuid,
      aid: 0, net: 'tcp', type: 'none', tls: '', scy: 'auto',
    };
    return 'vmess://' + Buffer.from(JSON.stringify(vmessObj)).toString('base64');
  }
  return '';
}

function getPublicIp() {
  const os = require('os');
  const ifaces = os.networkInterfaces();
  for (const list of Object.values(ifaces)) {
    for (const info of list) {
      if (info.family === 'IPv4' && !info.internal) return info.address;
    }
  }
  return '127.0.0.1';
}

// ─── Subscription endpoint (public, no auth) ────────────────────────
app.get('/sub/:uuid', (req, res) => {
  const acc = localAccounts.find(a => a.uuid === req.params.uuid);
  if (!acc) return res.status(404).send('Not found');

  const ua = (req.headers['user-agent'] || '').toLowerCase();
  const isClient = ua.includes('v2ray') || ua.includes('clash') || ua.includes('sing-box')
    || ua.includes('nekoray') || ua.includes('hiddify') || ua.includes('streisand')
    || ua.includes('v2box') || ua.includes('shadowrocket') || ua.includes('quantumult');

  if (isClient) {
    // V2Ray client — return base64 config links
    const link = generateLink(acc);
    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.setHeader('subscription-userinfo', `upload=0; download=${acc.usedTraffic || 0}; total=${acc.bandwidthLimit || 0}; expire=${acc.expiresAt || 0}`);
    res.send(Buffer.from(link).toString('base64'));
    return;
  }

  // Browser — render stats page
  const usedPct = acc.bandwidthLimit ? Math.min(100, ((acc.usedTraffic || 0) / acc.bandwidthLimit * 100)).toFixed(1) : 0;
  const os = userOnlineState[acc.id] || {};
  const daysLeft = acc.expiresAt ? Math.max(0, Math.ceil((acc.expiresAt - Date.now()) / 86400000)) : null;
  const link = generateLink(acc);
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(`<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${acc.name} - VPN Stats</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}body{font-family:system-ui,-apple-system,sans-serif;background:#06060b;color:#eee;min-height:100vh;display:flex;justify-content:center;padding:24px 16px}
.wrap{max-width:420px;width:100%}.card{background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.07);border-radius:14px;padding:20px;margin-bottom:12px}
h1{font-size:20px;font-weight:700;margin-bottom:16px;text-align:center}
.stat{display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid rgba(255,255,255,.06);font-size:14px}
.stat:last-child{border:none}.label{color:rgba(255,255,255,.45)}.val{font-weight:600}
.bar-wrap{background:rgba(255,255,255,.06);border-radius:8px;height:8px;margin-top:12px;overflow:hidden}
.bar-fill{height:100%;border-radius:8px;background:linear-gradient(90deg,#7c7ff9,#a78bfa);transition:width .3s}
.tag{display:inline-block;padding:2px 10px;border-radius:6px;font-size:12px;font-weight:600}
.tag-on{background:rgba(52,211,153,.12);color:#34d399}.tag-off{background:rgba(255,255,255,.06);color:rgba(255,255,255,.3)}
.tag-expired{background:rgba(248,113,113,.12);color:#f87171}
.link-box{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);border-radius:10px;padding:12px;margin-top:12px;word-break:break-all;font-family:monospace;font-size:11px;color:rgba(255,255,255,.5);cursor:pointer;position:relative}
.link-box:active{background:rgba(124,127,249,.1)}.copy-hint{text-align:center;font-size:11px;color:rgba(255,255,255,.25);margin-top:4px}
.qr{text-align:center;margin-top:16px}
</style></head><body><div class="wrap">
<h1>${escHtml(acc.name)}</h1>
<div class="card">
  <div class="stat"><span class="label">Status</span><span class="val">${acc.enabled ? (os.online ? '<span class="tag tag-on">Online</span>' : '<span class="tag tag-off">Offline</span>') : '<span class="tag tag-expired">Disabled</span>'}</span></div>
  <div class="stat"><span class="label">Protocol</span><span class="val">${acc.protocol.toUpperCase()}</span></div>
  <div class="stat"><span class="label">Connections</span><span class="val">${os.activeUniqueIps || 0} IPs / ${os.activeSessions || 0} sessions</span></div>
  <div class="stat"><span class="label">Traffic Used</span><span class="val">${formatBytes(acc.usedTraffic || 0)}</span></div>
  <div class="stat"><span class="label">Traffic Limit</span><span class="val">${acc.bandwidthLimit ? formatBytes(acc.bandwidthLimit) : 'Unlimited'}</span></div>
  ${acc.bandwidthLimit ? `<div class="bar-wrap"><div class="bar-fill" style="width:${usedPct}%"></div></div>` : ''}
  ${acc.maxConnections ? `<div class="stat"><span class="label">Max Unique IPs</span><span class="val">${acc.maxConnections}</span></div>` : ''}
  <div class="stat"><span class="label">Expires</span><span class="val">${acc.expiresAt ? (daysLeft > 0 ? daysLeft + ' days left' : '<span class="tag tag-expired">Expired</span>') : 'Never'}</span></div>
  ${os.online ? `<div class="stat"><span class="label">Speed</span><span class="val">${os.speedFormatted || '--'}</span></div>` : ''}
</div>
<div class="card">
  <div style="font-size:13px;font-weight:600;margin-bottom:8px">Subscription Link</div>
  <div class="link-box" onclick="navigator.clipboard.writeText(location.href).then(()=>this.style.background='rgba(52,211,153,.1)')">
    ${escHtml(req.protocol + '://' + req.get('host') + req.originalUrl)}
  </div>
  <div class="copy-hint">Tap to copy — paste in your V2Ray/Clash client</div>
</div>
<div class="card">
  <div style="font-size:13px;font-weight:600;margin-bottom:8px">Config Link</div>
  <div class="link-box" onclick="navigator.clipboard.writeText(this.dataset.link).then(()=>this.style.background='rgba(52,211,153,.1)')" data-link="${escHtml(link)}">
    ${escHtml(link)}
  </div>
  <div class="copy-hint">Tap to copy</div>
</div>
</div></body></html>`);
});

function escHtml(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }

function buildLocalAccountApiShape(acc) {
  const os = userOnlineState[acc.id] || {};
  const tunnel = getLocalAccountTunnelState(acc);
  const ports = getLocalAccountPorts(acc);
  return {
    ...acc,
    port: getLocalAccountPrimaryPort(acc),
    ports,
    portDisplay: ports.join(', '),
    assignedXrayConfigId: getAssignedXrayConfigId(acc),
    link: generateLink(acc),
    usedTrafficFormatted: formatBytes(acc.usedTraffic || 0),
    bandwidthLimitFormatted: acc.bandwidthLimit ? formatBytes(acc.bandwidthLimit) : 'Unlimited',
    online: !!os.online,
    speed: os.speedFormatted || '',
    activeConnections: os.activeUniqueIps || os.connections || 0,
    activeUniqueIps: os.activeUniqueIps || 0,
    activeSessions: os.activeSessions || 0,
    activeIps: os.activeIps || [],
    blockedIps: os.blockedIps || [],
    atCapacity: !!os.atCapacity,
    ipLimitSupported: !!os.limitSupported,
    assignedXrayRunning: tunnel.running,
    effectiveTunnelLabel: tunnel.label,
  };
}

app.get('/api/local-accounts', (req, res) => {
  res.json(localAccounts.map(buildLocalAccountApiShape));
});

function randomOpenPort() {
  const usedPorts = new Set(localAccounts.flatMap(acc => getLocalAccountPorts(acc)));
  for (let i = 0; i < 100; i++) {
    const p = 10000 + Math.floor(Math.random() * 30000); // 10000-39999
    if (!usedPorts.has(p)) {
      try { const s = net.createServer(); s.listen(p, () => s.close()); return p; } catch {}
    }
  }
  return 10000 + Math.floor(Math.random() * 30000);
}

app.get('/api/local-accounts/random-port', (req, res) => {
  res.json({ port: randomOpenPort() });
});

app.post('/api/local-accounts', (req, res) => {
  const { name, protocol, port, listen, bandwidthLimit, maxConnections, expiresAt, assignedXrayConfigId } = req.body;
  if (!name || !protocol) return res.status(400).json({ error: 'name, protocol required' });
  if (!['vless', 'vmess'].includes(protocol)) return res.status(400).json({ error: 'protocol must be vless or vmess' });
  const requestedPorts = buildRequestedLocalAccountPorts(req.body);
  const finalPort = requestedPorts[0] || randomOpenPort();
  const acc = {
    id: crypto.randomUUID(),
    name,
    protocol,
    port: finalPort,
    listen: listen || '0.0.0.0',
    uuid: crypto.randomUUID(),
    enabled: true,
    createdAt: Date.now(),
    // Limits
    bandwidthLimit: bandwidthLimit ? parseInt(bandwidthLimit) : 0,  // bytes, 0 = unlimited
    usedTraffic: 0,          // bytes (cumulative up + down)
    maxConnections: maxConnections ? parseInt(maxConnections) : 0,  // 0 = unlimited
    expiresAt: expiresAt || 0,  // epoch ms, 0 = never
    disabledReason: '',      // 'bandwidth' | 'expired' | '' (manual)
    assignedXrayConfigId: String(assignedXrayConfigId || '').trim() || null,
  };
  if (requestedPorts.length > 1 || hasExplicitLocalAccountPortList(req.body)) {
    acc.ports = requestedPorts.length ? requestedPorts : [finalPort];
  }
  localAccounts.push(acc);
  saveLocalAccounts();
  if (localXrayProc) restartLocalXray();
  res.json({ ok: true, account: buildLocalAccountApiShape(acc) });
});

app.put('/api/local-accounts/:id', (req, res) => {
  const acc = localAccounts.find(a => a.id === req.params.id);
  if (!acc) return res.status(404).json({ error: 'Not found' });
  const { name, port, listen, enabled, bandwidthLimit, maxConnections, expiresAt, assignedXrayConfigId } = req.body;
  if (name !== undefined) acc.name = name;
  if (port !== undefined) {
    const normalizedPort = normalizeTcpPort(port);
    if (normalizedPort) acc.port = normalizedPort;
  }
  if (hasExplicitLocalAccountPortList(req.body)) {
    const requestedPorts = buildRequestedLocalAccountPorts(req.body);
    if (requestedPorts.length > 0) {
      acc.port = requestedPorts[0];
      acc.ports = requestedPorts;
    } else {
      delete acc.ports;
    }
  }
  if (listen !== undefined) acc.listen = listen;
  if (enabled !== undefined) {
    acc.enabled = !!enabled;
    if (acc.enabled) acc.disabledReason = '';
  }
  if (bandwidthLimit !== undefined) acc.bandwidthLimit = parseInt(bandwidthLimit) || 0;
  if (maxConnections !== undefined) acc.maxConnections = parseInt(maxConnections) || 0;
  if (expiresAt !== undefined) acc.expiresAt = expiresAt || 0;
  if (assignedXrayConfigId !== undefined) {
    const value = String(assignedXrayConfigId || '').trim();
    acc.assignedXrayConfigId = value || null;
  }
  saveLocalAccounts();
  if (localXrayProc) restartLocalXray();
  res.json({ ok: true, account: buildLocalAccountApiShape(acc) });
});

app.post('/api/local-accounts/reset-traffic/:id', (req, res) => {
  const acc = localAccounts.find(a => a.id === req.params.id);
  if (!acc) return res.status(404).json({ error: 'Not found' });
  acc.usedTraffic = 0;
  if (acc.disabledReason === 'bandwidth') {
    acc.enabled = true;
    acc.disabledReason = '';
  }
  saveLocalAccounts();
  if (localXrayProc) restartLocalXray();
  res.json({ ok: true });
});

app.delete('/api/local-accounts/:id', (req, res) => {
  localAccounts = localAccounts.filter(a => a.id !== req.params.id);
  saveLocalAccounts();
  if (localXrayProc) restartLocalXray();
  res.json({ ok: true });
});

app.post('/api/local-accounts/regenerate-uuid/:id', (req, res) => {
  const acc = localAccounts.find(a => a.id === req.params.id);
  if (!acc) return res.status(404).json({ error: 'Not found' });
  acc.uuid = crypto.randomUUID();
  saveLocalAccounts();
  if (localXrayProc) restartLocalXray();
  res.json({ ok: true, account: buildLocalAccountApiShape(acc) });
});

app.get('/api/local-xray/settings', (req, res) => {
  res.json(localXraySettings);
});

app.post('/api/local-xray/settings', (req, res) => {
  const { outbound, domainStrategy } = req.body;
  if (outbound !== undefined) localXraySettings.outbound = outbound;
  if (domainStrategy !== undefined) localXraySettings.domainStrategy = domainStrategy;
  saveLocalXraySettings();
  if (localXrayProc) restartLocalXray();
  res.json({ ok: true, settings: localXraySettings });
});

app.put('/api/local-xray/settings', (req, res) => {
  const { outbound, domainStrategy } = req.body;
  if (outbound !== undefined) localXraySettings.outbound = outbound;
  if (domainStrategy !== undefined) localXraySettings.domainStrategy = domainStrategy;
  saveLocalXraySettings();
  if (localXrayProc) restartLocalXray();
  res.json({ ok: true, settings: localXraySettings });
});

// Routing rules CRUD
app.get('/api/local-xray/routes', (req, res) => {
  res.json(localXraySettings.routes || []);
});

app.post('/api/local-xray/routes', (req, res) => {
  const { type, pattern, outboundTag } = req.body;
  if (!type || !pattern || !outboundTag) return res.status(400).json({ error: 'type, pattern, outboundTag required' });
  if (!['domain', 'ip'].includes(type)) return res.status(400).json({ error: 'type must be domain or ip' });
  if (!['direct', 'proxy', 'block'].includes(outboundTag)) return res.status(400).json({ error: 'outboundTag must be direct, proxy, or block' });
  if (!localXraySettings.routes) localXraySettings.routes = [];
  const rule = { id: crypto.randomUUID(), type, pattern, outboundTag };
  localXraySettings.routes.push(rule);
  saveLocalXraySettings();
  if (localXrayProc) restartLocalXray();
  res.json({ ok: true, rule });
});

app.put('/api/local-xray/routes/:id', (req, res) => {
  if (!localXraySettings.routes) return res.status(404).json({ error: 'Not found' });
  const rule = localXraySettings.routes.find(r => r.id === req.params.id);
  if (!rule) return res.status(404).json({ error: 'Not found' });
  const { type, pattern, outboundTag } = req.body;
  if (type !== undefined) rule.type = type;
  if (pattern !== undefined) rule.pattern = pattern;
  if (outboundTag !== undefined) rule.outboundTag = outboundTag;
  saveLocalXraySettings();
  if (localXrayProc) restartLocalXray();
  res.json({ ok: true, rule });
});

app.delete('/api/local-xray/routes/:id', (req, res) => {
  if (!localXraySettings.routes) localXraySettings.routes = [];
  localXraySettings.routes = localXraySettings.routes.filter(r => r.id !== req.params.id);
  saveLocalXraySettings();
  if (localXrayProc) restartLocalXray();
  res.json({ ok: true });
});

app.get('/api/local-xray/status', (req, res) => {
  res.json({ running: !!localXrayProc, binary: !!findXrayBinary(), accounts: localAccounts.filter(a => a.enabled).length, outbound: localXraySettings.outbound });
});

app.post('/api/local-xray/start', async (req, res) => {
  if (localXrayProc) return res.status(400).json({ ok: false, error: 'Already running' });
  const enabled = localAccounts.filter(a => a.enabled);
  if (!enabled.length) return res.status(400).json({ ok: false, error: 'No enabled accounts' });
  const xrayBin = findXrayBinary();
  if (!xrayBin) return res.status(500).json({ ok: false, error: 'xray binary not found' });

  try {
    await ensureVpnRoutesForHosts(getLocalXrayOutboundRouteHosts(), 'Local Xray upstream via OpenVPN');
  } catch (e) {
    addOvpnLog(`[route] Local Xray route preparation failed: ${e.message}`);
  }

  localXrayAccessLogPath = path.join(os.tmpdir(), `xray-access-${Date.now()}.log`);
  const config = buildLocalXrayConfig();
  const configPath = path.join(__dirname, '.local-xray-run.json');
  fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
  console.log('[Local Xray] Config written to', configPath);
  console.log('[Local Xray] Inbounds:', config.inbounds.length, 'Outbounds:', config.outbounds.length);

  let proc;
  try {
    proc = spawnXray(xrayBin, configPath);
  } catch (err) {
    console.error('[Local Xray] Spawn error:', err.message);
    localXrayAccessLogPath = null;
    return res.status(500).json({ ok: false, error: `Failed to spawn xray: ${err.message}` });
  }
  localXrayProc = proc;
  localXrayLogs = [];
  startXrayAccessLogWatcher(localXrayAccessLogPath);
  const pushLog = (line) => { localXrayLogs.push({ t: Date.now(), m: line }); if (localXrayLogs.length > 500) localXrayLogs.shift(); };
  proc.stderr.on('data', d => d.toString().trim().split('\n').forEach(l => { if (l) { pushLog(l); console.log(`[Local Xray] ${l}`); } }));
  proc.stdout.on('data', d => d.toString().trim().split('\n').forEach(l => { if (l) { pushLog(l); console.log(`[Local Xray] ${l}`); } }));

  proc.on('error', (err) => {
    pushLog(`[spawn error] ${err.message}`);
    if (localXrayProc !== proc) return;
    stopTrafficPolling();
    stopXrayAccessLogWatcher();
    localXrayProc = null;
    refreshVpnRoutes().catch(() => {});
  });

  proc.on('exit', (code) => {
    pushLog(`[process exited code=${code}]`);
    if (localXrayProc !== proc) return;
    stopTrafficPolling();
    stopXrayAccessLogWatcher();
    localXrayProc = null;
    refreshVpnRoutes().catch(() => {});
  });

  setTimeout(() => {
    if (proc.exitCode !== null) {
      const recentLogs = localXrayLogs.map(l => l.m).join('\n');
      res.status(500).json({ ok: false, error: 'Xray exited immediately', logs: recentLogs });
    } else {
      startTrafficPolling();
      res.json({ ok: true, pid: proc.pid });
    }
  }, 800);
});

app.post('/api/local-xray/stop', (req, res) => {
  stopTrafficPolling();
  stopXrayAccessLogWatcher();
  if (!localXrayProc) return res.json({ ok: true });
  localXrayProc.kill('SIGTERM');
  localXrayProc = null;
  refreshVpnRoutes().catch(() => {});
  res.json({ ok: true });
});

app.post('/api/local-xray/restart', (req, res) => {
  if (localXrayProc) {
    clearAllLocalAccountLimitState();
    localXrayProc.kill('SIGTERM');
    localXrayProc = null;
  }
  stopXrayAccessLogWatcher();
  refreshVpnRoutes().catch(() => {});
  setTimeout(async () => {
    const enabled = localAccounts.filter(a => a.enabled);
    if (!enabled.length) return res.json({ ok: true, restarted: false });
    const xrayBin = findXrayBinary();
    if (!xrayBin) return res.status(500).json({ ok: false, error: 'xray binary not found' });

    try {
      await ensureVpnRoutesForHosts(getLocalXrayOutboundRouteHosts(), 'Local Xray upstream via OpenVPN');
    } catch (e) {
      addOvpnLog(`[route] Local Xray restart route preparation failed: ${e.message}`);
    }

    localXrayAccessLogPath = path.join(os.tmpdir(), `xray-access-${Date.now()}.log`);
    const config = buildLocalXrayConfig();
    const configPath = path.join(__dirname, '.local-xray-run.json');
    fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

    let proc;
    try {
      proc = spawnXray(xrayBin, configPath);
    } catch (err) {
      localXrayAccessLogPath = null;
      return res.status(500).json({ ok: false, error: `Failed to spawn xray: ${err.message}` });
    }
    localXrayProc = proc;
    localXrayLogs = [];
    startXrayAccessLogWatcher(localXrayAccessLogPath);
    const pushLog = (line) => { localXrayLogs.push({ t: Date.now(), m: line }); if (localXrayLogs.length > 500) localXrayLogs.shift(); console.log(`[Local Xray] ${line}`); };
    proc.stderr.on('data', d => d.toString().trim().split('\n').forEach(l => { if (l) pushLog(l); }));
    proc.stdout.on('data', d => d.toString().trim().split('\n').forEach(l => { if (l) pushLog(l); }));

    proc.on('error', (err) => {
      pushLog(`[spawn error] ${err.message}`);
      if (localXrayProc !== proc) return;
      stopTrafficPolling();
      stopXrayAccessLogWatcher();
      localXrayProc = null;
      refreshVpnRoutes().catch(() => {});
    });

    proc.on('exit', (code) => {
      pushLog(`[process exited code=${code}]`);
      if (localXrayProc !== proc) return;
      stopTrafficPolling();
      stopXrayAccessLogWatcher();
      localXrayProc = null;
      refreshVpnRoutes().catch(() => {});
    });

    setTimeout(() => {
      if (proc.exitCode !== null) {
        const recentLogs = localXrayLogs.map(l => l.m).join('\n');
        res.status(500).json({ ok: false, error: 'Xray exited on restart', logs: recentLogs });
      } else {
        startTrafficPolling();
        res.json({ ok: true, restarted: true, pid: proc.pid });
      }
    }, 800);
  }, 300);
});

app.get('/api/local-xray/logs', (req, res) => {
  res.json(localXrayLogs);
});

app.get('/api/local-xray/access-log', (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 500, 2000);
  const accountsById = new Map(localAccounts.map(a => [a.id, a]));
  res.json(localAccessLog.slice(-limit).map(entry => ({
    ...entry,
    accountName: entry.accountName || (entry.accountId ? (accountsById.get(entry.accountId)?.name || null) : null),
  })));
});

app.delete('/api/local-xray/access-log', (req, res) => {
  localAccessLog = [];
  localClientHistory = new Map();
  res.json({ ok: true });
});

app.get('/api/local-xray/client-stats', (req, res) => {
  const accountsById = new Map(localAccounts.map(a => [a.id, a]));
  const clients = [];
  for (const [ip, hist] of localClientHistory) {
    const acc = hist.accountId ? accountsById.get(hist.accountId) : null;
    clients.push({
      ip,
      accountId: hist.accountId || null,
      accountName: acc ? acc.name : (hist.accountName || hist.accountId || null),
      firstSeen: hist.firstSeen,
      lastSeen: hist.lastSeen,
      totalAccesses: hist.totalAccesses,
      recentDests: hist.accesses.slice(-20).reverse().map(a => ({ t: a.t, dest: a.dest, outbound: a.outbound })),
    });
  }
  // Merge with current active IPs that may not have access log entries yet
  const activeIpSet = new Set(clients.map(c => c.ip));
  for (const acc of localAccounts) {
    const os = userOnlineState[acc.id] || {};
    for (const ip of (os.activeIps || [])) {
      if (!activeIpSet.has(ip)) {
        clients.push({ ip, accountId: acc.id, accountName: acc.name, firstSeen: null, lastSeen: null, totalAccesses: 0, recentDests: [] });
        activeIpSet.add(ip);
      }
    }
  }
  clients.sort((a, b) => (b.lastSeen || 0) - (a.lastSeen || 0));
  res.json(clients);
});

// ─── Traffic Polling & Limit Enforcement ────────────────────────────
let trafficPollInterval = null;
const POLL_INTERVAL = 10000; // 10s
let userOnlineState = {}; // accId -> { online, bytesInPeriod, speed }

function startTrafficPolling() {
  if (trafficPollInterval) return;
  trafficPollInterval = setInterval(pollTrafficStats, POLL_INTERVAL);
  pollTrafficStats();
}

function stopTrafficPolling() {
  if (trafficPollInterval) { clearInterval(trafficPollInterval); trafficPollInterval = null; }
  clearAllLocalAccountLimitState();
  userOnlineState = {};
}

function pollTrafficStats() {
  if (!localXrayProc) return;
  const xrayBin = findXrayBinary();
  if (!xrayBin) return;

  const server = `127.0.0.1:${LOCAL_XRAY_API_PORT}`;
  const enabledAccs = localAccounts.filter(a => a.enabled);
  const connectionSnapshot = buildLocalAccountConnectionSnapshot(enabledAccs);
  const fallbackSessionCounts = new Map();

  // 1. Query traffic stats (with reset)
  try {
    const result = execSync(
      `"${xrayBin}" api statsquery -s "${server}" -reset 2>&1`,
      { timeout: 5000, encoding: 'utf8' }
    );
    let stats;
    try { stats = JSON.parse(result); } catch {
      // Might be empty or error text — log once for debugging
      if (result.trim() && !result.includes('dial')) {
        console.log('[Stats] Raw output:', result.substring(0, 200));
      }
      stats = null;
    }

    const periodBytes = {};
    let changed = false;

    if (stats && stats.stat) {
      for (const s of stats.stat) {
        if (!s.name) continue;
        const val = parseInt(s.value) || 0;
        if (val <= 0) continue;
        const parts = s.name.split('>>>');
        if (parts[0] !== 'user' || parts[2] !== 'traffic') continue;
        const email = parts[1];
        const accId = email.replace('@local', '');
        const direction = parts[3]; // 'uplink' or 'downlink'

        // Count both directions for speed display
        periodBytes[accId] = (periodBytes[accId] || 0) + val;

        // Count only downlink (download) toward quota — upload is negligible for most users
        if (direction === 'downlink') {
          const acc = localAccounts.find(a => a.id === accId);
          if (acc) {
            acc.usedTraffic = (acc.usedTraffic || 0) + val;
            changed = true;
          }
        }
      }
    }

    // Update speed for all accounts based on traffic in this period
    for (const acc of localAccounts) {
      const pb = periodBytes[acc.id] || 0;
      const speed = pb > 0 ? pb / (POLL_INTERVAL / 1000) : 0;
      if (!userOnlineState[acc.id]) userOnlineState[acc.id] = {};
      userOnlineState[acc.id].bytesInPeriod = pb;
      userOnlineState[acc.id].speed = speed;
      userOnlineState[acc.id].speedFormatted = speed > 0 ? formatSpeed(speed) : '';
    }

    if (changed) saveLocalAccounts();
    enforceAccountLimits();
  } catch (e) {
    // silently ignore — xray might not be ready yet
  }

  // 2. Query online users — keep statsonline as a fallback when IP/session discovery is unavailable.
  for (const acc of localAccounts) {
    if (!userOnlineState[acc.id]) userOnlineState[acc.id] = {};
  }

  if (connectionSnapshot.source === 'none') {
    for (const acc of enabledAccs) {
      const email = `${acc.id}@local`;
      try {
        const raw = execSync(
          `"${xrayBin}" api statsonline -s "${server}" -email "${email}" 2>&1`,
          { timeout: 3000, encoding: 'utf8' }
        );
        const trimmed = raw.trim();
        let count = 0;

        try {
          const parsed = JSON.parse(trimmed);
          count = typeof parsed === 'number' ? parsed
            : parsed.count ?? parsed.Count ?? parsed.value ?? 0;
        } catch {
          const m = trimmed.match(/(\d+)/);
          count = m ? parseInt(m[1], 10) : 0;
        }

        if (!pollTrafficStats._loggedOnline) {
          console.log(`[Stats] statsonline raw for ${email}: "${trimmed}" => count=${count}`);
          pollTrafficStats._loggedOnline = true;
        }

        fallbackSessionCounts.set(acc.id, count);
      } catch (e) {
        fallbackSessionCounts.set(acc.id, 0);
      }
    }
  }

  if (connectionSnapshot.limitSupported) {
    syncLocalAccountIpLimits(connectionSnapshot.byAccountId);
  } else {
    clearAllLocalAccountLimitState();
  }

  for (const acc of enabledAccs) {
    const state = userOnlineState[acc.id];
    const snapshot = connectionSnapshot.byAccountId.get(acc.id);
    const activeSessions = snapshot
      ? snapshot.activeSessions
      : (fallbackSessionCounts.get(acc.id) || 0);
    const activeUniqueIps = snapshot
      ? snapshot.activeUniqueIps
      : activeSessions;

    state.activeSessions = activeSessions;
    state.activeUniqueIps = activeUniqueIps;
    state.activeIps = snapshot ? (snapshot.activeIps || []) : [];
    state.blockedIps = snapshot ? (snapshot.blockedIps || []) : [];
    state.atCapacity = snapshot ? !!snapshot.atCapacity : !!(acc.maxConnections > 0 && activeUniqueIps >= acc.maxConnections);
    state.limitSupported = snapshot ? !!snapshot.limitSupported : false;
    state.online = activeSessions > 0 || (state.bytesInPeriod || 0) > 0;
    state.connections = activeUniqueIps;
  }

  // Mark disabled accounts as offline
  for (const acc of localAccounts) {
    if (!acc.enabled) {
      userOnlineState[acc.id].online = false;
      userOnlineState[acc.id].connections = 0;
      userOnlineState[acc.id].activeSessions = 0;
      userOnlineState[acc.id].activeUniqueIps = 0;
      userOnlineState[acc.id].activeIps = [];
      userOnlineState[acc.id].blockedIps = [];
      userOnlineState[acc.id].atCapacity = false;
      userOnlineState[acc.id].limitSupported = false;
    }
  }
}

function formatSpeed(bps) {
  if (bps > 1024 * 1024) return (bps / (1024 * 1024)).toFixed(1) + ' MB/s';
  if (bps > 1024) return (bps / 1024).toFixed(0) + ' KB/s';
  return Math.round(bps) + ' B/s';
}

function enforceAccountLimits() {
  let needsRestart = false;
  const now = Date.now();
  for (const acc of localAccounts) {
    if (!acc.enabled) continue;

    // Check bandwidth limit
    if (acc.bandwidthLimit > 0 && acc.usedTraffic >= acc.bandwidthLimit) {
      acc.enabled = false;
      acc.disabledReason = 'bandwidth';
      needsRestart = true;
      console.log(`[Limits] Account "${acc.name}" disabled: bandwidth limit reached (${formatBytes(acc.usedTraffic)} / ${formatBytes(acc.bandwidthLimit)})`);
    }

    // Check expiration
    if (acc.expiresAt > 0 && now >= acc.expiresAt) {
      acc.enabled = false;
      acc.disabledReason = 'expired';
      needsRestart = true;
      console.log(`[Limits] Account "${acc.name}" disabled: expired`);
    }
  }
  if (needsRestart) {
    saveLocalAccounts();
    restartLocalXray();
  }
}

function formatBytes(b) {
  if (!b) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(b) / Math.log(1024));
  return (b / Math.pow(1024, i)).toFixed(1) + ' ' + units[i];
}

function restartLocalXray() {
  if (!localXrayProc) return;
  clearAllLocalAccountLimitState();
  localXrayProc.kill('SIGTERM');
  localXrayProc = null;
  refreshVpnRoutes().catch(() => {});
  setTimeout(async () => {
    const enabled = localAccounts.filter(a => a.enabled);
    if (!enabled.length) { stopTrafficPolling(); return; }
    const xrayBin = findXrayBinary();
    if (!xrayBin) return;

    try {
      await ensureVpnRoutesForHosts(getLocalXrayOutboundRouteHosts(), 'Local Xray upstream via OpenVPN');
    } catch (e) {
      addOvpnLog(`[route] Local Xray auto-restart route preparation failed: ${e.message}`);
    }

    localXrayAccessLogPath = path.join(os.tmpdir(), `xray-access-${Date.now()}.log`);
    const config = buildLocalXrayConfig();
    const configPath = path.join(__dirname, '.local-xray-run.json');
    fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

    let proc;
    try { proc = spawnXray(xrayBin, configPath); } catch { localXrayAccessLogPath = null; return; }
    localXrayProc = proc;
    startXrayAccessLogWatcher(localXrayAccessLogPath);
    const pushLog = (line) => { localXrayLogs.push({ t: Date.now(), m: line }); if (localXrayLogs.length > 500) localXrayLogs.shift(); };
    proc.stderr.on('data', d => d.toString().trim().split('\n').forEach(l => { if (l) pushLog(l); }));
    proc.stdout.on('data', d => d.toString().trim().split('\n').forEach(l => { if (l) pushLog(l); }));
    proc.on('error', (err) => {
      pushLog(`[spawn error] ${err.message}`);
      if (localXrayProc !== proc) return;
      stopTrafficPolling();
      stopXrayAccessLogWatcher();
      localXrayProc = null;
      refreshVpnRoutes().catch(() => {});
    });
    proc.on('exit', (code) => {
      pushLog(`[process exited code=${code}]`);
      if (localXrayProc !== proc) return;
      stopTrafficPolling();
      stopXrayAccessLogWatcher();
      localXrayProc = null;
      refreshVpnRoutes().catch(() => {});
    });
    startTrafficPolling();
  }, 500);
}

// Also check expiration every 60s even if xray isn't running (for time-based expiry)
setInterval(() => {
  const now = Date.now();
  let changed = false;
  for (const acc of localAccounts) {
    if (!acc.enabled) continue;
    if (acc.expiresAt > 0 && now >= acc.expiresAt) {
      acc.enabled = false;
      acc.disabledReason = 'expired';
      changed = true;
      console.log(`[Limits] Account "${acc.name}" disabled: expired`);
    }
  }
  if (changed) {
    saveLocalAccounts();
    if (localXrayProc) restartLocalXray();
  }
}, 60000);

// ─── Xray Client Tunnel & Scanner ───────────────────────────────────
// Xray instance #2: client tunnel — connects to remote server, opens local SOCKS/HTTP
const XRAY_CONFIGS_FILE = path.join(__dirname, 'xray-local-configs.json');
let xrayLocalConfigs = [];
let xraySessions = new Map();
let xrayLogs = [];

const XRAY_BINDINGS_DEFAULTS = {
  listenAddr: '127.0.0.1',
  socksPort: 10808,
  httpPort: 10809,
};

function normalizeXrayConfigBindings(bindings, fallback = XRAY_BINDINGS_DEFAULTS) {
  const listenAddr = String(bindings && bindings.listenAddr || fallback.listenAddr || XRAY_BINDINGS_DEFAULTS.listenAddr).trim() || XRAY_BINDINGS_DEFAULTS.listenAddr;
  const socksPort = parseInt(bindings && bindings.socksPort, 10);
  const httpPort = parseInt(bindings && bindings.httpPort, 10);
  return {
    listenAddr,
    socksPort: Number.isInteger(socksPort) && socksPort > 0 ? socksPort : (fallback.socksPort || XRAY_BINDINGS_DEFAULTS.socksPort),
    httpPort: Number.isInteger(httpPort) && httpPort >= 0 ? httpPort : (fallback.httpPort ?? XRAY_BINDINGS_DEFAULTS.httpPort),
  };
}

function getNextSuggestedXrayBindingPorts(usedPorts) {
  let socksPort = XRAY_BINDINGS_DEFAULTS.socksPort;
  while (usedPorts.has(socksPort) || usedPorts.has(socksPort + 1)) {
    socksPort += 2;
  }
  return { socksPort, httpPort: socksPort + 1 };
}

function getEffectiveXrayConfigBindingsMap(configs = xrayLocalConfigs) {
  const map = new Map();
  const usedPorts = new Set();

  for (const cfg of configs || []) {
    if (!cfg || !cfg.id || !cfg.localBindings) continue;
    const normalized = normalizeXrayConfigBindings(cfg.localBindings);
    map.set(cfg.id, normalized);
    usedPorts.add(normalized.socksPort);
    if (normalized.httpPort > 0) usedPorts.add(normalized.httpPort);
  }

  for (const cfg of configs || []) {
    if (!cfg || !cfg.id || map.has(cfg.id)) continue;
    const suggested = getNextSuggestedXrayBindingPorts(usedPorts);
    const normalized = {
      listenAddr: XRAY_BINDINGS_DEFAULTS.listenAddr,
      socksPort: suggested.socksPort,
      httpPort: suggested.httpPort,
    };
    map.set(cfg.id, normalized);
    usedPorts.add(normalized.socksPort);
    if (normalized.httpPort > 0) usedPorts.add(normalized.httpPort);
  }

  return map;
}

function getEffectiveXrayConfigBindings(cfg) {
  if (!cfg || !cfg.id) return { ...XRAY_BINDINGS_DEFAULTS };
  return getEffectiveXrayConfigBindingsMap().get(cfg.id) || { ...XRAY_BINDINGS_DEFAULTS };
}

function getXrayConfigRuntimePath(configId) {
  return path.join(__dirname, `.xray-local-run-${safeIdFragment(configId)}.json`);
}

function getXrayConfigById(configId) {
  return xrayLocalConfigs.find(cfg => cfg.id === configId) || null;
}

function getXraySessionTag(configId) {
  return `xray-session-${configId}`;
}

function getXraySessionConnectHost(listenAddr) {
  return !listenAddr || listenAddr === '0.0.0.0' ? '127.0.0.1' : listenAddr;
}

function getAssignedXrayConfigId(acc) {
  const value = String(acc && acc.assignedXrayConfigId || '').trim();
  return value || null;
}

function loadXrayLocalConfigs() {
  try {
    if (fs.existsSync(XRAY_CONFIGS_FILE)) {
      xrayLocalConfigs = JSON.parse(fs.readFileSync(XRAY_CONFIGS_FILE, 'utf8'));
      for (const cfg of xrayLocalConfigs) {
        const ob = cfg.outbound;
        if (ob && ob.protocol === 'vless' && ob.settings && ob.settings.address && !ob.settings.vnext) {
          const { address, port, id, flow, encryption } = ob.settings;
          ob.settings = { vnext: [{ address, port, users: [{ id, flow: flow || '', encryption: encryption || 'none' }] }] };
        }
        cfg.routeMode = normalizeXrayRouteMode(cfg.routeMode);
        cfg.routeSocks = normalizeXrayRouteSocks(cfg.routeSocks);
      }
    }
  } catch {}
}
function saveXrayLocalConfigs() {
  ensureAdditiveJsonBackup(
    XRAY_CONFIGS_FILE,
    'xray-local-configs-bindings',
    xrayLocalConfigs.some(cfg => Object.prototype.hasOwnProperty.call(cfg || {}, 'localBindings'))
  );
  fs.writeFileSync(XRAY_CONFIGS_FILE, JSON.stringify(xrayLocalConfigs, null, 2));
}
loadXrayLocalConfigs();

function getXraySessionPublicState(session) {
  const cfg = getXrayConfigById(session.configId);
  return {
    configId: session.configId,
    pid: session.proc && session.proc.pid ? session.proc.pid : null,
    socksPort: session.localBindings.socksPort,
    httpPort: session.localBindings.httpPort,
    listenAddr: session.localBindings.listenAddr,
    routeMode: cfg ? normalizeXrayRouteMode(cfg.routeMode) : 'direct',
    routeSocks: cfg ? normalizeXrayRouteSocks(cfg.routeSocks) : getDefaultXrayRouteSocks(),
    startedAt: session.startedAt || null,
  };
}

function appendXrayLog(configId, line) {
  const cfg = getXrayConfigById(configId);
  const prefix = cfg ? cfg.remark : configId;
  const entry = { t: Date.now(), configId, m: `[${prefix}] ${line}` };
  xrayLogs.push(entry);
  if (xrayLogs.length > 1000) xrayLogs.shift();
}

function isManagedXraySessionActive(session) {
  return !!(session && !session.stopping);
}

// --- V2Ray link parser ---
function parseV2RayLink(link) {
  link = link.trim();
  if (link.startsWith('vless://')) return parseVlessLink(link);
  if (link.startsWith('vmess://')) return parseVmessLink(link);
  if (link.startsWith('trojan://')) return parseTrojanLink(link);
  if (link.startsWith('ss://')) return parseSsLink(link);
  throw new Error('Unsupported link format');
}

function parseVlessLink(link) {
  // vless://uuid@host:port?params#remark
  const url = new URL(link.replace('vless://', 'https://'));
  const uuid = url.username;
  const host = url.hostname;
  const port = parseInt(url.port) || 443;
  const remark = decodeURIComponent(url.hash.slice(1) || '');
  const p = url.searchParams;
  const ob = {
    protocol: 'vless',
    tag: 'proxy',
    settings: { vnext: [{ address: host, port, users: [{ id: uuid, flow: p.get('flow') || '', encryption: 'none' }] }] },
    streamSettings: buildStreamSettings(p, host),
  };
  return { remark: remark || `${host}:${port}`, outbound: ob };
}

function parseVmessLink(link) {
  // vmess://base64json
  const raw = link.replace('vmess://', '');
  let cfg;
  try { cfg = JSON.parse(Buffer.from(raw, 'base64').toString()); } catch {
    // Some formats use URL style
    return parseVlessStyleLink(link, 'vmess');
  }
  const host = cfg.add || cfg.host || '';
  const port = parseInt(cfg.port) || 443;
  const ob = {
    protocol: 'vmess',
    tag: 'proxy',
    settings: { vnext: [{ address: host, port, users: [{ id: cfg.id, security: cfg.scy || 'auto', alterId: parseInt(cfg.aid) || 0 }] }] },
    streamSettings: {
      network: cfg.net || 'tcp',
      security: cfg.tls === 'tls' ? 'tls' : 'none',
    },
  };
  if (cfg.tls === 'tls') {
    ob.streamSettings.tlsSettings = { serverName: cfg.sni || cfg.host || host, fingerprint: cfg.fp || 'chrome', allowInsecure: false };
  }
  if (cfg.net === 'ws') {
    ob.streamSettings.wsSettings = { path: cfg.path || '/', host: cfg.host || host };
  } else if (cfg.net === 'grpc') {
    ob.streamSettings.grpcSettings = { serviceName: cfg.path || '' };
  }
  return { remark: cfg.ps || `${host}:${port}`, outbound: ob };
}

function parseTrojanLink(link) {
  const url = new URL(link.replace('trojan://', 'https://'));
  const password = decodeURIComponent(url.username);
  const host = url.hostname;
  const port = parseInt(url.port) || 443;
  const remark = decodeURIComponent(url.hash.slice(1) || '');
  const p = url.searchParams;
  const ob = {
    protocol: 'trojan',
    tag: 'proxy',
    settings: { servers: [{ address: host, port, password }] },
    streamSettings: buildStreamSettings(p, host),
  };
  return { remark: remark || `${host}:${port}`, outbound: ob };
}

function parseSsLink(link) {
  // ss://base64(method:pass)@host:port#remark or ss://base64@host:port#remark
  let raw = link.replace('ss://', '');
  let remark = '';
  const hashIdx = raw.indexOf('#');
  if (hashIdx !== -1) { remark = decodeURIComponent(raw.slice(hashIdx + 1)); raw = raw.slice(0, hashIdx); }
  let method, password, host, port;
  if (raw.includes('@')) {
    const [userPart, serverPart] = raw.split('@');
    const decoded = Buffer.from(userPart, 'base64').toString();
    [method, password] = decoded.split(':');
    const parts = serverPart.split(':');
    port = parseInt(parts.pop()); host = parts.join(':');
  } else {
    const decoded = Buffer.from(raw, 'base64').toString();
    const m = decoded.match(/^(.+?):(.+)@(.+):(\d+)$/);
    if (m) { method = m[1]; password = m[2]; host = m[3]; port = parseInt(m[4]); }
  }
  return {
    remark: remark || `${host}:${port}`,
    outbound: {
      protocol: 'shadowsocks', tag: 'proxy',
      settings: { servers: [{ address: host, port, method: method || 'aes-256-gcm', password }] },
      streamSettings: { network: 'tcp', security: 'none' },
    },
  };
}

function parseVlessStyleLink(link, proto) {
  const url = new URL(link.replace(`${proto}://`, 'https://'));
  return parseVlessLink(link.replace(`${proto}://`, 'vless://'));
}

function buildStreamSettings(params, host) {
  const net = params.get('type') || 'tcp';
  const sec = params.get('security') || 'none';
  const ss = { network: net, security: sec };
  if (sec === 'tls') {
    ss.tlsSettings = {
      serverName: params.get('sni') || host,
      fingerprint: params.get('fp') || 'chrome',
      allowInsecure: false,
      alpn: params.get('alpn') ? params.get('alpn').split(',') : undefined,
    };
  } else if (sec === 'reality') {
    ss.realitySettings = {
      serverName: params.get('sni') || host,
      fingerprint: params.get('fp') || 'chrome',
      publicKey: params.get('pbk') || '',
      shortId: params.get('sid') || '',
      spiderX: params.get('spx') || '',
    };
  }
  if (net === 'ws') {
    ss.wsSettings = { path: params.get('path') || '/', host: params.get('host') || host };
  } else if (net === 'grpc') {
    ss.grpcSettings = { serviceName: params.get('serviceName') || '' };
  } else if (net === 'tcp' && params.get('headerType') === 'http') {
    ss.tcpSettings = { header: { type: 'http', request: { path: [params.get('path') || '/'] } } };
  } else if (net === 'httpupgrade') {
    ss.httpupgradeSettings = { path: params.get('path') || '/', host: params.get('host') || host };
  } else if (net === 'splithttp') {
    ss.splithttpSettings = { path: params.get('path') || '/', host: params.get('host') || host };
  }
  return ss;
}

function cloneJson(value) {
  return value == null ? value : JSON.parse(JSON.stringify(value));
}

const XRAY_ROUTE_MODES = new Set(['direct', 'openconnect', 'openvpn', 'socks']);
let xrayConfigRouteState = new Map();

function normalizeXrayRouteMode(value) {
  return XRAY_ROUTE_MODES.has(value) ? value : 'direct';
}

function isLoopbackHost(host) {
  const value = String(host || '').trim().toLowerCase();
  return value === '127.0.0.1' || value === 'localhost' || value === '::1';
}

function getManagedTunnelDevices() {
  return new Set([ovpnTunDevice, ocTunDevice, OC_TUN_DEVICE].filter(Boolean));
}

function markManagedTunnelRoute(ip, device) {
  if (!ip || !device) return;
  if (ovpnTunDevice && device === ovpnTunDevice) ovpnRoutedHosts.add(ip);
  if ((ocTunDevice || OC_TUN_DEVICE) && device === (ocTunDevice || OC_TUN_DEVICE)) ocRoutedHosts.add(ip);
}

function unmarkManagedTunnelRoute(ip) {
  if (!ip) return;
  ovpnRoutedHosts.delete(ip);
  ocRoutedHosts.delete(ip);
}

function getTunnelDeviceForXrayRouteMode(mode) {
  if (mode === 'openvpn') {
    return ovpnStatus === 'connected' && ovpnTunDevice ? ovpnTunDevice : null;
  }
  if (mode === 'openconnect') {
    return appSettings.ocMode === 'local' && ocStatus === 'connected' && ocTunDevice ? ocTunDevice : null;
  }
  return null;
}

async function resolveXrayConfigRouteIps(cfg) {
  return resolveVpnRouteIps(extractOutboundRouteHosts(cfg && cfg.outbound));
}

function ensureManagedTunnelRoute(ip, device, label = 'Xray route sync') {
  if (!ip || net.isIP(ip) !== 4 || !device || process.platform !== 'linux') {
    return { ok: true, skipped: true };
  }

  const currentDevice = getLinuxRouteDevice(ip);
  if (currentDevice === device) {
    markManagedTunnelRoute(ip, device);
    return { ok: true, alreadyPresent: true, device };
  }

  const knownDevices = getManagedTunnelDevices();
  if (currentDevice && knownDevices.has(currentDevice) && currentDevice !== device) {
    try { execSync(`ip route del ${ip}/32`, { stdio: ['ignore', 'pipe', 'pipe'], timeout: 5000 }); } catch {}
    unmarkManagedTunnelRoute(ip);
  }

  try {
    execSync(`ip route add ${ip}/32 dev ${device}`, {
      stdio: ['ignore', 'pipe', 'pipe'],
      timeout: 5000,
    });
  } catch (addErr) {
    try {
      execSync(`ip route replace ${ip}/32 dev ${device}`, {
        stdio: ['ignore', 'pipe', 'pipe'],
        timeout: 5000,
      });
    } catch (replaceErr) {
      const stderr = replaceErr.stderr ? replaceErr.stderr.toString().trim() : '';
      return {
        ok: false,
        error: stderr || replaceErr.message || `Failed to route ${ip} via ${device}`,
      };
    }
  }

  const verifiedDevice = getLinuxRouteDevice(ip);
  if (verifiedDevice !== device) {
    return {
      ok: false,
      error: `Route for ${ip} is using ${verifiedDevice || 'unknown'} instead of ${device}`,
    };
  }

  markManagedTunnelRoute(ip, device);
  return { ok: true, added: true, device };
}

function removeManagedTunnelRoute(ip, expectedDevice = null, label = 'Xray route sync') {
  if (!ip || net.isIP(ip) !== 4 || process.platform !== 'linux') {
    unmarkManagedTunnelRoute(ip);
    return { ok: true, skipped: true };
  }

  const currentDevice = getLinuxRouteDevice(ip);
  const knownDevices = getManagedTunnelDevices();
  const canDelete = expectedDevice
    ? currentDevice === expectedDevice || knownDevices.has(currentDevice)
    : knownDevices.has(currentDevice);

  if (!currentDevice || !canDelete) {
    unmarkManagedTunnelRoute(ip);
    return { ok: true, skipped: true };
  }

  try {
    execSync(`ip route del ${ip}/32`, {
      stdio: ['ignore', 'pipe', 'pipe'],
      timeout: 5000,
    });
  } catch (e) {
    // Ignore already-missing routes; route sync will converge again on the next change.
  }

  unmarkManagedTunnelRoute(ip);
  return { ok: true, removed: true, device: currentDevice };
}

async function syncXrayConfigRouteSelection(cfg, reason = 'Xray route sync') {
  if (!cfg || !cfg.id) return { mode: 'direct', pending: false, ips: [], device: null };

  const mode = normalizeXrayRouteMode(cfg.routeMode);
  const routeSocks = normalizeXrayRouteSocks(cfg.routeSocks);
  const device = mode === 'socks' ? null : getTunnelDeviceForXrayRouteMode(mode);
  const resolvedIps = mode === 'socks' ? [] : await resolveXrayConfigRouteIps(cfg);
  const previous = xrayConfigRouteState.get(cfg.id) || [];
  const previousByIp = new Map(previous.map(entry => [entry.ip, entry.device]));
  const allIps = new Set([...resolvedIps, ...previousByIp.keys()]);

  for (const ip of allIps) {
    const previousDevice = previousByIp.get(ip) || null;
    const shouldUseTarget = !!device && resolvedIps.includes(ip);
    if (!shouldUseTarget || previousDevice !== device) {
      removeManagedTunnelRoute(ip, previousDevice, reason);
    }
  }

  const applied = [];
  if (device) {
    for (const ip of resolvedIps) {
      const result = ensureManagedTunnelRoute(ip, device, reason);
      if (!result.ok) {
        throw new Error(result.error || `Failed to route ${ip} via ${device}`);
      }
      applied.push({ ip, device });
    }
  }

  xrayConfigRouteState.set(cfg.id, applied);
  return {
    mode,
    device,
    viaLabel: mode === 'socks' ? `SOCKS ${routeSocks.host}:${routeSocks.port}` : null,
    ips: resolvedIps,
    socks: mode === 'socks' ? routeSocks : null,
    pending: mode !== 'direct' && mode !== 'socks' && !device,
  };
}

async function syncAllXrayConfigRoutes(reason = 'Xray route refresh') {
  if (!Array.isArray(xrayLocalConfigs)) return;
  for (const cfg of xrayLocalConfigs) {
    try {
      if (isManagedXraySessionActive(xraySessions.get(cfg.id))) {
        await syncXrayConfigRouteSelection(cfg, reason);
      } else {
        await clearXrayConfigRouteSelection(cfg.id, reason);
      }
    } catch (e) {
      console.error(`[Xray Routes] Failed to sync ${cfg.id}:`, e.message);
    }
  }
  broadcastOvpnState();
  broadcastOcState();
}

async function clearXrayConfigRouteSelection(configId, reason = 'Xray route clear') {
  const previous = xrayConfigRouteState.get(configId) || [];
  for (const entry of previous) {
    removeManagedTunnelRoute(entry.ip, entry.device, reason);
  }
  xrayConfigRouteState.delete(configId);
  broadcastOvpnState();
  broadcastOcState();
}

// --- Build standalone xray config for test/scanner (temporary processes) ---
function buildStandaloneXrayConfig(outbound, socksPort, httpPort, listenAddr, options = {}) {
  const addr = listenAddr || '127.0.0.1';
  const routeMode = normalizeXrayRouteMode(options.routeMode);
  const routeSocks = normalizeXrayRouteSocks(options.routeSocks);
  const primaryOutbound = cloneJson(outbound) || { protocol: 'freedom', tag: 'proxy' };
  primaryOutbound.tag = primaryOutbound.tag || 'proxy';
  if (routeMode === 'socks') {
    primaryOutbound.proxySettings = {
      ...(primaryOutbound.proxySettings && typeof primaryOutbound.proxySettings === 'object' ? primaryOutbound.proxySettings : {}),
      tag: 'route-socks',
    };
  }

  const outbounds = [
    primaryOutbound,
    { protocol: 'freedom', tag: 'direct' },
    { protocol: 'blackhole', tag: 'block' },
  ];
  if (routeMode === 'socks') {
    outbounds.splice(1, 0, {
      protocol: 'socks',
      tag: 'route-socks',
      settings: {
        servers: [{
          address: routeSocks.host,
          port: routeSocks.port,
        }],
      },
    });
  }

  const config = {
    log: { loglevel: 'warning' },
    inbounds: [
      { tag: 'socks-in', protocol: 'socks', listen: addr, port: socksPort, settings: { auth: 'noauth', udp: true } },
    ],
    outbounds,
  };
  if (httpPort && httpPort > 0) {
    config.inbounds.push({ tag: 'http-in', protocol: 'http', listen: addr, port: httpPort, settings: {} });
  }
  return config;
}

function listXrayRuns() {
  return [...xraySessions.values()]
    .filter(isManagedXraySessionActive)
    .sort((a, b) => (a.startedAt || 0) - (b.startedAt || 0))
    .map(getXraySessionPublicState);
}

function serializeXrayConfig(cfg) {
  const bindings = getEffectiveXrayConfigBindings(cfg);
  const session = xraySessions.get(cfg.id);
  const running = isManagedXraySessionActive(session);
  return {
    ...cfg,
    routeMode: normalizeXrayRouteMode(cfg.routeMode),
    routeSocks: normalizeXrayRouteSocks(cfg.routeSocks),
    localBindings: bindings,
    runState: {
      running,
      pid: session && session.proc ? session.proc.pid : null,
      startedAt: session ? session.startedAt : null,
    },
  };
}

function collectConfiguredXrayPorts(configId, bindingsOverride = null) {
  const ports = new Map();
  for (const cfg of xrayLocalConfigs) {
    if (!cfg || !cfg.id) continue;
    const bindings = cfg.id === configId && bindingsOverride
      ? normalizeXrayConfigBindings(bindingsOverride)
      : getEffectiveXrayConfigBindings(cfg);
    ports.set(`${cfg.id}:socks`, bindings.socksPort);
    if (bindings.httpPort > 0) ports.set(`${cfg.id}:http`, bindings.httpPort);
  }
  return ports;
}

function findConfiguredXrayPortConflict(configId, bindings) {
  const ports = collectConfiguredXrayPorts(configId, bindings);
  const seen = new Map();
  for (const [key, port] of ports.entries()) {
    if (!port || port <= 0) continue;
    if (seen.has(port) && !key.startsWith(`${configId}:`) && !seen.get(port).startsWith(`${configId}:`)) continue;
    if (seen.has(port)) {
      const existing = seen.get(port);
      if (!existing.startsWith(`${configId}:`) || !key.startsWith(`${configId}:`)) {
        return port;
      }
    }
    seen.set(port, key);
  }
  return null;
}

function isTcpPortInUse(port) {
  if (!port || port <= 0) return false;
  if (hasCommand('lsof')) {
    try {
      execSync(`lsof -nP -iTCP:${port} -sTCP:LISTEN 2>/dev/null`, {
        stdio: ['ignore', 'pipe', 'pipe'],
        timeout: 3000,
      });
      return true;
    } catch {}
  }
  if (process.platform === 'linux' && hasCommand('ss')) {
    try {
      execSync(`ss -tlnH 2>/dev/null | grep -q '[:.]${port} '`, {
        stdio: ['ignore', 'pipe', 'pipe'],
        timeout: 3000,
      });
      return true;
    } catch {}
  }
  return false;
}

function validateXrayLocalBindings(configId, bindings, opts = {}) {
  const normalized = normalizeXrayConfigBindings(bindings);
  if (!(normalized.socksPort > 0 && normalized.socksPort <= 65535)) {
    return 'SOCKS port must be between 1 and 65535.';
  }
  if (!(normalized.httpPort >= 0 && normalized.httpPort <= 65535)) {
    return 'HTTP port must be 0 or between 1 and 65535.';
  }
  if (normalized.httpPort > 0 && normalized.httpPort === normalized.socksPort) {
    return 'SOCKS and HTTP ports must be different.';
  }

  const configuredConflict = findConfiguredXrayPortConflict(configId, normalized);
  if (configuredConflict) {
    return `Port ${configuredConflict} is already assigned to another Xray config.`;
  }

  if (opts.checkRunningConflicts !== false) {
    for (const [otherConfigId, session] of xraySessions.entries()) {
      if (otherConfigId === configId) continue;
      if (session.localBindings.socksPort === normalized.socksPort || session.localBindings.socksPort === normalized.httpPort) {
        return `Port ${session.localBindings.socksPort} is already in use by another running Xray tunnel.`;
      }
      if (session.localBindings.httpPort > 0 && (session.localBindings.httpPort === normalized.socksPort || session.localBindings.httpPort === normalized.httpPort)) {
        return `Port ${session.localBindings.httpPort} is already in use by another running Xray tunnel.`;
      }
    }
    if (socksTunnelProc && (socksTunnelProc.tunnel.socksPort === normalized.socksPort || socksTunnelProc.tunnel.socksPort === normalized.httpPort)) {
      return `Port ${socksTunnelProc.tunnel.socksPort} is already in use by the SSH SOCKS tunnel.`;
    }
  }

  if (opts.checkListeners) {
    if (isTcpPortInUse(normalized.socksPort)) {
      return `Port ${normalized.socksPort} is already in use by another process.`;
    }
    if (normalized.httpPort > 0 && isTcpPortInUse(normalized.httpPort)) {
      return `Port ${normalized.httpPort} is already in use by another process.`;
    }
  }

  return null;
}

async function startManagedXraySession(configId, overrideBindings = null) {
  if (xraySessions.has(configId)) {
    throw new Error('This Xray config is already running.');
  }
  const cfg = getXrayConfigById(configId);
  if (!cfg) throw new Error('Config not found');

  const xrayBin = findXrayBinary();
  if (!xrayBin) throw new Error('xray binary not found on this machine');

  const bindings = normalizeXrayConfigBindings(overrideBindings || getEffectiveXrayConfigBindings(cfg));
  const bindingError = validateXrayLocalBindings(configId, bindings, { checkListeners: true });
  if (bindingError) throw new Error(bindingError);

  const routeState = await syncXrayConfigRouteSelection(cfg, `Xray start (${cfg.remark || cfg.id})`);
  if (routeState.pending) {
    throw new Error(`Selected tunnel ${routeState.mode} is not connected or does not expose a local TUN device yet.`);
  }
  if (routeState.socks && isLoopbackHost(routeState.socks.host) && !isTcpPortInUse(routeState.socks.port)) {
    throw new Error(`Selected SOCKS proxy ${routeState.socks.host}:${routeState.socks.port} is not listening.`);
  }

  const runtimeFile = getXrayConfigRuntimePath(configId);
  const runtimeConfig = buildStandaloneXrayConfig(
    cfg.outbound,
    bindings.socksPort,
    bindings.httpPort,
    bindings.listenAddr,
    {
      routeMode: cfg.routeMode,
      routeSocks: cfg.routeSocks,
    }
  );
  fs.writeFileSync(runtimeFile, JSON.stringify(runtimeConfig, null, 2));

  let proc;
  try {
    proc = spawnXray(xrayBin, runtimeFile);
  } catch (e) {
    try { fs.unlinkSync(runtimeFile); } catch {}
    await clearXrayConfigRouteSelection(configId, `Xray start failed (${cfg.remark || cfg.id})`).catch(() => {});
    throw new Error(`Failed to spawn xray: ${e.message}`);
  }

  const session = {
    configId,
    proc,
    runtimeFile,
    localBindings: bindings,
    startedAt: Date.now(),
    recentStderr: '',
    finalized: false,
    stopping: false,
  };
  xraySessions.set(configId, session);

  appendXrayLog(
    configId,
    `[panel] Route mode: ${routeState.mode}${routeState.viaLabel ? ` via ${routeState.viaLabel}` : routeState.device ? ` via ${routeState.device}` : ''}${routeState.ips.length ? ` for ${routeState.ips.join(', ')}` : ''}`
  );

  const finalize = (line) => {
    if (session.finalized) return;
    session.finalized = true;
    if (line) appendXrayLog(configId, line);
    if (xraySessions.get(configId) === session) {
      xraySessions.delete(configId);
    }
    try { fs.unlinkSync(runtimeFile); } catch {}
    clearXrayConfigRouteSelection(configId, `Xray session ended (${cfg.remark || cfg.id})`).catch(() => {});
    refreshVpnRoutes().catch(() => {});
    if (localXrayProc) restartLocalXray();
  };

  proc.stderr.on('data', data => {
    const text = data.toString().trim();
    if (!text) return;
    session.recentStderr += text + '\n';
    session.recentStderr = session.recentStderr.slice(-4000);
    text.split('\n').forEach(line => appendXrayLog(configId, line));
  });
  proc.stdout.on('data', data => {
    const text = data.toString().trim();
    if (!text) return;
    text.split('\n').forEach(line => appendXrayLog(configId, line));
  });
  proc.on('error', err => finalize(`[process error: ${err.message}]`));
  proc.on('exit', code => finalize(`[process exited code=${code}]`));

  await new Promise(resolve => setTimeout(resolve, 800));
  if (proc.exitCode !== null) {
    throw new Error(`Xray exited immediately: ${session.recentStderr.slice(0, 500)}`);
  }

  if (localXrayProc) restartLocalXray();
  return { session, routeState };
}

function stopManagedXraySession(configId, reason = 'Stopped by user') {
  const session = xraySessions.get(configId);
  if (!session || session.stopping) return false;
  session.stopping = true;
  appendXrayLog(configId, `[panel] ${reason}`);
  clearXrayConfigRouteSelection(configId, reason).catch(() => {});
  if (localXrayProc) restartLocalXray();
  try { session.proc.kill('SIGTERM'); } catch {}
  return true;
}

// --- Xray config CRUD ---
app.get('/api/xray-local/configs', (req, res) => {
  res.json(xrayLocalConfigs.map(serializeXrayConfig));
});

app.post('/api/xray-local/configs', (req, res) => {
  try {
    const { link } = req.body;
    const parsed = parseV2RayLink(link);
    const existingPorts = new Set();
    for (const bindings of getEffectiveXrayConfigBindingsMap().values()) {
      existingPorts.add(bindings.socksPort);
      if (bindings.httpPort > 0) existingPorts.add(bindings.httpPort);
    }
    const suggestedPorts = getNextSuggestedXrayBindingPorts(existingPorts);
    const entry = {
      id: crypto.randomUUID(),
      remark: parsed.remark,
      link,
      outbound: parsed.outbound,
      routeMode: 'direct',
      routeSocks: getDefaultXrayRouteSocks(),
      localBindings: {
        listenAddr: XRAY_BINDINGS_DEFAULTS.listenAddr,
        socksPort: suggestedPorts.socksPort,
        httpPort: suggestedPorts.httpPort,
      },
      createdAt: Date.now(),
    };
    xrayLocalConfigs.push(entry);
    saveXrayLocalConfigs();
    res.json({ ok: true, config: serializeXrayConfig(entry) });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

app.delete('/api/xray-local/configs/:id', async (req, res) => {
  const configId = req.params.id;
  const wasRunning = xraySessions.has(configId);
  stopManagedXraySession(configId, 'Config deleted');
  await clearXrayConfigRouteSelection(configId, 'Config deleted').catch(() => {});
  xrayLocalConfigs = xrayLocalConfigs.filter(c => c.id !== configId);
  saveXrayLocalConfigs();
  if (!wasRunning && localXrayProc) restartLocalXray();
  res.json({ ok: true });
});

app.put('/api/xray-local/configs/:id/remark', (req, res) => {
  const cfg = xrayLocalConfigs.find(c => c.id === req.params.id);
  if (cfg) {
    cfg.remark = req.body.remark || cfg.remark;
    saveXrayLocalConfigs();
  }
  res.json({ ok: true, config: cfg ? serializeXrayConfig(cfg) : null });
});

app.put('/api/xray-local/configs/:id/route-mode', async (req, res) => {
  const cfg = xrayLocalConfigs.find(c => c.id === req.params.id);
  if (!cfg) return res.status(404).json({ ok: false, error: 'Config not found' });

  const previousMode = normalizeXrayRouteMode(cfg.routeMode);
  cfg.routeMode = normalizeXrayRouteMode(req.body.routeMode);
  if (req.body && (req.body.routeSocks || req.body.socksHost !== undefined || req.body.socksPort !== undefined)) {
    cfg.routeSocks = normalizeXrayRouteSocks(
      req.body.routeSocks || {
        host: req.body.socksHost,
        port: req.body.socksPort,
      },
      cfg.routeSocks
    );
  } else {
    cfg.routeSocks = normalizeXrayRouteSocks(cfg.routeSocks);
  }
  saveXrayLocalConfigs();

  try {
    let state;
    if (isManagedXraySessionActive(xraySessions.get(cfg.id))) {
      if (previousMode === 'socks' || cfg.routeMode === 'socks') {
        state = {
          mode: cfg.routeMode,
          device: null,
          viaLabel: cfg.routeMode === 'socks' ? `SOCKS ${cfg.routeSocks.host}:${cfg.routeSocks.port}` : null,
          socks: cfg.routeMode === 'socks' ? normalizeXrayRouteSocks(cfg.routeSocks) : null,
          ips: [],
          pending: false,
          willApplyOnRun: true,
        };
      } else {
        state = await syncXrayConfigRouteSelection(cfg, `Route mode changed (${cfg.remark || cfg.id})`);
      }
    } else {
      await clearXrayConfigRouteSelection(cfg.id, `Route mode saved (${cfg.remark || cfg.id})`);
      const mode = normalizeXrayRouteMode(cfg.routeMode);
      const socks = normalizeXrayRouteSocks(cfg.routeSocks);
      const device = mode === 'socks' ? null : getTunnelDeviceForXrayRouteMode(mode);
      state = {
        mode,
        device,
        viaLabel: mode === 'socks' ? `SOCKS ${socks.host}:${socks.port}` : null,
        socks: mode === 'socks' ? socks : null,
        ips: mode === 'socks' ? [] : await resolveXrayConfigRouteIps(cfg),
        pending: mode !== 'direct' && mode !== 'socks' && !device,
        willApplyOnRun: mode !== 'direct',
      };
    }
    res.json({ ok: true, config: serializeXrayConfig(cfg), routeState: state });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.put('/api/xray-local/configs/:id/local-bindings', (req, res) => {
  const cfg = xrayLocalConfigs.find(c => c.id === req.params.id);
  if (!cfg) return res.status(404).json({ ok: false, error: 'Config not found' });
  if (xraySessions.has(cfg.id)) {
    return res.status(400).json({ ok: false, error: 'Stop this Xray tunnel before changing its bindings.' });
  }

  const bindings = normalizeXrayConfigBindings(req.body || {}, getEffectiveXrayConfigBindings(cfg));
  const validationError = validateXrayLocalBindings(cfg.id, bindings, { checkRunningConflicts: true, checkListeners: false });
  if (validationError) {
    return res.status(400).json({ ok: false, error: validationError });
  }

  cfg.localBindings = bindings;
  saveXrayLocalConfigs();
  res.json({ ok: true, config: serializeXrayConfig(cfg) });
});

// Edit full xray outbound config (JSON)
app.put('/api/xray-local/configs/:id/outbound', (req, res) => {
  const cfg = xrayLocalConfigs.find(c => c.id === req.params.id);
  if (!cfg) return res.status(404).json({ ok: false, error: 'Config not found' });
  if (xraySessions.has(cfg.id)) {
    return res.status(400).json({ ok: false, error: 'Stop this Xray tunnel before editing its outbound config.' });
  }
  const { outbound } = req.body;
  if (!outbound || !outbound.protocol) return res.status(400).json({ ok: false, error: 'Invalid outbound object' });
  cfg.outbound = outbound;
  saveXrayLocalConfigs();
  res.json({ ok: true, config: serializeXrayConfig(cfg) });
});

// Get single config (for edit modal)
app.get('/api/xray-local/configs/:id', (req, res) => {
  const cfg = xrayLocalConfigs.find(c => c.id === req.params.id);
  if (!cfg) return res.status(404).json({ error: 'Not found' });
  res.json(serializeXrayConfig(cfg));
});

// --- Xray process management ---
function findXrayBinary() {
  // Use configured path if set and valid
  if (appSettings.xrayBinaryPath && fs.existsSync(appSettings.xrayBinaryPath)) {
    return appSettings.xrayBinaryPath;
  }
  return autoDetectXrayBinary();
}

function spawnXray(xrayBin, configPath, opts) {
  // Set XRAY_LOCATION_ASSET so xray finds geoip.dat/geosite.dat next to the binary
  const env = { ...process.env, XRAY_LOCATION_ASSET: path.dirname(xrayBin) };
  return spawn(xrayBin, ['run', '-c', configPath], { ...opts, env, stdio: ['ignore', 'pipe', 'pipe'] });
}

app.get('/api/xray-local/status', (req, res) => {
  const runs = listXrayRuns();
  res.json({
    running: runs.length > 0,
    configId: runs.length === 1 ? runs[0].configId : null,
    runs,
    binary: !!findXrayBinary(),
  });
});

app.get('/api/xray-local/interfaces', (req, res) => {
  const os = require('os');
  const ifaces = os.networkInterfaces();
  const addrs = ['0.0.0.0', '127.0.0.1'];
  for (const [name, list] of Object.entries(ifaces)) {
    for (const info of list) {
      if (info.family === 'IPv4' && !info.internal && !addrs.includes(info.address)) {
        addrs.push(info.address);
      }
    }
  }
  res.json(addrs);
});

app.post('/api/xray-local/start', async (req, res) => {
  const { configId, socksPort, httpPort, listenAddr } = req.body;
  const cfg = xrayLocalConfigs.find(c => c.id === configId);
  if (!cfg) return res.status(404).json({ ok: false, error: 'Config not found' });
  const bindings = normalizeXrayConfigBindings({
    ...getEffectiveXrayConfigBindings(cfg),
    ...(listenAddr !== undefined ? { listenAddr } : {}),
    ...(socksPort !== undefined ? { socksPort } : {}),
    ...(httpPort !== undefined ? { httpPort } : {}),
  });

  const saveError = validateXrayLocalBindings(cfg.id, bindings, { checkRunningConflicts: true, checkListeners: false });
  if (saveError) {
    return res.status(400).json({ ok: false, error: saveError });
  }

  cfg.localBindings = bindings;
  saveXrayLocalConfigs();

  try {
    const { session, routeState } = await startManagedXraySession(configId, bindings);
    res.json({ ok: true, pid: session.proc.pid, routeState, run: getXraySessionPublicState(session) });
  } catch (e) {
    const status = /already running|not found|not connected|port|Stop this/i.test(e.message) ? 400 : 500;
    res.status(status).json({ ok: false, error: e.message });
  }
});

app.post('/api/xray-local/stop', (req, res) => {
  const configId = String(req.body && req.body.configId || '').trim() || null;
  if (configId) {
    stopManagedXraySession(configId, 'Stopped by user');
    return res.json({ ok: true });
  }
  for (const id of [...xraySessions.keys()]) {
    stopManagedXraySession(id, 'Stopped by user');
  }
  res.json({ ok: true });
});

app.get('/api/xray-local/logs', (req, res) => {
  const configId = String(req.query.configId || '').trim();
  res.json(configId ? xrayLogs.filter(entry => entry.configId === configId) : xrayLogs);
});

// Quick test: spin up xray with config, try SOCKS connect, return pass/fail + latency
app.post('/api/xray-local/test', async (req, res) => {
  const { configId } = req.body;
  const cfg = xrayLocalConfigs.find(c => c.id === configId);
  if (!cfg) return res.status(404).json({ ok: false, error: 'Config not found' });
  const xrayBin = findXrayBinary();
  if (!xrayBin) return res.status(500).json({ ok: false, error: 'xray binary not found' });

  let routeState;
  try {
    routeState = await syncXrayConfigRouteSelection(cfg, `Xray test (${cfg.remark || cfg.id})`);
    if (routeState.pending) {
      return res.status(400).json({
        ok: false,
        error: `Selected tunnel ${routeState.mode} is not connected or does not expose a local TUN device yet.`,
      });
    }
  } catch (e) {
    return res.status(500).json({ ok: false, error: `Failed to sync config route: ${e.message}` });
  }

  const testPort = 30000 + Math.floor(Math.random() * 10000);
  const testConfig = buildStandaloneXrayConfig(cfg.outbound, testPort, 0, '127.0.0.1', {
    routeMode: cfg.routeMode,
    routeSocks: cfg.routeSocks,
  });
  // Set log level to info so we capture connection details
  testConfig.log = { loglevel: 'info' };
  const tmpPath = path.join(__dirname, `.xray-test-${testPort}.json`);
  fs.writeFileSync(tmpPath, JSON.stringify(testConfig));

  const proc = spawnXray(xrayBin, tmpPath);
  let logs = '';
  proc.stdout.on('data', d => { logs += d.toString(); });
  proc.stderr.on('data', d => { logs += d.toString(); });

  const start = Date.now();
  // Wait for xray to start
  await new Promise(r => setTimeout(r, 600));

  if (proc.exitCode !== null) {
    try { fs.unlinkSync(tmpPath); } catch {}
    return res.json({ ok: false, error: 'Xray failed to start', logs: logs.slice(-1000) });
  }

  let success = false;
  let latency = 0;
  logs += `[panel] Route mode: ${routeState.mode}${routeState.device ? ` via ${routeState.device}` : ''}${routeState.ips.length ? ` for ${routeState.ips.join(', ')}` : ''}\n`;
  try {
    await Promise.race([
      testHttpThroughSocks(testPort, 8000),
      new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 8000)),
    ]);
    success = true;
    latency = Date.now() - start;
  } catch (e) {
    logs += '\nConnection test: ' + e.message;
  }

  proc.kill('SIGTERM');
  // Wait a bit for final logs
  await new Promise(r => setTimeout(r, 300));
  try { fs.unlinkSync(tmpPath); } catch {}

  res.json({ ok: success, latency, logs: logs.slice(-2000) });
});

// --- IP Scanner ---
let scanAbort = null;

app.post('/api/xray-local/scan', async (req, res) => {
  const { configId, cidrs, concurrency, timeout, maxIps } = req.body;
  const cfg = xrayLocalConfigs.find(c => c.id === configId);
  if (!cfg) return res.status(404).json({ ok: false, error: 'Config not found' });

  const ips = [];
  for (const cidr of (cidrs || [])) {
    const generated = expandCidr(cidr.trim(), maxIps || 100);
    ips.push(...generated);
    if (ips.length >= (maxIps || 100)) break;
  }
  ips.splice(maxIps || 100);

  if (!ips.length) return res.status(400).json({ ok: false, error: 'No IPs from CIDR' });

  const xrayBin = findXrayBinary();
  if (!xrayBin) return res.status(500).json({ ok: false, error: 'xray binary not found' });

  const routeMode = normalizeXrayRouteMode(cfg.routeMode);
  const routeDevice = getTunnelDeviceForXrayRouteMode(routeMode);
  try {
    await syncXrayConfigRouteSelection(cfg, `Xray scan (${cfg.remark || cfg.id})`);
    if (routeMode !== 'direct' && !routeDevice) {
      return res.status(400).json({
        ok: false,
        error: `Selected tunnel ${routeMode} is not connected or does not expose a local TUN device yet.`,
      });
    }
  } catch (e) {
    return res.status(500).json({ ok: false, error: `Failed to sync config route: ${e.message}` });
  }

  const controller = new AbortController();
  scanAbort = controller;

  const results = [];
  const conc = Math.min(concurrency || 10, 50);
  const timeoutMs = (timeout || 5) * 1000;
  let tested = 0;

  // Set SSE headers
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const sendEvent = (data) => { if (!res.writableEnded) res.write(`data: ${JSON.stringify(data)}\n\n`); };

  const testIp = async (ip) => {
    if (controller.signal.aborted) return;
    // Build a config with this IP as the server address
    const ob = JSON.parse(JSON.stringify(cfg.outbound));
    replaceOutboundAddress(ob, ip);

    const scanPort = 20000 + Math.floor(Math.random() * 10000);
    const scanConfig = buildStandaloneXrayConfig(ob, scanPort, 0, '127.0.0.1', {
      routeMode: cfg.routeMode,
      routeSocks: cfg.routeSocks,
    });
    const tmpPath = path.join(__dirname, `.xray-scan-${scanPort}.json`);
    fs.writeFileSync(tmpPath, JSON.stringify(scanConfig));

    const start = Date.now();
    let success = false;
    let latency = 0;

    try {
      if (routeDevice) {
        const routeResult = ensureManagedTunnelRoute(ip, routeDevice, `Xray scan (${cfg.remark || cfg.id})`);
        if (!routeResult.ok) throw new Error(routeResult.error || `Failed to route ${ip} via ${routeDevice}`);
      }

      const proc = spawnXray(xrayBin, tmpPath);
      await new Promise(resolve => setTimeout(resolve, 500)); // let xray start

      if (proc.exitCode !== null) { proc.kill(); throw new Error('xray exited'); }

      // Try connecting through the SOCKS proxy
      try {
        await Promise.race([
          testHttpThroughSocks(scanPort, timeoutMs),
          new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), timeoutMs)),
        ]);
        success = true;
        latency = Date.now() - start;
      } catch {}

      proc.kill('SIGTERM');
    } catch {} finally {
      if (routeDevice) removeManagedTunnelRoute(ip, routeDevice, `Xray scan cleanup (${cfg.remark || cfg.id})`);
      try { fs.unlinkSync(tmpPath); } catch {}
    }

    tested++;
    const result = { ip, success, latency };
    if (success) results.push(result);
    sendEvent({ type: 'progress', tested, total: ips.length, result });
  };

  // Run in batches
  for (let i = 0; i < ips.length && !controller.signal.aborted; i += conc) {
    const batch = ips.slice(i, i + conc);
    await Promise.all(batch.map(testIp));
  }

  try {
    await syncXrayConfigRouteSelection(cfg, `Restore config route (${cfg.remark || cfg.id})`);
  } catch (e) {
    console.error(`[Xray Scan] Failed to restore route for ${cfg.id}:`, e.message);
  }
  sendEvent({ type: 'done', results, total: ips.length, tested });
  scanAbort = null;
  res.end();
});

app.post('/api/xray-local/scan/stop', (req, res) => {
  if (scanAbort) { scanAbort.abort(); scanAbort = null; }
  res.json({ ok: true });
});

app.post('/api/xray-local/scan/estimate', (req, res) => {
  const { cidrs, maxIps } = req.body;
  if (!cidrs || !cidrs.length) return res.json({ totalIps: 0, suggestedConcurrency: 1 });
  const max = maxIps || 100;
  const totalIps = countCidrIps(cidrs, max);
  // Suggest concurrency: ~10% of IPs, min 1, max 50
  const suggestedConcurrency = Math.max(1, Math.min(50, Math.ceil(totalIps * 0.1)));
  res.json({ totalIps, suggestedConcurrency });
});

function replaceOutboundAddress(ob, newAddress) {
  if (ob.settings?.vnext?.[0]) {
    ob.settings.vnext[0].address = newAddress;
  } else if (ob.settings?.servers?.[0]) {
    ob.settings.servers[0].address = newAddress;
  }
}

function testHttpThroughSocks(socksPort, timeoutMs) {
  return new Promise((resolve, reject) => {
    const sock = new net.Socket();
    sock.setTimeout(timeoutMs);
    let phase = 'handshake'; // handshake -> connect -> http
    sock.connect(socksPort, '127.0.0.1', () => {
      sock.write(Buffer.from([0x05, 0x01, 0x00])); // SOCKS5 no-auth
    });
    sock.on('data', (data) => {
      if (phase === 'handshake') {
        if (data[0] !== 0x05 || data[1] !== 0x00) { sock.destroy(); return reject(new Error('socks handshake failed')); }
        phase = 'connect';
        // Connect to google.com:80 via domain (0x03)
        const domain = 'google.com';
        const buf = Buffer.alloc(7 + domain.length);
        buf[0] = 0x05; buf[1] = 0x01; buf[2] = 0x00; buf[3] = 0x03;
        buf[4] = domain.length;
        buf.write(domain, 5);
        buf.writeUInt16BE(80, 5 + domain.length);
        sock.write(buf);
      } else if (phase === 'connect') {
        if (data[1] !== 0x00) { sock.destroy(); return reject(new Error('socks connect failed')); }
        phase = 'http';
        sock.write('GET / HTTP/1.1\r\nHost: google.com\r\nConnection: close\r\n\r\n');
      } else if (phase === 'http') {
        const resp = data.toString();
        sock.destroy();
        if (resp.includes('HTTP/')) resolve(); // got HTTP response
        else reject(new Error('no HTTP response'));
      }
    });
    sock.on('error', (e) => { sock.destroy(); reject(e); });
    sock.on('timeout', () => { sock.destroy(); reject(new Error('timeout')); });
  });
}

function expandCidr(cidr, maxCount) {
  const [base, bitsStr] = cidr.split('/');
  const bits = parseInt(bitsStr);

  // No CIDR suffix or invalid — treat as single IP
  if (isNaN(bits) || bits < 8 || bits > 32) return [base];

  // /32 = exact single host
  if (bits === 32) return [base];

  // /31 = point-to-point link, both IPs usable
  if (bits === 31) {
    const parts = base.split('.').map(Number);
    const baseNum = ((parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]) >>> 0;
    const networkStart = (baseNum & 0xFFFFFFFE) >>> 0;
    return [0, 1].map(i => {
      const ip = (networkStart + i) >>> 0;
      return `${(ip >> 24) & 0xFF}.${(ip >> 16) & 0xFF}.${(ip >> 8) & 0xFF}.${ip & 0xFF}`;
    });
  }

  const parts = base.split('.').map(Number);
  const baseNum = ((parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]) >>> 0;
  const mask = ~((1 << (32 - bits)) - 1);
  const networkStart = (baseNum & mask) >>> 0;
  const totalHosts = 1 << (32 - bits);
  const usableHosts = totalHosts - 2; // exclude network + broadcast

  const ips = [];
  const step = Math.max(1, Math.floor(usableHosts / maxCount));
  for (let i = 1; i <= usableHosts && ips.length < maxCount; i += step) {
    const ip = (networkStart + i) >>> 0;
    ips.push(`${(ip >> 24) & 0xFF}.${(ip >> 16) & 0xFF}.${(ip >> 8) & 0xFF}.${ip & 0xFF}`);
  }
  return ips;
}

// Count total IPs that would be generated from CIDRs (for UI estimation)
function countCidrIps(cidrs, maxIps) {
  let total = 0;
  for (const cidr of cidrs) {
    const [base, bitsStr] = cidr.trim().split('/');
    const bits = parseInt(bitsStr);
    if (isNaN(bits) || bits > 32) { total += 1; continue; }
    if (bits === 32) { total += 1; continue; }
    if (bits === 31) { total += 2; continue; }
    if (bits < 8) continue;
    total += Math.max(1, (1 << (32 - bits)) - 2);
  }
  return Math.min(total, maxIps);
}

// ─── Redsocks + DNS2SOCKS + iptables (Traffic Routing) ──────────────

let redsocksProc = null;
let dns2socksProc = null;
let redsocksStatus = 'stopped';  // stopped | running | error
let vpnRoutingActive = false;
let routingLogs = [];
const MAX_ROUTING_LOGS = 400;

const REDSOCKS_PORT = 12345;
const DNS2SOCKS_PORT = 5353;
const VPN_TCP_MSS = 1200;
const OPENVPN_SUBNET = '10.8.0.0/24';
const L2TP_SUBNET = '10.9.0.0/24';
const VPN_REDSOCKS_TCP_CHAIN = 'VPN_REDSOCKS_TCP';

let routingSocksPort = null; // user-overridden SOCKS port for redsocks

function addRoutingLog(text) {
  const entry = { time: Date.now(), text };
  routingLogs.push(entry);
  if (routingLogs.length > MAX_ROUTING_LOGS) routingLogs = routingLogs.slice(-MAX_ROUTING_LOGS);
  broadcast({ type: 'routing_log', entry });
}

function getActiveSocksPort() {
  // 1. User-configured override
  if (routingSocksPort) return routingSocksPort;
  // 2. SSH SOCKS tunnel (-D)
  if (socksTunnelProc) return socksTunnelProc.tunnel.socksPort;
  // 3. Auto-pick a single Xray run only when exactly one session exists.
  const runs = listXrayRuns();
  if (runs.length === 1) return runs[0].socksPort;
  return null;
}

function getRoutingSocksSources() {
  const sources = [];
  if (socksTunnelProc) sources.push({ label: `SSH SOCKS (${socksTunnelProc.tunnel.name})`, port: socksTunnelProc.tunnel.socksPort });
  for (const run of listXrayRuns()) {
    const cfg = getXrayConfigById(run.configId);
    sources.push({
      label: `Xray Client (${cfg ? cfg.remark : run.configId})`,
      port: run.socksPort,
    });
  }
  return sources;
}

function getTransparentProxyBypassCidrs() {
  const cidrs = new Set([
    '0.0.0.0/8',
    '10.0.0.0/8',
    '100.64.0.0/10',
    '127.0.0.0/8',
    '169.254.0.0/16',
    '172.16.0.0/12',
    '192.0.0.0/24',
    '192.168.0.0/16',
    '198.18.0.0/15',
    '224.0.0.0/4',
    '240.0.0.0/4',
    OPENVPN_SUBNET,
    L2TP_SUBNET,
  ]);

  const interfaces = os.networkInterfaces ? os.networkInterfaces() : {};
  Object.values(interfaces || {}).forEach(addresses => {
    (addresses || []).forEach(address => {
      if (!address || address.family !== 'IPv4' || !address.address) return;
      cidrs.add(`${address.address}/32`);
    });
  });

  return [...cidrs];
}

function rebuildVpnTransparentProxyChain() {
  const bypassCidrs = getTransparentProxyBypassCidrs();

  try { runIptablesCommand(`iptables -t nat -N ${VPN_REDSOCKS_TCP_CHAIN}`); } catch {}
  try { runIptablesCommand(`iptables -t nat -F ${VPN_REDSOCKS_TCP_CHAIN}`); } catch {}

  for (const cidr of bypassCidrs) {
    try {
      runIptablesCommand(`iptables -t nat -A ${VPN_REDSOCKS_TCP_CHAIN} -d ${cidr} -j RETURN`);
    } catch (e) {
      console.error('[iptables] Failed to add transparent proxy bypass:', cidr, e.message);
      addRoutingLog(`iptables bypass failed: ${cidr} (${e.message})`);
    }
  }

  try {
    runIptablesCommand(`iptables -t nat -A ${VPN_REDSOCKS_TCP_CHAIN} -p tcp -j REDIRECT --to-ports ${REDSOCKS_PORT}`);
  } catch (e) {
    console.error('[iptables] Failed to finalize transparent proxy chain:', e.message);
    addRoutingLog(`iptables redirect chain failed: ${e.message}`);
  }
}

function writeRedsocksConfig(socksPort) {
  const config = `base {
    log_debug = off;
    log_info = on;
    daemon = off;
    redirector = iptables;
}

redsocks {
    local_ip = 0.0.0.0;
    local_port = ${REDSOCKS_PORT};
    ip = 127.0.0.1;
    port = ${socksPort};
    type = socks5;
}
`;
  try {
    fs.writeFileSync('/etc/redsocks.conf', config);
    return true;
  } catch (e) {
    console.error('[Redsocks] Failed to write config:', e.message);
    return false;
  }
}

function startRedsocks(socksPort) {
  stopRedsocks();

  if (!socksPort) {
    console.error('[Redsocks] No SOCKS port provided');
    addRoutingLog('No SOCKS port was available for routing');
    return false;
  }
  if (!isTcpPortInUse(socksPort)) {
    console.error('[Redsocks] Selected upstream SOCKS port is not listening:', socksPort);
    addRoutingLog(`SOCKS port ${socksPort} is not listening; routing not enabled`);
    redsocksStatus = 'error';
    broadcastVpnServerState();
    return false;
  }

  // Kill any stale redsocks/dns2socks processes from previous runs
  try { execSync('killall -q redsocks 2>/dev/null || true', { stdio: 'ignore' }); } catch (e) {}
  try { execSync('killall -q dns2socks 2>/dev/null || true', { stdio: 'ignore' }); } catch (e) {}

  if (!writeRedsocksConfig(socksPort)) {
    addRoutingLog('Failed to write /etc/redsocks.conf');
    redsocksStatus = 'error';
    broadcastVpnServerState();
    return false;
  }
  addRoutingLog(`Preparing routing via SOCKS port ${socksPort}`);

  // First, flush any stale iptables rules from previous runs
  flushVpnIptables();

  // Start redsocks
  console.log('[Redsocks] Spawning: redsocks -c /etc/redsocks.conf');
  addRoutingLog(`Starting redsocks on local port ${REDSOCKS_PORT}`);
  redsocksProc = spawn('redsocks', ['-c', '/etc/redsocks.conf'], {
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  redsocksProc.on('error', (err) => {
    console.error('[Redsocks] Spawn error:', err.message);
    addRoutingLog(`redsocks spawn error: ${err.message}`);
    redsocksStatus = 'error';
    redsocksProc = null;
    broadcastVpnServerState();
  });
  redsocksProc.on('close', (code) => {
    console.log(`[Redsocks] Exited with code ${code}`);
    addRoutingLog(`redsocks exited with code ${code}`);
    redsocksProc = null;
    redsocksStatus = 'stopped';
    broadcastVpnServerState();
  });
  redsocksProc.stdout.on('data', d => {
    const lines = d.toString().split('\n').map(s => s.trim()).filter(Boolean);
    lines.forEach(line => {
      console.log('[Redsocks stdout]', line);
      addRoutingLog(`[redsocks] ${line}`);
    });
  });
  redsocksProc.stderr.on('data', d => {
    const lines = d.toString().split('\n').map(s => s.trim()).filter(Boolean);
    lines.forEach(line => {
      console.log('[Redsocks stderr]', line);
      addRoutingLog(`[redsocks] ${line}`);
    });
  });

  // Start dns2socks
  const dns2socksPath = fs.existsSync('/usr/local/bin/dns2socks') ? '/usr/local/bin/dns2socks' : 'dns2socks';
  console.log(`[dns2socks] Spawning: ${dns2socksPath} 127.0.0.1:${socksPort} 8.8.8.8 127.0.0.1:${DNS2SOCKS_PORT}`);
  addRoutingLog(`Starting dns2socks on UDP ${DNS2SOCKS_PORT}`);
  dns2socksProc = spawn(dns2socksPath, [
    `127.0.0.1:${socksPort}`, '8.8.8.8', `127.0.0.1:${DNS2SOCKS_PORT}`
  ], {
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  dns2socksProc.on('error', (err) => {
    console.error('[dns2socks] Spawn error:', err.message);
    addRoutingLog(`dns2socks spawn error: ${err.message}`);
    dns2socksProc = null;
  });
  dns2socksProc.on('close', (code) => {
    console.log(`[dns2socks] Exited with code ${code}`);
    addRoutingLog(`dns2socks exited with code ${code}`);
    dns2socksProc = null;
  });
  dns2socksProc.stdout.on('data', d => {
    const lines = d.toString().split('\n').map(s => s.trim()).filter(Boolean);
    lines.forEach(line => {
      console.log('[dns2socks]', line);
      addRoutingLog(`[dns2socks] ${line}`);
    });
  });
  dns2socksProc.stderr.on('data', d => {
    const lines = d.toString().split('\n').map(s => s.trim()).filter(Boolean);
    lines.forEach(line => {
      console.log('[dns2socks]', line);
      addRoutingLog(`[dns2socks] ${line}`);
    });
  });

  // Wait for the local TCP and DNS proxy listeners before applying iptables.
  let redsocksReady = false;
  let dns2socksReady = false;
  for (let i = 0; i < 10; i++) {
    try { execSync('sleep 0.3', { stdio: 'ignore', timeout: 3000 }); } catch (e) {}
    try {
      execSync(`ss -tlnp | grep -q ':${REDSOCKS_PORT} '`, { stdio: 'pipe', timeout: 3000 });
      redsocksReady = true;
    } catch (e) {}
    try {
      execSync(`ss -ulnp | grep -q ':${DNS2SOCKS_PORT} '`, { stdio: 'pipe', timeout: 3000 });
      dns2socksReady = true;
    } catch (e) {}
    if (redsocksReady && dns2socksReady) break;
    // Check if process already exited
    if ((!redsocksProc || redsocksProc.exitCode !== null) && (!dns2socksProc || dns2socksProc.exitCode !== null)) break;
  }

  if (!redsocksReady) {
    console.error('[Redsocks] Not listening on port ' + REDSOCKS_PORT + ' — NOT applying iptables (would black-hole traffic)');
    addRoutingLog(`redsocks never started listening on ${REDSOCKS_PORT}; routing not enabled`);
    stopRedsocks();
    redsocksStatus = 'error';
    broadcastVpnServerState();
    return false;
  }
  if (!dns2socksReady) {
    console.error('[dns2socks] Not listening on UDP ' + DNS2SOCKS_PORT + ' — NOT applying iptables (DNS would black-hole)');
    addRoutingLog(`dns2socks never started listening on ${DNS2SOCKS_PORT}; routing not enabled`);
    stopRedsocks();
    redsocksStatus = 'error';
    broadcastVpnServerState();
    return false;
  }

  // Apply iptables only after confirming redsocks is running
  applyVpnIptables();

  redsocksStatus = 'running';
  broadcastVpnServerState();
  addAudit('redsocks_started', `SOCKS port ${socksPort}`);
  addRoutingLog(`Routing active: TCP -> ${REDSOCKS_PORT}, DNS -> ${DNS2SOCKS_PORT}, upstream SOCKS ${socksPort}`);
  console.log(`[Redsocks] Started on :${REDSOCKS_PORT} → SOCKS :${socksPort}`);
  return true;
}

function stopRedsocks() {
  addRoutingLog('Stopping routing and removing VPN redirect rules');
  flushVpnIptables();

  if (redsocksProc) {
    try { redsocksProc.kill('SIGTERM'); } catch (e) {}
    redsocksProc = null;
  }
  if (dns2socksProc) {
    try { dns2socksProc.kill('SIGTERM'); } catch (e) {}
    dns2socksProc = null;
  }

  // Also kill any orphaned system processes
  try { execSync('killall -q redsocks 2>/dev/null || true', { stdio: 'ignore' }); } catch (e) {}
  try { execSync('killall -q dns2socks 2>/dev/null || true', { stdio: 'ignore' }); } catch (e) {}

  redsocksStatus = 'stopped';
  vpnRoutingActive = false;
  addRoutingLog('Routing stopped');
}

function applyVpnIptables() {
  addRoutingLog('Applying VPN routing iptables rules');
  // Use -C (check) before -A (append) to prevent duplicate rules
  const rules = [
    { check: `iptables -t nat -C POSTROUTING -s ${OPENVPN_SUBNET} -j MASQUERADE`, add: `iptables -t nat -A POSTROUTING -s ${OPENVPN_SUBNET} -j MASQUERADE` },
    { check: `iptables -t nat -C POSTROUTING -s ${L2TP_SUBNET} -j MASQUERADE`, add: `iptables -t nat -A POSTROUTING -s ${L2TP_SUBNET} -j MASQUERADE` },
    { check: `iptables -t mangle -C PREROUTING -s ${OPENVPN_SUBNET} -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --set-mss ${VPN_TCP_MSS}`, add: `iptables -t mangle -A PREROUTING -s ${OPENVPN_SUBNET} -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --set-mss ${VPN_TCP_MSS}` },
    { check: `iptables -t mangle -C PREROUTING -s ${L2TP_SUBNET} -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --set-mss ${VPN_TCP_MSS}`, add: `iptables -t mangle -A PREROUTING -s ${L2TP_SUBNET} -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --set-mss ${VPN_TCP_MSS}` },
    // TCP → redsocks, but only after bypassing local/private/server destinations.
    { check: `iptables -t nat -C PREROUTING -s ${OPENVPN_SUBNET} -p tcp -j ${VPN_REDSOCKS_TCP_CHAIN}`, add: `iptables -t nat -A PREROUTING -s ${OPENVPN_SUBNET} -p tcp -j ${VPN_REDSOCKS_TCP_CHAIN}` },
    { check: `iptables -t nat -C PREROUTING -s ${L2TP_SUBNET} -p tcp -j ${VPN_REDSOCKS_TCP_CHAIN}`, add: `iptables -t nat -A PREROUTING -s ${L2TP_SUBNET} -p tcp -j ${VPN_REDSOCKS_TCP_CHAIN}` },
    // DNS → dns2socks
    { check: `iptables -t nat -C PREROUTING -s ${OPENVPN_SUBNET} -p udp --dport 53 -j DNAT --to-destination 127.0.0.1:${DNS2SOCKS_PORT}`, add: `iptables -t nat -A PREROUTING -s ${OPENVPN_SUBNET} -p udp --dport 53 -j DNAT --to-destination 127.0.0.1:${DNS2SOCKS_PORT}` },
    { check: `iptables -t nat -C PREROUTING -s ${L2TP_SUBNET} -p udp --dport 53 -j DNAT --to-destination 127.0.0.1:${DNS2SOCKS_PORT}`, add: `iptables -t nat -A PREROUTING -s ${L2TP_SUBNET} -p udp --dport 53 -j DNAT --to-destination 127.0.0.1:${DNS2SOCKS_PORT}` },
    // Allow redirected traffic to reach the local proxy listeners on hosts with INPUT DROP/UFW.
    { check: `iptables -C INPUT -s ${OPENVPN_SUBNET} -p tcp --dport ${REDSOCKS_PORT} -j ACCEPT`, add: `iptables -I INPUT -s ${OPENVPN_SUBNET} -p tcp --dport ${REDSOCKS_PORT} -j ACCEPT` },
    { check: `iptables -C INPUT -s ${L2TP_SUBNET} -p tcp --dport ${REDSOCKS_PORT} -j ACCEPT`, add: `iptables -I INPUT -s ${L2TP_SUBNET} -p tcp --dport ${REDSOCKS_PORT} -j ACCEPT` },
    { check: `iptables -C INPUT -s ${OPENVPN_SUBNET} -p udp --dport ${DNS2SOCKS_PORT} -j ACCEPT`, add: `iptables -I INPUT -s ${OPENVPN_SUBNET} -p udp --dport ${DNS2SOCKS_PORT} -j ACCEPT` },
    { check: `iptables -C INPUT -s ${L2TP_SUBNET} -p udp --dport ${DNS2SOCKS_PORT} -j ACCEPT`, add: `iptables -I INPUT -s ${L2TP_SUBNET} -p udp --dport ${DNS2SOCKS_PORT} -j ACCEPT` },
    // QUIC and other UDP traffic cannot traverse redsocks. Reject it so clients fall back to TCP.
    { check: `iptables -C FORWARD -s ${OPENVPN_SUBNET} -p udp -j REJECT --reject-with icmp-port-unreachable`, add: `iptables -I FORWARD -s ${OPENVPN_SUBNET} -p udp -j REJECT --reject-with icmp-port-unreachable` },
    { check: `iptables -C FORWARD -s ${L2TP_SUBNET} -p udp -j REJECT --reject-with icmp-port-unreachable`, add: `iptables -I FORWARD -s ${L2TP_SUBNET} -p udp -j REJECT --reject-with icmp-port-unreachable` },
    // FORWARD
    { check: `iptables -C FORWARD -s ${OPENVPN_SUBNET} -j ACCEPT`, add: `iptables -A FORWARD -s ${OPENVPN_SUBNET} -j ACCEPT` },
    { check: `iptables -C FORWARD -s ${L2TP_SUBNET} -j ACCEPT`, add: `iptables -A FORWARD -s ${L2TP_SUBNET} -j ACCEPT` },
    { check: `iptables -C FORWARD -m state --state RELATED,ESTABLISHED -j ACCEPT`, add: `iptables -A FORWARD -m state --state RELATED,ESTABLISHED -j ACCEPT` },
  ];

  // Sysctl settings first
  try { execSync('sysctl -w net.ipv4.ip_forward=1', { stdio: 'ignore' }); } catch (e) {}
  try { execSync('sysctl -w net.ipv4.conf.all.route_localnet=1', { stdio: 'ignore' }); } catch (e) {}
  rebuildVpnTransparentProxyChain();

  // Apply rules only if they don't already exist (prevents duplicates)
  for (const rule of rules) {
    try {
      execSync(`${rule.check} 2>/dev/null`, { stdio: 'ignore' });
      // Rule already exists, skip
    } catch (e) {
      // Rule doesn't exist, add it
      try { execSync(rule.add, { stdio: 'ignore' }); } catch (e2) {
        console.error('[iptables] Failed to add rule:', rule.add, e2.message);
        addRoutingLog(`iptables add failed: ${rule.add} (${e2.message})`);
      }
    }
  }
  vpnRoutingActive = true;
  addRoutingLog(`iptables rules applied (TCP MSS clamped to ${VPN_TCP_MSS})`);
  addRoutingLog('Non-DNS UDP is rejected so clients fall back to TCP through redsocks');
  console.log('[iptables] VPN routing rules applied (duplicate-safe)');
}

function flushVpnIptables() {
  // Delete rules in a loop to remove ALL duplicates (iptables -D only removes the first match)
  const rules = [
    `iptables -t nat -D POSTROUTING -s ${OPENVPN_SUBNET} -j MASQUERADE`,
    `iptables -t nat -D POSTROUTING -s ${L2TP_SUBNET} -j MASQUERADE`,
    `iptables -t mangle -D PREROUTING -s ${OPENVPN_SUBNET} -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --set-mss ${VPN_TCP_MSS}`,
    `iptables -t mangle -D PREROUTING -s ${L2TP_SUBNET} -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --set-mss ${VPN_TCP_MSS}`,
    `iptables -t nat -D PREROUTING -s ${OPENVPN_SUBNET} -p tcp -j ${VPN_REDSOCKS_TCP_CHAIN}`,
    `iptables -t nat -D PREROUTING -s ${L2TP_SUBNET} -p tcp -j ${VPN_REDSOCKS_TCP_CHAIN}`,
    `iptables -t nat -D PREROUTING -s ${OPENVPN_SUBNET} -p udp --dport 53 -j DNAT --to-destination 127.0.0.1:${DNS2SOCKS_PORT}`,
    `iptables -t nat -D PREROUTING -s ${L2TP_SUBNET} -p udp --dport 53 -j DNAT --to-destination 127.0.0.1:${DNS2SOCKS_PORT}`,
    `iptables -D INPUT -s ${OPENVPN_SUBNET} -p tcp --dport ${REDSOCKS_PORT} -j ACCEPT`,
    `iptables -D INPUT -s ${L2TP_SUBNET} -p tcp --dport ${REDSOCKS_PORT} -j ACCEPT`,
    `iptables -D INPUT -s ${OPENVPN_SUBNET} -p udp --dport ${DNS2SOCKS_PORT} -j ACCEPT`,
    `iptables -D INPUT -s ${L2TP_SUBNET} -p udp --dport ${DNS2SOCKS_PORT} -j ACCEPT`,
    `iptables -D FORWARD -s ${OPENVPN_SUBNET} -p udp -j REJECT --reject-with icmp-port-unreachable`,
    `iptables -D FORWARD -s ${L2TP_SUBNET} -p udp -j REJECT --reject-with icmp-port-unreachable`,
    `iptables -D FORWARD -s ${OPENVPN_SUBNET} -j ACCEPT`,
    `iptables -D FORWARD -s ${L2TP_SUBNET} -j ACCEPT`,
  ];
  for (const cmd of rules) {
    // Loop to remove ALL duplicates — keep deleting until the rule no longer exists
    for (let i = 0; i < 10; i++) {
      try { execSync(cmd, { stdio: 'ignore' }); } catch (e) { break; }
    }
  }
  try { runIptablesCommand(`iptables -t nat -F ${VPN_REDSOCKS_TCP_CHAIN}`); } catch {}
  try { runIptablesCommand(`iptables -t nat -X ${VPN_REDSOCKS_TCP_CHAIN}`); } catch {}
  vpnRoutingActive = false;
  addRoutingLog('iptables rules flushed');
  console.log('[iptables] VPN routing rules flushed (all duplicates removed)');
}

// ─── OpenVPN Server Management ─────────────────────────────────────

const OVPN_SERVER_USERS_FILE = path.join(__dirname, 'ovpn-server-users.json');
const OVPN_SERVER_SETTINGS_FILE = path.join(__dirname, 'ovpn-server-settings.json');
const EASYRSA_DIR = '/etc/openvpn/easy-rsa';

let ovpnServerProc = null;
let ovpnServerStatus = 'stopped';  // stopped | running | error
let ovpnServerUsers = [];
let ovpnServerSettings = { maxUsers: 0 }; // 0 = unlimited
let ovpnLastConnected = [];  // cached parsed status log
let ovpnServerRuntimeLogs = [];
const MAX_OVPN_SERVER_LOGS = 300;

function addOvpnServerRuntimeLog(text) {
  const entry = { time: Date.now(), text };
  ovpnServerRuntimeLogs.push(entry);
  if (ovpnServerRuntimeLogs.length > MAX_OVPN_SERVER_LOGS) {
    ovpnServerRuntimeLogs = ovpnServerRuntimeLogs.slice(-MAX_OVPN_SERVER_LOGS);
  }
  broadcast({ type: 'ovpn_server_log', entry });
}

function readLogTail(filePath, maxLines = 200) {
  try {
    const data = execSync(`tail -n ${maxLines} "${filePath}" 2>/dev/null || true`, {
      stdio: 'pipe',
      timeout: 3000,
      encoding: 'utf8',
    });
    return data.split('\n').filter(Boolean);
  } catch {
    return [];
  }
}

function ensureOvpnServerTransportTuning() {
  const filePath = '/etc/openvpn/server.conf';
  try {
    const original = fs.readFileSync(filePath, 'utf8');
    let updated = original;
    const linesToEnsure = [
      'tun-mtu 1280',
      `mssfix ${VPN_TCP_MSS}`,
    ];

    for (const line of linesToEnsure) {
      if (!updated.split('\n').some(existing => existing.trim() === line)) {
        updated += `${updated.endsWith('\n') ? '' : '\n'}${line}\n`;
      }
    }

    if (updated !== original) {
      fs.writeFileSync(filePath, updated);
      addOvpnServerRuntimeLog(`Patched /etc/openvpn/server.conf with tun-mtu 1280 and mssfix ${VPN_TCP_MSS}`);
    }
  } catch (e) {
    addOvpnServerRuntimeLog(`Failed to patch OpenVPN transport tuning: ${e.message}`);
  }
}

function runCommandCapture(cmd, timeout = 10000) {
  try {
    return {
      ok: true,
      output: execSync(cmd, { stdio: 'pipe', timeout, encoding: 'utf8' }).trim(),
    };
  } catch (e) {
    const stdout = e.stdout ? e.stdout.toString() : '';
    const stderr = e.stderr ? e.stderr.toString() : '';
    const output = [stdout, stderr, e.message].filter(Boolean).join('\n').trim();
    return { ok: false, output: output.slice(-2000) };
  }
}

function runRoutingDiagnostics() {
  const socksPort = getActiveSocksPort();
  const result = {
    time: Date.now(),
    socksPort,
    checks: [],
  };

  if (!socksPort) {
    result.error = 'No active SOCKS port detected';
    return result;
  }

  result.checks.push({
    name: 'SOCKS listener',
    ...runCommandCapture(`sh -lc 'ss -tln | grep -q ":${socksPort} " && echo "listening on ${socksPort}"'`, 4000),
  });
  result.checks.push({
    name: 'redsocks listener',
    ...runCommandCapture(`sh -lc 'ss -tln | grep -q ":${REDSOCKS_PORT} " && echo "listening on ${REDSOCKS_PORT}"'`, 4000),
  });
  result.checks.push({
    name: 'dns2socks listener',
    ...runCommandCapture(`sh -lc 'ss -uln | grep -q ":${DNS2SOCKS_PORT} " && echo "listening on ${DNS2SOCKS_PORT}"'`, 4000),
  });
  result.checks.push({
    name: 'HTTP via SOCKS',
    ...runCommandCapture(
      `curl --socks5-hostname 127.0.0.1:${socksPort} -L -sS -o /dev/null -w "http_code=%{http_code} remote_ip=%{remote_ip} time_total=%{time_total}\\n" --max-time 12 http://neverssl.com/`,
      15000
    ),
  });
  result.checks.push({
    name: 'HTTPS via SOCKS',
    ...runCommandCapture(
      `curl --socks5-hostname 127.0.0.1:${socksPort} -L -sS -o /dev/null -w "http_code=%{http_code} remote_ip=%{remote_ip} time_total=%{time_total}\\n" --max-time 12 https://example.com/`,
      15000
    ),
  });

  return result;
}

function loadOvpnServerUsers() {
  try {
    if (fs.existsSync(OVPN_SERVER_USERS_FILE)) {
      ovpnServerUsers = JSON.parse(fs.readFileSync(OVPN_SERVER_USERS_FILE, 'utf8'));
    }
  } catch (e) {
    ovpnServerUsers = [];
  }
}
function saveOvpnServerUsers() {
  fs.writeFileSync(OVPN_SERVER_USERS_FILE, JSON.stringify(ovpnServerUsers, null, 2));
}
function loadOvpnServerSettings() {
  try {
    if (fs.existsSync(OVPN_SERVER_SETTINGS_FILE)) {
      ovpnServerSettings = { maxUsers: 0, ...JSON.parse(fs.readFileSync(OVPN_SERVER_SETTINGS_FILE, 'utf8')) };
    }
  } catch (e) {}
}
function saveOvpnServerSettings() {
  fs.writeFileSync(OVPN_SERVER_SETTINGS_FILE, JSON.stringify(ovpnServerSettings, null, 2));
}
loadOvpnServerUsers();
loadOvpnServerSettings();

function ensureOvpnFirewall() {
  const rules = [
    'iptables -C INPUT -p udp --dport 1194 -j ACCEPT 2>/dev/null || iptables -I INPUT -p udp --dport 1194 -j ACCEPT',
    'iptables -t nat -C POSTROUTING -s 10.8.0.0/24 -j MASQUERADE 2>/dev/null || iptables -t nat -A POSTROUTING -s 10.8.0.0/24 -j MASQUERADE',
    'iptables -C FORWARD -s 10.8.0.0/24 -j ACCEPT 2>/dev/null || iptables -A FORWARD -s 10.8.0.0/24 -j ACCEPT',
    'iptables -C FORWARD -m state --state RELATED,ESTABLISHED -j ACCEPT 2>/dev/null || iptables -A FORWARD -m state --state RELATED,ESTABLISHED -j ACCEPT',
    'sysctl -w net.ipv4.ip_forward=1',
    // Required for DNAT to 127.0.0.1 (dns2socks) to work from VPN subnets
    'sysctl -w net.ipv4.conf.all.route_localnet=1',
  ];
  for (const cmd of rules) {
    try { execSync(cmd, { stdio: 'ignore', timeout: 5000 }); } catch (e) {}
  }
}

function startOvpnServer() {
  if (ovpnServerProc) return { ok: false, error: 'Already running' };
  ovpnServerRuntimeLogs = [];
  addOvpnServerRuntimeLog('Starting OpenVPN server');

  // Kill any stale openvpn server processes
  try { execSync('killall -q openvpn 2>/dev/null || true', { stdio: 'ignore', timeout: 3000 }); } catch (e) {}
  // Wait a moment for port to be released
  try { execSync('sleep 0.5', { stdio: 'ignore', timeout: 3000 }); } catch (e) {}

  // Verify config file exists
  if (!fs.existsSync('/etc/openvpn/server.conf')) {
    ovpnServerStatus = 'error';
    addOvpnServerRuntimeLog('OpenVPN config not found at /etc/openvpn/server.conf');
    broadcastVpnServerState();
    return { ok: false, error: 'OpenVPN config not found. Run the install script first.' };
  }

  ensureOvpnServerTransportTuning();
  ensureOvpnFirewall();

  // Clear the log file so we can read fresh startup errors
  try { fs.writeFileSync('/var/log/openvpn.log', '', { flag: 'w' }); } catch (e) {}

  ovpnServerProc = spawn('openvpn', ['--config', '/etc/openvpn/server.conf'], {
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let startupError = null;

  ovpnServerProc.stdout.on('data', d => {
    const lines = d.toString().split('\n').map(s => s.trim()).filter(Boolean);
    lines.forEach(line => {
      console.log('[OpenVPN-Server]', line);
      addOvpnServerRuntimeLog(line);
    });
  });
  ovpnServerProc.stderr.on('data', d => {
    const lines = d.toString().split('\n').map(s => s.trim()).filter(Boolean);
    lines.forEach(line => {
      console.log('[OpenVPN-Server]', line);
      addOvpnServerRuntimeLog(line);
      startupError = line;
    });
  });

  ovpnServerProc.on('error', (err) => {
    console.error('[OpenVPN-Server] Spawn error:', err.message);
    addOvpnServerRuntimeLog(`Spawn error: ${err.message}`);
    ovpnServerStatus = 'error';
    ovpnServerProc = null;
    broadcastVpnServerState();
  });

  ovpnServerProc.on('close', (code) => {
    console.log(`[OpenVPN-Server] Exited with code ${code}`);
    addOvpnServerRuntimeLog(`OpenVPN process exited with code ${code}`);
    // Read last lines of log for error details
    if (code !== 0) {
      try {
        const logTail = execSync('tail -10 /var/log/openvpn.log 2>/dev/null || true', { stdio: 'pipe', timeout: 3000 }).toString().trim();
        if (logTail) {
          console.error('[OpenVPN-Server] Log tail:', logTail);
          logTail.split('\n').map(s => s.trim()).filter(Boolean).forEach(line => addOvpnServerRuntimeLog(line));
        }
      } catch (e) {}
    }
    ovpnServerProc = null;
    ovpnServerStatus = code === 0 ? 'stopped' : 'error';
    broadcastVpnServerState();
  });

  // Wait briefly to verify the process didn't crash immediately
  try { execSync('sleep 1', { stdio: 'ignore', timeout: 3000 }); } catch (e) {}

  if (!ovpnServerProc || ovpnServerProc.exitCode !== null) {
    // Process already exited — it crashed on startup
    let errMsg = startupError || 'OpenVPN exited immediately';
    try {
      const logTail = execSync('tail -10 /var/log/openvpn.log 2>/dev/null || true', { stdio: 'pipe', timeout: 3000 }).toString().trim();
      if (logTail) errMsg = logTail;
    } catch (e) {}
    ovpnServerProc = null;
    ovpnServerStatus = 'error';
    addOvpnServerRuntimeLog(`Startup failed: ${errMsg}`);
    broadcastVpnServerState();
    return { ok: false, error: errMsg };
  }

  ovpnServerStatus = 'running';
  addOvpnServerRuntimeLog('OpenVPN server is running');
  addAudit('ovpn_server_started', 'OpenVPN server started');
  broadcastVpnServerState();
  return { ok: true };
}

function stopOvpnServer() {
  if (!ovpnServerProc) return { ok: false, error: 'Not running' };
  addOvpnServerRuntimeLog('Stopping OpenVPN server');
  try { ovpnServerProc.kill('SIGTERM'); } catch (e) {}
  // Wait for process to actually exit so the port is released
  try { execSync('sleep 1', { stdio: 'ignore', timeout: 3000 }); } catch (e) {}
  // If still alive, force kill
  if (ovpnServerProc && ovpnServerProc.exitCode === null) {
    try { ovpnServerProc.kill('SIGKILL'); } catch (e) {}
  }
  // Also kill any orphaned openvpn processes (safety net)
  try { execSync('killall -q openvpn 2>/dev/null || true', { stdio: 'ignore', timeout: 3000 }); } catch (e) {}
  ovpnServerProc = null;
  ovpnServerStatus = 'stopped';
  addOvpnServerRuntimeLog('OpenVPN server stopped');
  addAudit('ovpn_server_stopped', 'OpenVPN server stopped');
  broadcastVpnServerState();
  return { ok: true };
}

function parseOvpnStatusLog() {
  try {
    const data = fs.readFileSync('/var/log/openvpn-status.log', 'utf8');
    const clients = [];
    let inClients = false;
    for (const line of data.split('\n')) {
      if (line.startsWith('Common Name,')) { inClients = true; continue; }
      if (line.startsWith('ROUTING TABLE')) { inClients = false; continue; }
      if (inClients && line.trim()) {
        const [name, realAddr, bytesRecv, bytesSent, connSince] = line.split(',');
        if (name && realAddr) {
          clients.push({ name, realAddr, bytesRecv: parseInt(bytesRecv) || 0, bytesSent: parseInt(bytesSent) || 0, connectedSince: connSince });
        }
      }
    }
    ovpnLastConnected = clients;
    return clients;
  } catch (e) {
    return [];
  }
}

// Enrich users with online status and cumulative traffic from status log
function getOvpnUsersWithStatus() {
  const connected = parseOvpnStatusLog();
  return ovpnServerUsers.map(u => {
    const conn = connected.find(c => c.name === u.name);
    const isExpired = u.expiresAt && u.expiresAt < Date.now();
    const isOverBandwidth = u.bandwidthLimit && u.usedTraffic >= u.bandwidthLimit;
    return {
      ...u,
      online: !!conn,
      clientIp: conn ? conn.realAddr : null,
      sessionBytesRecv: conn ? conn.bytesRecv : 0,
      sessionBytesSent: conn ? conn.bytesSent : 0,
      expired: isExpired,
      overBandwidth: isOverBandwidth,
    };
  });
}

// Track OpenVPN user traffic from status log diffs
let ovpnTrafficSnapshot = {};  // { userName: { bytesRecv, bytesSent } }

function trackOvpnTraffic() {
  const connected = ovpnLastConnected.length ? ovpnLastConnected : parseOvpnStatusLog();
  let changed = false;
  for (const c of connected) {
    const prev = ovpnTrafficSnapshot[c.name] || { bytesRecv: 0, bytesSent: 0 };
    // Only add delta if current > prev (status log resets on reconnect)
    const deltaRecv = c.bytesRecv > prev.bytesRecv ? c.bytesRecv - prev.bytesRecv : c.bytesRecv;
    const deltaSent = c.bytesSent > prev.bytesSent ? c.bytesSent - prev.bytesSent : c.bytesSent;

    if (deltaRecv > 0 || deltaSent > 0) {
      const user = ovpnServerUsers.find(u => u.name === c.name);
      if (user) {
        user.usedTraffic = (user.usedTraffic || 0) + deltaRecv + deltaSent;
        changed = true;
      }
    }
    ovpnTrafficSnapshot[c.name] = { bytesRecv: c.bytesRecv, bytesSent: c.bytesSent };
  }
  if (changed) saveOvpnServerUsers();

  // Enforce: kill connections for expired/over-bandwidth users via management
  enforceOvpnLimits();
}

function enforceOvpnLimits() {
  const connected = ovpnLastConnected;
  for (const c of connected) {
    const user = ovpnServerUsers.find(u => u.name === c.name);
    if (!user) continue;
    const expired = user.expiresAt && user.expiresAt < Date.now();
    const overBw = user.bandwidthLimit && (user.usedTraffic || 0) >= user.bandwidthLimit;
    const disabled = !user.enabled;
    if (expired || overBw || disabled) {
      // Kill client by revoking and reloading CRL — or use management interface
      // For now, log it. A proper kill requires openvpn management interface.
      console.log(`[OpenVPN-Server] User ${c.name} should be disconnected: expired=${expired} overBw=${overBw} disabled=${disabled}`);
    }
  }

  // Enforce max users limit
  if (ovpnServerSettings.maxUsers > 0 && connected.length > ovpnServerSettings.maxUsers) {
    console.log(`[OpenVPN-Server] Max users (${ovpnServerSettings.maxUsers}) exceeded: ${connected.length} connected`);
  }
}

function generateOvpnClientConfig(clientName, serverIp) {
  try {
    const ca = fs.readFileSync(`${EASYRSA_DIR}/pki/ca.crt`, 'utf8');
    const cert = fs.readFileSync(`${EASYRSA_DIR}/pki/issued/${clientName}.crt`, 'utf8');
    const key = fs.readFileSync(`${EASYRSA_DIR}/pki/private/${clientName}.key`, 'utf8');
    const ta = fs.readFileSync(`${EASYRSA_DIR}/ta.key`, 'utf8');

    return `client
dev tun
proto udp
remote ${serverIp} 1194
resolv-retry infinite
nobind
persist-key
persist-tun
tun-mtu 1280
mssfix 1200
remote-cert-tls server
cipher AES-256-GCM
data-ciphers AES-256-GCM:AES-128-GCM:CHACHA20-POLY1305
key-direction 1
verb 3
<ca>
${ca.trim()}
</ca>
<cert>
${cert.trim()}
</cert>
<key>
${key.trim()}
</key>
<tls-auth>
${ta.trim()}
</tls-auth>`;
  } catch (e) {
    console.error('[OpenVPN-Server] Config gen error:', e.message);
    return null;
  }
}

// ─── OpenVPN Server API ─────────────────────────────────────────────

app.get('/api/ovpn-server/status', (req, res) => {
  res.json({
    status: ovpnServerStatus,
    connectedClients: parseOvpnStatusLog(),
    usersCount: ovpnServerUsers.length,
    maxUsers: ovpnServerSettings.maxUsers,
    redsocks: redsocksStatus,
    vpnRouting: vpnRoutingActive,
    socksPort: getActiveSocksPort(),
    socksSources: getRoutingSocksSources(),
  });
});

app.get('/api/ovpn-server/logs', (req, res) => {
  res.json({
    runtime: ovpnServerRuntimeLogs,
    fileTail: readLogTail('/var/log/openvpn.log', 200),
  });
});

app.post('/api/ovpn-server/logs/clear', (req, res) => {
  ovpnServerRuntimeLogs = [];
  try {
    fs.writeFileSync('/var/log/openvpn.log', '', { flag: 'w' });
  } catch (e) {
    return res.status(500).json({ error: `Failed to clear /var/log/openvpn.log: ${e.message}` });
  }
  res.json({ ok: true });
});

app.post('/api/ovpn-server/start', (req, res) => {
  res.json(startOvpnServer());
});

app.post('/api/ovpn-server/stop', (req, res) => {
  res.json(stopOvpnServer());
});

app.get('/api/ovpn-server/settings', (req, res) => {
  res.json(ovpnServerSettings);
});

app.post('/api/ovpn-server/settings', (req, res) => {
  if (req.body.maxUsers !== undefined) ovpnServerSettings.maxUsers = parseInt(req.body.maxUsers) || 0;
  saveOvpnServerSettings();
  broadcastVpnServerState();
  res.json(ovpnServerSettings);
});

app.get('/api/ovpn-server/users', (req, res) => {
  res.json(getOvpnUsersWithStatus());
});

app.post('/api/ovpn-server/users', (req, res) => {
  const { name, expiresAt, bandwidthLimit, speedLimit } = req.body;
  if (!name) return res.status(400).json({ error: 'Name is required' });

  // Sanitize name (alphanumeric + dash + underscore only)
  const safeName = name.replace(/[^a-zA-Z0-9_-]/g, '');
  if (!safeName) return res.status(400).json({ error: 'Invalid name' });

  if (ovpnServerUsers.find(u => u.name === safeName)) {
    return res.status(400).json({ error: 'User already exists' });
  }

  // Check max users limit
  if (ovpnServerSettings.maxUsers > 0 && ovpnServerUsers.length >= ovpnServerSettings.maxUsers) {
    return res.status(400).json({ error: `Max users limit (${ovpnServerSettings.maxUsers}) reached` });
  }

  try {
    execSync(`cd ${EASYRSA_DIR} && ./easyrsa build-client-full ${safeName} nopass`, {
      stdio: 'pipe', env: { ...process.env, EASYRSA_BATCH: '1' }
    });

    const user = {
      id: genId(),
      name: safeName,
      enabled: true,
      expiresAt: expiresAt ? new Date(expiresAt).getTime() : null,       // null = never
      bandwidthLimit: bandwidthLimit ? parseInt(bandwidthLimit) : null,    // bytes, null = unlimited
      usedTraffic: 0,
      speedLimit: speedLimit ? parseInt(speedLimit) : null,               // bytes/s, null = unlimited
      createdAt: Date.now(),
    };
    ovpnServerUsers.push(user);
    saveOvpnServerUsers();
    addAudit('ovpn_user_created', safeName);
    broadcastVpnServerState();
    res.json(user);
  } catch (e) {
    res.status(500).json({ error: 'Failed to create cert: ' + e.message });
  }
});

app.put('/api/ovpn-server/users/:id', (req, res) => {
  const user = ovpnServerUsers.find(u => u.id === req.params.id);
  if (!user) return res.status(404).json({ error: 'User not found' });

  if (req.body.enabled !== undefined) user.enabled = req.body.enabled;
  if (req.body.expiresAt !== undefined) user.expiresAt = req.body.expiresAt ? new Date(req.body.expiresAt).getTime() : null;
  if (req.body.bandwidthLimit !== undefined) user.bandwidthLimit = req.body.bandwidthLimit ? parseInt(req.body.bandwidthLimit) : null;
  if (req.body.speedLimit !== undefined) user.speedLimit = req.body.speedLimit ? parseInt(req.body.speedLimit) : null;
  if (req.body.resetTraffic) user.usedTraffic = 0;

  saveOvpnServerUsers();
  broadcastVpnServerState();
  res.json(user);
});

app.delete('/api/ovpn-server/users/:id', (req, res) => {
  const user = ovpnServerUsers.find(u => u.id === req.params.id);
  if (!user) return res.status(404).json({ error: 'User not found' });

  try {
    execSync(`cd ${EASYRSA_DIR} && ./easyrsa revoke ${user.name} && ./easyrsa gen-crl`, {
      stdio: 'pipe', input: 'yes\n', env: { ...process.env, EASYRSA_BATCH: '1' }
    });
  } catch (e) {
    console.error('[OpenVPN-Server] Revoke error:', e.message);
  }

  ovpnServerUsers = ovpnServerUsers.filter(u => u.id !== req.params.id);
  saveOvpnServerUsers();
  addAudit('ovpn_user_revoked', user.name);
  broadcastVpnServerState();
  res.json({ ok: true });
});

app.get('/api/ovpn-server/users/:id/config', (req, res) => {
  const user = ovpnServerUsers.find(u => u.id === req.params.id);
  if (!user) return res.status(404).json({ error: 'User not found' });

  const { serverIp } = req.query;
  if (!serverIp) return res.status(400).json({ error: 'serverIp query param required' });

  const config = generateOvpnClientConfig(user.name, serverIp);
  if (!config) return res.status(500).json({ error: 'Failed to generate config' });

  res.setHeader('Content-Type', 'application/x-openvpn-profile');
  res.setHeader('Content-Disposition', `attachment; filename="${user.name}.ovpn"`);
  res.send(config);
});

// ─── L2TP/IPsec Server Management ──────────────────────────────────

const L2TP_USERS_FILE = path.join(__dirname, 'l2tp-users.json');
const L2TP_SETTINGS_FILE = path.join(__dirname, 'l2tp-settings.json');
const IPSEC_CONF_FILE = '/etc/ipsec.conf';
const CHAP_SECRETS_FILE = '/etc/ppp/chap-secrets';
const IPSEC_SECRETS_FILE = '/etc/ipsec.secrets';
const XL2TPD_CONF_FILE = '/etc/xl2tpd/xl2tpd.conf';
const PPP_OPTIONS_FILE = '/etc/ppp/options.xl2tpd';
const L2TP_SERVER_NAME = 'l2tpd';

let l2tpStatus = 'stopped';  // stopped | running | error
let l2tpUsers = [];
let l2tpSettings = { maxUsers: 0 }; // 0 = unlimited
let l2tpLastConnected = [];

function loadL2tpUsers() {
  try {
    if (fs.existsSync(L2TP_USERS_FILE)) {
      l2tpUsers = JSON.parse(fs.readFileSync(L2TP_USERS_FILE, 'utf8'));
    }
  } catch (e) {
    l2tpUsers = [];
  }
}
function saveL2tpUsers() {
  fs.writeFileSync(L2TP_USERS_FILE, JSON.stringify(l2tpUsers, null, 2));
}
function loadL2tpSettings() {
  try {
    if (fs.existsSync(L2TP_SETTINGS_FILE)) {
      l2tpSettings = { maxUsers: 0, ...JSON.parse(fs.readFileSync(L2TP_SETTINGS_FILE, 'utf8')) };
    }
  } catch (e) {}
}
function saveL2tpSettings() {
  fs.writeFileSync(L2TP_SETTINGS_FILE, JSON.stringify(l2tpSettings, null, 2));
}
loadL2tpUsers();
loadL2tpSettings();

function buildManagedIpsecConf() {
  return `# Managed by VPN Panel - L2TP/IPsec
config setup
    charondebug="ike 1, knl 1, cfg 0"
    uniqueids=no
    sha2-truncbug=yes

conn L2TP-PSK
    keyexchange=ikev1
    authby=secret
    auto=add
    keyingtries=3
    rekey=no
    ikelifetime=8h
    keylife=1h
    type=transport
    fragmentation=yes
    left=%defaultroute
    leftprotoport=17/1701
    right=%any
    rightprotoport=17/%any
    forceencaps=yes
    dpddelay=30
    dpdtimeout=120
    dpdaction=clear
    ike=aes256-sha1-modp1024,aes128-sha1-modp1024,aes256-sha256-modp2048,aes128-sha256-modp2048,3des-sha1-modp1024!
    esp=aes256-sha1,aes128-sha1,aes256-sha256,3des-sha1!
`;
}

function buildManagedXl2tpdConf() {
  return `[global]
port = 1701

[lns default]
ip range = 10.9.0.10-10.9.0.250
local ip = 10.9.0.1
require chap = yes
refuse pap = yes
require authentication = yes
name = ${L2TP_SERVER_NAME}
pppoptfile = ${PPP_OPTIONS_FILE}
length bit = yes
`;
}

function buildManagedPppOptions() {
  return `# Managed by VPN Panel - L2TP PPP
ipcp-accept-local
ipcp-accept-remote
require-mschap-v2
refuse-eap
refuse-pap
refuse-chap
refuse-mschap
name ${L2TP_SERVER_NAME}
ms-dns 1.1.1.1
ms-dns 8.8.8.8
noccp
nodefaultroute
auth
mtu 1280
mru 1280
proxyarp
asyncmap 0
hide-password
lock
lcp-echo-failure 4
lcp-echo-interval 30
connect-delay 5000
logfile /var/log/pppd.log
`;
}

function writeManagedTextFile(filePath, content, mode = 0o644) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, content);
  fs.chmodSync(filePath, mode);
}

function quoteChapField(value) {
  return `"${String(value == null ? '' : value).replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
}

function writeChapSecrets() {
  const lines = ['# Managed by VPN Panel - do not edit manually'];
  const now = Date.now();
  for (const u of l2tpUsers) {
    const expired = u.expiresAt && u.expiresAt < now;
    const overBw = u.bandwidthLimit && (u.usedTraffic || 0) >= u.bandwidthLimit;
    if (u.enabled && !expired && !overBw) {
      lines.push(`${quoteChapField(u.username)}\t*\t${quoteChapField(u.password)}\t*`);
    }
  }
  try {
    writeManagedTextFile(CHAP_SECRETS_FILE, lines.join('\n') + '\n', 0o600);
  } catch (e) {
    console.error('[L2TP] Failed to write chap-secrets:', e.message);
  }
}

function getL2tpPsk() {
  try {
    const data = fs.readFileSync(IPSEC_SECRETS_FILE, 'utf8');
    const match = data.match(/PSK\s+"([^"]+)"/);
    return match ? match[1] : null;
  } catch (e) {
    return null;
  }
}

function ensureL2tpFirewall() {
  // Ensure iptables INPUT rules for L2TP/IPsec are in place
  const rules = [
    'iptables -C INPUT -p udp --dport 500 -j ACCEPT 2>/dev/null || iptables -I INPUT -p udp --dport 500 -j ACCEPT',
    'iptables -C INPUT -p udp --dport 4500 -j ACCEPT 2>/dev/null || iptables -I INPUT -p udp --dport 4500 -j ACCEPT',
    'iptables -C INPUT -p udp --dport 1701 -j ACCEPT 2>/dev/null || iptables -I INPUT -p udp --dport 1701 -j ACCEPT',
    'iptables -C INPUT -p esp -j ACCEPT 2>/dev/null || iptables -I INPUT -p esp -j ACCEPT',
    'iptables -C INPUT -m state --state RELATED,ESTABLISHED -j ACCEPT 2>/dev/null || iptables -I INPUT -m state --state RELATED,ESTABLISHED -j ACCEPT',
    // MASQUERADE for L2TP clients (so they have internet even without redsocks routing)
    'iptables -t nat -C POSTROUTING -s 10.9.0.0/24 -j MASQUERADE 2>/dev/null || iptables -t nat -A POSTROUTING -s 10.9.0.0/24 -j MASQUERADE',
    // Clamp MSS so client traffic survives L2TP/IPsec overhead reliably.
    `iptables -t mangle -C FORWARD -s ${L2TP_SUBNET} -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --set-mss ${VPN_TCP_MSS} 2>/dev/null || iptables -t mangle -A FORWARD -s ${L2TP_SUBNET} -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --set-mss ${VPN_TCP_MSS}`,
    // FORWARD rules for L2TP traffic
    'iptables -C FORWARD -s 10.9.0.0/24 -j ACCEPT 2>/dev/null || iptables -A FORWARD -s 10.9.0.0/24 -j ACCEPT',
    'iptables -C FORWARD -m state --state RELATED,ESTABLISHED -j ACCEPT 2>/dev/null || iptables -A FORWARD -m state --state RELATED,ESTABLISHED -j ACCEPT',
    'sysctl -w net.ipv4.ip_forward=1',
    // Required for DNAT to 127.0.0.1 (dns2socks) to work from VPN subnets
    'sysctl -w net.ipv4.conf.all.route_localnet=1',
  ];
  for (const cmd of rules) {
    try { execSync(cmd, { stdio: 'ignore', timeout: 5000 }); } catch (e) {}
  }
}

// Ensure /etc/ipsec.conf has the L2TP-PSK connection (writes it if missing)
function ensureIpsecConf() {
  const ipsecConf = buildManagedIpsecConf();
  const requiredSnippets = [
    'conn L2TP-PSK',
    'keyexchange=ikev1',
    'leftprotoport=17/1701',
    'rightprotoport=17/%any',
    'forceencaps=yes',
    'sha2-truncbug=yes',
  ];
  try {
    let needsWrite = false;
    if (fs.existsSync(IPSEC_CONF_FILE)) {
      const current = fs.readFileSync(IPSEC_CONF_FILE, 'utf8');
      if (!requiredSnippets.every(snippet => current.includes(snippet))) {
        console.log('[L2TP] /etc/ipsec.conf is missing required L2TP settings, rewriting');
        needsWrite = true;
      }
    } else {
      console.log('[L2TP] /etc/ipsec.conf not found, creating');
      needsWrite = true;
    }
    if (needsWrite) {
      writeManagedTextFile(IPSEC_CONF_FILE, ipsecConf);
    }
  } catch (e) {
    console.error('[L2TP] Failed to write ipsec.conf:', e.message);
  }
}

function ensureXl2tpdConf() {
  try {
    const desired = buildManagedXl2tpdConf();
    const current = fs.existsSync(XL2TPD_CONF_FILE) ? fs.readFileSync(XL2TPD_CONF_FILE, 'utf8') : '';
    if (current !== desired) {
      writeManagedTextFile(XL2TPD_CONF_FILE, desired);
    }
  } catch (e) {
    console.error('[L2TP] Failed to write xl2tpd.conf:', e.message);
  }
}

function ensureL2tpPppOptions() {
  try {
    const desired = buildManagedPppOptions();
    const current = fs.existsSync(PPP_OPTIONS_FILE) ? fs.readFileSync(PPP_OPTIONS_FILE, 'utf8') : '';
    if (current !== desired) {
      writeManagedTextFile(PPP_OPTIONS_FILE, desired);
    }
  } catch (e) {
    console.error('[L2TP] Warning: could not write PPP options:', e.message);
  }
}

function ensureL2tpPsk() {
  let psk = getL2tpPsk();
  try {
    if (!psk) {
      psk = crypto.randomBytes(16).toString('hex');
      writeManagedTextFile(IPSEC_SECRETS_FILE, `: PSK "${psk}"\n`, 0o600);
      console.log('[L2TP] Generated new IPsec PSK');
    } else if (fs.existsSync(IPSEC_SECRETS_FILE)) {
      fs.chmodSync(IPSEC_SECRETS_FILE, 0o600);
    }
  } catch (e) {
    console.error('[L2TP] Warning: could not ensure ipsec.secrets:', e.message);
  }
  return psk;
}

function hasUnsafeChapField(value) {
  return /[\r\n]/.test(String(value == null ? '' : value));
}

function startL2tp() {
  try {
    // Kill any stale processes first
    try { execSync('killall -q charon 2>/dev/null || true', { stdio: 'ignore', timeout: 3000 }); } catch (e) {}
    try { execSync('systemctl stop xl2tpd 2>/dev/null || true', { stdio: 'ignore', timeout: 5000 }); } catch (e) {}
    try { execSync('ipsec stop 2>/dev/null || true', { stdio: 'ignore', timeout: 5000 }); } catch (e) {}

    // Load L2TP kernel modules (required for xl2tpd)
    try { execSync('modprobe l2tp_ppp', { stdio: 'pipe', timeout: 5000 }); } catch (e) {}
    try { execSync('modprobe pppol2tp', { stdio: 'pipe', timeout: 5000 }); } catch (e) {}

    ensureXl2tpdConf();
    ensureL2tpPppOptions();

    // Write chap-secrets to ensure users are up to date
    writeChapSecrets();

    // Ensure ipsec.conf has L2TP-PSK connection (fixes missing connection issue)
    ensureIpsecConf();

    // Ensure ipsec.secrets exists with a valid PSK
    ensureL2tpPsk();

    // Ensure firewall rules are in place
    ensureL2tpFirewall();

    // Start ipsec (StrongSwan charon)
    execSync('ipsec restart', { stdio: 'pipe', timeout: 15000 });

    // Wait for charon to be ready (listening on UDP 500)
    let ipsecReady = false;
    for (let i = 0; i < 10; i++) {
      try {
        execSync('sleep 0.5', { stdio: 'ignore', timeout: 3000 });
        const out = execSync('ss -ulnp | grep ":500 "', { stdio: 'pipe', timeout: 3000 }).toString();
        if (out.trim()) { ipsecReady = true; break; }
      } catch (e) {}
    }
    if (!ipsecReady) {
      console.error('[L2TP] Warning: IPsec (charon) not listening on UDP 500 after 5s');
    }

    // Verify L2TP-PSK connection is loaded in StrongSwan
    let connLoaded = false;
    try {
      const statusOut = execSync('ipsec statusall 2>/dev/null || true', { stdio: 'pipe', timeout: 5000 }).toString();
      connLoaded = statusOut.includes('L2TP-PSK');
      if (!connLoaded) {
        console.log('[L2TP] L2TP-PSK connection not loaded, forcing reload...');
        try { execSync('ipsec rereadsecrets', { stdio: 'pipe', timeout: 5000 }); } catch (e) {}
        try { execSync('ipsec reload', { stdio: 'pipe', timeout: 5000 }); } catch (e) {}
        // Check again
        try { execSync('sleep 1', { stdio: 'ignore', timeout: 3000 }); } catch (e) {}
        const statusOut2 = execSync('ipsec statusall 2>/dev/null || true', { stdio: 'pipe', timeout: 5000 }).toString();
        connLoaded = statusOut2.includes('L2TP-PSK');
        if (!connLoaded) {
          console.error('[L2TP] WARNING: L2TP-PSK connection still not loaded. Check /etc/ipsec.conf');
        }
      }
    } catch (e) {
      console.error('[L2TP] Could not verify ipsec connections:', e.message);
    }

    // Now start xl2tpd
    execSync('systemctl restart xl2tpd', { stdio: 'pipe', timeout: 10000 });

    // Verify xl2tpd is running
    try {
      execSync('sleep 1', { stdio: 'ignore', timeout: 3000 });
      execSync('systemctl is-active xl2tpd', { stdio: 'pipe', timeout: 3000 });
    } catch (e) {
      // Try to get error details
      let errMsg = 'xl2tpd failed to start';
      try {
        errMsg = execSync('journalctl -u xl2tpd --no-pager -n 5 2>/dev/null || echo "check logs"', { stdio: 'pipe', timeout: 3000 }).toString().trim();
      } catch (e2) {}
      console.error('[L2TP] xl2tpd not active:', errMsg);
      l2tpStatus = 'error';
      broadcastVpnServerState();
      return { ok: false, error: errMsg };
    }

    l2tpStatus = 'running';
    addAudit('l2tp_started', 'L2TP/IPsec started');
    broadcastVpnServerState();
    return { ok: true, ipsecReady };
  } catch (e) {
    console.error('[L2TP] Start error:', e.message);
    l2tpStatus = 'error';
    broadcastVpnServerState();
    return { ok: false, error: e.message };
  }
}

function stopL2tp() {
  try {
    execSync('systemctl stop xl2tpd', { stdio: 'pipe', timeout: 10000 });
    execSync('ipsec stop', { stdio: 'pipe', timeout: 10000 });
  } catch (e) {}
  l2tpStatus = 'stopped';
  addAudit('l2tp_stopped', 'L2TP/IPsec stopped');
  broadcastVpnServerState();
  return { ok: true };
}

function getL2tpConnectedClients() {
  try {
    // Extract peer IPs from ppp interfaces (format: "inet 10.9.0.1 peer 10.9.0.10/32")
    const output = execSync("ip -o -4 addr show | grep ppp | grep -oP 'peer \\K[\\d.]+'", {
      stdio: 'pipe', timeout: 5000,
    }).toString().trim();
    const ips = output ? output.split('\n').filter(Boolean) : [];

    // Get ppp interface names to match with usernames
    let pppUsers = [];
    try {
      // pppd logs authenticated usernames to auth.log / syslog
      const who = execSync("grep 'pppd.*CHAP.*succeeded' /var/log/auth.log 2>/dev/null | tail -20 | grep -oP 'peer\\s+\\K\\S+' || grep 'pppd.*CHAP.*succeeded' /var/log/syslog 2>/dev/null | tail -20 | grep -oP 'peer\\s+\\K\\S+' || true", {
        stdio: 'pipe', timeout: 5000,
      }).toString().trim();
      pppUsers = who ? who.split('\n').filter(Boolean) : [];
    } catch (e) {}

    // Match IPs to usernames (best effort — use last N usernames for N connections)
    const recentUsers = pppUsers.slice(-ips.length);
    l2tpLastConnected = ips.map((ip, i) => ({
      ip,
      username: recentUsers[i] || null,
    }));
    return l2tpLastConnected;
  } catch (e) {
    return [];
  }
}

function getL2tpUsersWithStatus() {
  const connected = l2tpLastConnected.length ? l2tpLastConnected : getL2tpConnectedClients();
  return l2tpUsers.map(u => {
    const conn = connected.find(c => c.username === u.username);
    const isExpired = u.expiresAt && u.expiresAt < Date.now();
    const isOverBandwidth = u.bandwidthLimit && (u.usedTraffic || 0) >= u.bandwidthLimit;
    return {
      ...u,
      password: '***',
      online: !!conn,
      clientIp: conn ? conn.ip : null,
      expired: isExpired,
      overBandwidth: isOverBandwidth,
    };
  });
}

// Track L2TP traffic via iptables byte counters for ppp interfaces
function trackL2tpTraffic() {
  if (l2tpStatus !== 'running') return;
  try {
    // Use iptables accounting for the L2TP subnet
    const output = execSync("iptables -L FORWARD -v -n -x 2>/dev/null | grep '10.9.0' || true", {
      stdio: 'pipe', timeout: 5000,
    }).toString().trim();
    // Simple estimation: count bytes from connected clients
    const connected = getL2tpConnectedClients();
    // For now, track based on connected time (rough estimation)
    // A proper implementation would use per-user iptables chains
  } catch (e) {}
}

// ─── L2TP API ───────────────────────────────────────────────────────

app.get('/api/l2tp/status', (req, res) => {
  res.json({
    status: l2tpStatus,
    connectedClients: getL2tpConnectedClients(),
    usersCount: l2tpUsers.length,
    maxUsers: l2tpSettings.maxUsers,
    psk: getL2tpPsk(),
  });
});

app.post('/api/l2tp/start', (req, res) => {
  res.json(startL2tp());
});

app.post('/api/l2tp/stop', (req, res) => {
  res.json(stopL2tp());
});

app.get('/api/l2tp/users', (req, res) => {
  res.json(getL2tpUsersWithStatus());
});

app.post('/api/l2tp/users', (req, res) => {
  const username = String(req.body.username || '').trim();
  const password = String(req.body.password || '');
  const { expiresAt, bandwidthLimit, speedLimit } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'Username and password required' });
  if (hasUnsafeChapField(username) || hasUnsafeChapField(password)) {
    return res.status(400).json({ error: 'Username/password cannot contain new lines' });
  }

  if (l2tpUsers.find(u => u.username === username)) {
    return res.status(400).json({ error: 'User already exists' });
  }

  // Check max users limit
  if (l2tpSettings.maxUsers > 0 && l2tpUsers.length >= l2tpSettings.maxUsers) {
    return res.status(400).json({ error: `Max users limit (${l2tpSettings.maxUsers}) reached` });
  }

  const user = {
    id: genId(),
    username,
    password,
    enabled: true,
    expiresAt: expiresAt ? new Date(expiresAt).getTime() : null,
    bandwidthLimit: bandwidthLimit ? parseInt(bandwidthLimit) : null,
    usedTraffic: 0,
    speedLimit: speedLimit ? parseInt(speedLimit) : null,
    createdAt: Date.now(),
  };
  l2tpUsers.push(user);
  saveL2tpUsers();
  writeChapSecrets();
  addAudit('l2tp_user_created', username);
  broadcastVpnServerState();
  res.json({ ...user, password: '***' });
});

app.put('/api/l2tp/users/:id', (req, res) => {
  const user = l2tpUsers.find(u => u.id === req.params.id);
  if (!user) return res.status(404).json({ error: 'User not found' });

  if (req.body.password !== undefined) {
    if (!req.body.password) return res.status(400).json({ error: 'Password cannot be empty' });
    if (hasUnsafeChapField(req.body.password)) {
      return res.status(400).json({ error: 'Password cannot contain new lines' });
    }
    user.password = String(req.body.password);
  }
  if (req.body.enabled !== undefined) user.enabled = req.body.enabled;
  if (req.body.expiresAt !== undefined) user.expiresAt = req.body.expiresAt ? new Date(req.body.expiresAt).getTime() : null;
  if (req.body.bandwidthLimit !== undefined) user.bandwidthLimit = req.body.bandwidthLimit ? parseInt(req.body.bandwidthLimit) : null;
  if (req.body.speedLimit !== undefined) user.speedLimit = req.body.speedLimit ? parseInt(req.body.speedLimit) : null;
  if (req.body.resetTraffic) user.usedTraffic = 0;

  saveL2tpUsers();
  writeChapSecrets();
  broadcastVpnServerState();
  res.json({ ...user, password: '***' });
});

app.delete('/api/l2tp/users/:id', (req, res) => {
  const user = l2tpUsers.find(u => u.id === req.params.id);
  if (!user) return res.status(404).json({ error: 'User not found' });

  l2tpUsers = l2tpUsers.filter(u => u.id !== req.params.id);
  saveL2tpUsers();
  writeChapSecrets();
  addAudit('l2tp_user_deleted', user.username);
  broadcastVpnServerState();
  res.json({ ok: true });
});

app.get('/api/l2tp/settings', (req, res) => {
  res.json({
    psk: getL2tpPsk(),
    subnet: L2TP_SUBNET,
    localIp: '10.9.0.1',
    ipRange: '10.9.0.10-10.9.0.250',
    maxUsers: l2tpSettings.maxUsers,
  });
});

app.post('/api/l2tp/settings', (req, res) => {
  if (req.body.maxUsers !== undefined) l2tpSettings.maxUsers = parseInt(req.body.maxUsers) || 0;
  saveL2tpSettings();
  broadcastVpnServerState();
  res.json(l2tpSettings);
});

// ─── Redsocks/Routing API ───────────────────────────────────────────

app.get('/api/routing/status', (req, res) => {
  res.json({
    redsocks: redsocksStatus,
    vpnRouting: vpnRoutingActive,
    socksPort: getActiveSocksPort(),
    socksSources: getRoutingSocksSources(),
    redsocksPort: REDSOCKS_PORT,
    dns2socksPort: DNS2SOCKS_PORT,
  });
});

app.get('/api/routing/logs', (req, res) => {
  res.json(routingLogs);
});

app.post('/api/routing/logs/clear', (req, res) => {
  routingLogs = [];
  res.json({ ok: true });
});

app.post('/api/routing/diagnose', (req, res) => {
  res.json(runRoutingDiagnostics());
});

app.post('/api/routing/start', (req, res) => {
  // Allow user to specify a custom SOCKS port, otherwise auto-detect
  if (req.body.socksPort) {
    routingSocksPort = parseInt(req.body.socksPort);
  }
  const port = getActiveSocksPort();
  if (!port) {
    const xrayRuns = listXrayRuns();
    if (xrayRuns.length > 1) {
      return res.status(400).json({ error: 'Multiple Xray SOCKS tunnels are running. Select an explicit SOCKS source before starting routing.' });
    }
    return res.status(400).json({ error: 'No active SOCKS tunnel. Start a SOCKS tunnel first, or provide a socksPort.' });
  }
  const ok = startRedsocks(port);
  res.json({ ok, socksPort: port });
});

app.post('/api/routing/stop', (req, res) => {
  stopRedsocks();
  routingSocksPort = null;
  res.json({ ok: true });
});

// ─── VPN Install Check API ─────────────────────────────────────────

app.get('/api/vpn-server/install-status', (req, res) => {
  const resultFile = path.join(__dirname, 'vpn-install-result.json');
  try {
    if (fs.existsSync(resultFile)) {
      const data = JSON.parse(fs.readFileSync(resultFile, 'utf8'));
      res.json({ installed: true, ...data });
    } else {
      res.json({ installed: false });
    }
  } catch (e) {
    res.json({ installed: false, error: e.message });
  }
});

app.post('/api/vpn-server/install', (req, res) => {
  const scriptPath = path.join(__dirname, 'install-vpn-servers.sh');
  if (!fs.existsSync(scriptPath)) {
    return res.status(404).json({ error: 'Install script not found' });
  }

  res.json({ ok: true, message: 'Installation started. Check logs.' });

  const proc = spawn('bash', [scriptPath], { stdio: ['ignore', 'pipe', 'pipe'] });
  proc.stdout.on('data', d => {
    const line = d.toString().trim();
    console.log('[VPN-Install]', line);
    broadcast({ type: 'vpn_install_log', line });
  });
  proc.stderr.on('data', d => {
    const line = d.toString().trim();
    console.log('[VPN-Install]', line);
    broadcast({ type: 'vpn_install_log', line });
  });
  proc.on('close', (code) => {
    console.log(`[VPN-Install] Exited with code ${code}`);
    broadcast({ type: 'vpn_install_done', code });
  });
});

// ─── VPN Server State Broadcast ─────────────────────────────────────

function broadcastVpnServerState() {
  broadcast({
    type: 'vpn_server_state',
    openvpn: {
      status: ovpnServerStatus,
      connectedClients: parseOvpnStatusLog(),
      usersCount: ovpnServerUsers.length,
    },
    l2tp: {
      status: l2tpStatus,
      connectedClients: getL2tpConnectedClients(),
      usersCount: l2tpUsers.length,
    },
    routing: {
      redsocks: redsocksStatus,
      vpnRouting: vpnRoutingActive,
      socksPort: getActiveSocksPort(),
      socksSources: getRoutingSocksSources(),
    },
  });
}

// Track traffic & broadcast VPN server state every 5s
setInterval(() => {
  if (ovpnServerStatus === 'running') trackOvpnTraffic();
  if (l2tpStatus === 'running') trackL2tpTraffic();
  if (ovpnServerStatus === 'running' || l2tpStatus === 'running') {
    broadcastVpnServerState();
  }
}, 5000);

// ─── Orphan process detection ────────────────────────────────────────
// Re-adopt processes that were already running before this service started/restarted.
function makeOrphanProxy(pid) {
  const listeners = {};
  const proxy = {
    pid,
    exitCode: null,
    stdout: { on() {} },
    stderr: { on() {} },
    on(event, cb) {
      if (!listeners[event]) listeners[event] = [];
      listeners[event].push(cb);
    },
    kill(sig) { try { process.kill(pid, sig || 'SIGTERM'); } catch {} },
  };
  const iv = setInterval(() => {
    try { process.kill(pid, 0); }
    catch {
      clearInterval(iv);
      (listeners['exit'] || []).forEach(cb => cb(null));
    }
  }, 3000);
  return proxy;
}

function pgrepForFile(filePath) {
  try {
    const out = execSync(`pgrep -f ${JSON.stringify(path.basename(filePath))}`, { timeout: 3000 }).toString().trim();
    const pid = parseInt(out.split('\n')[0]);
    return Number.isInteger(pid) && pid > 0 ? pid : null;
  } catch { return null; }
}

function detectOrphanProcesses() {
  // 1. Local Xray server
  const localXrayConfigPath = path.join(__dirname, '.local-xray-run.json');
  if (!localXrayProc && fs.existsSync(localXrayConfigPath)) {
    const pid = pgrepForFile(localXrayConfigPath);
    if (pid) {
      console.log(`[startup] Adopting orphan local Xray server pid=${pid}`);
      const proc = makeOrphanProxy(pid);
      localXrayProc = proc;
      localXrayLogs.push({ t: Date.now(), m: `[panel] Adopted orphan process pid=${pid}` });
      proc.on('exit', () => {
        stopTrafficPolling();
        stopXrayAccessLogWatcher();
        if (localXrayProc === proc) localXrayProc = null;
        refreshVpnRoutes().catch(() => {});
      });
      startTrafficPolling();
    }
  }

  // 2. Xray client tunnel sessions
  for (const cfg of xrayLocalConfigs) {
    if (!cfg || !cfg.id || xraySessions.has(cfg.id)) continue;
    const runtimeFile = getXrayConfigRuntimePath(cfg.id);
    if (!fs.existsSync(runtimeFile)) continue;
    const pid = pgrepForFile(runtimeFile);
    if (!pid) continue;
    console.log(`[startup] Adopting orphan Xray session configId=${cfg.id} pid=${pid}`);
    const proc = makeOrphanProxy(pid);
    const bindings = getEffectiveXrayConfigBindings(cfg);
    const session = {
      configId: cfg.id,
      proc,
      runtimeFile,
      localBindings: bindings,
      startedAt: Date.now(),
      recentStderr: '',
      finalized: false,
      stopping: false,
    };
    xraySessions.set(cfg.id, session);
    const finalize = () => {
      if (session.finalized) return;
      session.finalized = true;
      if (xraySessions.get(cfg.id) === session) xraySessions.delete(cfg.id);
      try { fs.unlinkSync(runtimeFile); } catch {}
      clearXrayConfigRouteSelection(cfg.id, `Xray session ended (${cfg.remark || cfg.id})`).catch(() => {});
      refreshVpnRoutes().catch(() => {});
      if (localXrayProc) restartLocalXray();
    };
    proc.on('exit', finalize);
  }

  // 3. OpenVPN client
  if (!ovpnProcess) {
    try {
      const out = execSync('pgrep -x openvpn', { timeout: 3000 }).toString().trim();
      const pids = out.split('\n').map(p => parseInt(p)).filter(p => Number.isInteger(p) && p > 0);
      if (pids.length > 0) {
        const pid = pids[0];
        console.log(`[startup] Adopting orphan OpenVPN client pid=${pid}`);
        ovpnProcess = makeOrphanProxy(pid);
        ovpnStatus = 'connected';
        ovpnConnectedSince = Date.now();
        ovpnStatusMessage = 'Connected (restored after restart)';
        addOvpnLog(`[panel] Adopted orphan OpenVPN process pid=${pid}`);
        ovpnProcess.on('exit', () => {
          ovpnStatus = 'disconnected';
          ovpnStatusMessage = 'Disconnected';
          ovpnConnectedSince = null;
          ovpnProcess = null;
          refreshVpnRoutes().catch(() => {});
        });
      }
    } catch {}
  }
}

detectOrphanProcesses();

// ─── Start server ───────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
refreshConfiguredDnsEnforcement('startup');
server.listen(PORT, () => {
  console.log(`\n  🚀 SSH Tunnel Manager running at http://localhost:${PORT}\n`);
});
