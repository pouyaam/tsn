#!/usr/bin/env node
// ─── OpenConnect Agent ──────────────────────────────────────────────
// Runs on the machine that needs the VPN connection.
// - Manages openconnect process (start/stop/auto-reconnect)
// - Broadcasts presence on LAN via UDP
// - Exposes HTTP API for remote control & status
//
// Usage:  sudo node oc-agent.js
// ─────────────────────────────────────────────────────────────────────

const http = require('http');
const dgram = require('dgram');
const { spawn } = require('child_process');
const os = require('os');

// ─── Config ─────────────────────────────────────────
const OC_CONFIG = {
  url: 'ra.albb.ir',
  authgroup: 'HQ',
  username: 'p.amirahmadi',
  password: 'P@mirahmadi123456',
};

const AGENT_PORT = 7800;           // HTTP API port
const BROADCAST_PORT = 7801;       // UDP broadcast port
const BROADCAST_INTERVAL = 3000;   // broadcast every 3s

// ─── State ──────────────────────────────────────────
let ocProcess = null;
let status = 'disconnected'; // connecting | connected | disconnected | reconnecting | error
let statusMessage = 'Idle';
let shouldReconnect = false;
let reconnectTimer = null;
let reconnectAttempt = 0;
let connectedSince = null;
let logs = [];
const MAX_LOGS = 300;

function addLog(line) {
  logs.push({ time: Date.now(), text: line });
  if (logs.length > MAX_LOGS) logs.shift();
}

// ─── OpenConnect Process Management ─────────────────
function stopVpn(reason = '') {
  shouldReconnect = false;
  clearTimeout(reconnectTimer);
  reconnectTimer = null;

  if (ocProcess) {
    try { ocProcess.kill('SIGTERM'); } catch (e) { /* ignore */ }
    ocProcess = null;
  }

  status = 'disconnected';
  statusMessage = reason || 'Stopped by user';
  connectedSince = null;
  reconnectAttempt = 0;
  addLog(`[agent] Stopped: ${statusMessage}`);
  console.log(`[OpenConnect] Stopped: ${statusMessage}`);
}

function startVpn() {
  if (ocProcess) stopVpn('Restarting');
  shouldReconnect = true;
  reconnectAttempt = 0;
  logs = [];
  connectVpn();
}

function connectVpn() {
  status = reconnectAttempt > 0 ? 'reconnecting' : 'connecting';
  statusMessage = reconnectAttempt > 0
    ? `Reconnect attempt ${reconnectAttempt}...`
    : 'Connecting...';

  const args = [
    '--protocol=anyconnect',
    `--user=${OC_CONFIG.username}`,
    `--authgroup=${OC_CONFIG.authgroup}`,
    '--passwd-on-stdin',
    OC_CONFIG.url,
  ];

  console.log(`[OpenConnect] Spawning: openconnect ${args.join(' ')}`);
  addLog(`[agent] Connecting to ${OC_CONFIG.url} (group: ${OC_CONFIG.authgroup})...`);

  const proc = spawn('openconnect', args, {
    stdio: ['pipe', 'pipe', 'pipe'],
  });

  ocProcess = proc;

  // Send password via stdin
  proc.stdin.write(OC_CONFIG.password + '\n');
  proc.stdin.end();

  const handleOutput = (source) => (data) => {
    const lines = data.toString().split('\n').filter(l => l.trim());
    for (const line of lines) {
      console.log(`[OC ${source}] ${line}`);
      addLog(line);

      if (line.includes('Connected as') || line.includes('ESP session established') || line.includes('DTLS connected')) {
        status = 'connected';
        statusMessage = line;
        if (!connectedSince) connectedSince = Date.now();
        reconnectAttempt = 0;
      }
    }
  };

  proc.stdout.on('data', handleOutput('stdout'));
  proc.stderr.on('data', handleOutput('stderr'));

  proc.on('error', (err) => {
    console.error(`[OpenConnect] Process error:`, err.message);
    addLog(`[agent] Error: ${err.message}`);
    status = 'error';
    statusMessage = err.message;
  });

  proc.on('close', (code) => {
    console.log(`[OpenConnect] Exited with code ${code}`);
    addLog(`[agent] Process exited with code ${code}`);
    ocProcess = null;
    connectedSince = null;

    if (shouldReconnect) {
      reconnectAttempt++;
      const delay = Math.min(1000 * Math.pow(2, reconnectAttempt - 1), 30000);
      status = 'reconnecting';
      statusMessage = `Reconnecting in ${(delay / 1000).toFixed(0)}s (attempt ${reconnectAttempt})...`;
      addLog(`[agent] ${statusMessage}`);
      console.log(`[OpenConnect] ${statusMessage}`);
      reconnectTimer = setTimeout(() => {
        if (shouldReconnect) connectVpn();
      }, delay);
    } else {
      status = 'disconnected';
      statusMessage = `Exited with code ${code}`;
    }
  });
}

// ─── Get local IP addresses ─────────────────────────
function getLocalIPs() {
  const ips = [];
  const ifaces = os.networkInterfaces();
  for (const name of Object.keys(ifaces)) {
    for (const iface of ifaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        ips.push(iface.address);
      }
    }
  }
  return ips;
}

// ─── UDP Broadcast ──────────────────────────────────
const udp = dgram.createSocket({ type: 'udp4', reuseAddr: true });

function broadcastPresence() {
  const msg = JSON.stringify({
    type: 'oc-agent',
    hostname: os.hostname(),
    ips: getLocalIPs(),
    port: AGENT_PORT,
    status,
    statusMessage,
    connectedSince,
    uptime: connectedSince ? Date.now() - connectedSince : 0,
  });
  const buf = Buffer.from(msg);
  udp.send(buf, 0, buf.length, BROADCAST_PORT, '255.255.255.255', (err) => {
    if (err) console.error('[UDP] Broadcast error:', err.message);
  });
}

udp.bind(() => {
  udp.setBroadcast(true);
  setInterval(broadcastPresence, BROADCAST_INTERVAL);
  console.log(`[UDP] Broadcasting on port ${BROADCAST_PORT} every ${BROADCAST_INTERVAL / 1000}s`);
});

// ─── HTTP API ───────────────────────────────────────
const httpServer = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Content-Type', 'application/json');

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  if (req.method === 'GET' && req.url === '/status') {
    res.writeHead(200);
    res.end(JSON.stringify({
      hostname: os.hostname(),
      status,
      statusMessage,
      connectedSince,
      uptime: connectedSince ? Date.now() - connectedSince : 0,
      vpnConfig: {
        url: OC_CONFIG.url,
        authgroup: OC_CONFIG.authgroup,
        username: OC_CONFIG.username,
      },
    }));
    return;
  }

  if (req.method === 'GET' && req.url === '/logs') {
    res.writeHead(200);
    res.end(JSON.stringify(logs));
    return;
  }

  if (req.method === 'POST' && req.url === '/start') {
    startVpn();
    res.writeHead(200);
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  if (req.method === 'POST' && req.url === '/stop') {
    stopVpn();
    res.writeHead(200);
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  if (req.method === 'POST' && req.url === '/retry') {
    if (status === 'connected' || status === 'connecting') {
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, message: 'Already running' }));
      return;
    }
    startVpn();
    res.writeHead(200);
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  res.writeHead(404);
  res.end(JSON.stringify({ error: 'Not found' }));
});

httpServer.listen(AGENT_PORT, '0.0.0.0', () => {
  console.log(`\n  [oc-agent] OpenConnect Agent running`);
  console.log(`  HTTP API:   http://0.0.0.0:${AGENT_PORT}`);
  console.log(`  UDP Broadcast: port ${BROADCAST_PORT}`);
  console.log(`  VPN target: ${OC_CONFIG.url} (group: ${OC_CONFIG.authgroup})\n`);
});
