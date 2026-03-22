// ─── PWA Service Worker ─────────────────────────────
if ('serviceWorker' in navigator) {
  navigator.serviceWorker.register('/sw.js').catch(() => {});
}

// ─── State ───────────────────────────────────────────
let tunnels = [];
let state = {
  activeTunnelId: null,
  status: 'disconnected',
  statusMessage: '',
  panelUrl: null,
  stats: { bytesIn: 0, bytesOut: 0, activeConnections: 0, uptime: 0, reconnectCount: 0 },
};
let ws = null;
let currentPage = 'tunnels';

// ─── DOM refs ────────────────────────────────────────
const $ = id => document.getElementById(id);

const $tunnels = $('tunnels-container');
const $emptyState = $('empty-state');
const $statsBar = $('stats-bar');
const $globalPill = $('global-pill');
const $globalPillText = $('global-pill-text');
const $uptime = $('stat-uptime');
const $bytesIn = $('stat-bytes-in');
const $bytesOut = $('stat-bytes-out');
const $connections = $('stat-connections');
const $reconnects = $('stat-reconnects');
const $latency = $('stat-latency');
const $bwIn = $('stat-bw-in');
const $bwOut = $('stat-bw-out');
const $latencyGraph = $('latency-graph');
const $panelUrl = $('panel-url');
const $panelUrlText = $('panel-url-text');
const $noRemote = $('no-remote');
const $navDotTunnel = $('nav-dot-tunnel');
const $navDotVpn = $('nav-dot-vpn');
// tab dots removed (hamburger menu replaces tab bar)

// Modals
const $tunnelModal = $('tunnel-modal');
const $tunnelForm = $('tunnel-form');
const $modalTitle = $('modal-title');
const $editId = $('edit-tunnel-id');
const $pwModal = $('password-modal');
const $pwForm = $('password-form');
const $pwTunnelId = $('pw-tunnel-id');
const $pwInfo = $('pw-tunnel-info');
const $pwInput = $('input-password');

// ─── Navigation ──────────────────────────────────────
function toggleDrawer() {
  const sidebar = $('sidebar');
  const overlay = $('drawer-overlay');
  const isOpen = sidebar.classList.contains('open');
  if (isOpen) {
    closeDrawer();
  } else {
    sidebar.classList.add('open');
    overlay.classList.remove('hidden');
  }
}

function closeDrawer() {
  $('sidebar').classList.remove('open');
  $('drawer-overlay').classList.add('hidden');
}

function switchPage(page) {
  currentPage = page;
  location.hash = '#' + page;
  closeDrawer();
  document.querySelectorAll('.nav-item[data-nav]').forEach(el => {
    el.classList.toggle('active', el.dataset.nav === page);
  });
  const pages = ['dashboard', 'tunnels', 'vpn', 'connections', 'v2ray', 'xray', 'ovpn-server', 'l2tp', 'statistics', 'settings'];
  pages.forEach(p => {
    const el = $('page-' + p);
    if (el) el.classList.toggle('hidden', p !== page);
  });
  if (page === 'dashboard') updateDashboard();
  if (page === 'tunnels') refreshSshTunnelLogs();
  if (page === 'vpn') loadVpnPage();
  if (page === 'connections') {
    if (!auditLoaded) loadAuditLog();
    refreshConnectionsTab();
    clearInterval(connRefreshTimer);
    connRefreshTimer = setInterval(refreshConnectionsTab, 10000);
  } else {
    clearInterval(connRefreshTimer);
    connRefreshTimer = null;
  }
  if (page === 'statistics') startStatsPolling();
  else stopStatsPolling();
  if (page === 'v2ray') { loadLocalAccountsPage(); if (!xuiLoaded) loadXuiData(); }
  else stopAccPolling();
  if (page === 'xray') loadXrayLocalPage();
  if (page === 'settings') initSettingsPage();
  if (page === 'ovpn-server') {
    loadOvpnServerPage();
    startOvpnServerDebugPolling();
  } else {
    stopOvpnServerDebugPolling();
  }
  if (page === 'l2tp') loadL2tpPage();
}

// ─── Helpers ─────────────────────────────────────────
function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return (bytes / Math.pow(1024, i)).toFixed(i > 0 ? 1 : 0) + ' ' + units[i];
}

function formatUptime(ms) {
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  return [h, m, sec].map(v => String(v).padStart(2, '0')).join(':');
}

function statusLabel(status) {
  return { disconnected: 'Idle', connecting: 'Connecting', connected: 'Connected', reconnecting: 'Reconnecting', error: 'Error' }[status] || status;
}

function statusColor(s) {
  return { connected: 'green', connecting: 'purple', reconnecting: 'purple', error: 'red', disconnected: '' }[s] || '';
}

function escapeHtml(str) {
  const d = document.createElement('div');
  d.textContent = str;
  return d.innerHTML;
}

// ─── API ─────────────────────────────────────────────
async function api(path, opts = {}) {
  const res = await fetch(`/api${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  return res.json();
}

async function loadTunnels() {
  tunnels = await api('/tunnels');
  renderTunnels();
}

let cachedInterfaces = null;
async function loadTunnelInterfaces() {
  try {
    const data = await api('/tunnels/interfaces');
    cachedInterfaces = data;
    return data;
  } catch { return { interfaces: [], default: 'direct' }; }
}

function populateRouteInterfaceSelect(selectedValue) {
  const sel = $('input-route-interface');
  const data = cachedInterfaces || { interfaces: [], default: 'direct' };
  const ifaces = data.interfaces;
  sel.innerHTML = '';

  // Add 'direct' first
  sel.insertAdjacentHTML('beforeend', '<option value="direct">Direct (no route change)</option>');

  // Add detected interfaces
  for (const iface of ifaces) {
    const isTun = /^(tun|utun|tap)/.test(iface);
    const label = isTun ? `${iface} (VPN tunnel)` : iface;
    sel.insertAdjacentHTML('beforeend', `<option value="${iface}">${label}</option>`);
  }

  // Set selected value, fallback to detected default
  const val = selectedValue || data.default || 'direct';
  if ([...sel.options].some(o => o.value === val)) {
    sel.value = val;
  } else {
    // Value not in list (e.g. saved 'tun0' but tun0 not up right now) — add it greyed
    sel.insertAdjacentHTML('beforeend', `<option value="${val}">${val} (not detected)</option>`);
    sel.value = val;
  }
}

// ─── Tunnel Type Toggle ─────────────────────────────
function toggleTunnelTypeFields() {
  const type = $('input-tunnel-type').value;
  const reverseFields = $('reverse-fields');
  const socksFields = $('socks-fields');
  if (type === 'socks') {
    reverseFields.classList.add('hidden');
    socksFields.classList.remove('hidden');
  } else {
    reverseFields.classList.remove('hidden');
    socksFields.classList.add('hidden');
  }
}

// ─── Render Tunnels ──────────────────────────────────
function buildCardHTML(t, isActive) {
  const cardClass = isActive ? `card-${statusColor(state.status)}` : '';
  const isSocks = t.type === 'socks';
  const typeBadge = isSocks ? '<span style="background:var(--purple);color:#fff;padding:1px 6px;border-radius:4px;font-size:11px;margin-left:6px">SOCKS</span>' : '';
  const routeBadge = t.routeInterface === 'direct'
    ? '<span style="background:var(--t3);color:#fff;padding:1px 6px;border-radius:4px;font-size:10px;margin-left:4px">direct</span>'
    : `<span style="background:var(--blue);color:#fff;padding:1px 6px;border-radius:4px;font-size:10px;margin-left:4px">${escapeHtml(t.routeInterface || 'tun0')}</span>`;
  const metaLine = isSocks
    ? `<span>${escapeHtml(t.username)}</span>@${escapeHtml(t.host)}:${t.sshPort}<br>SOCKS5 on <span>:${t.socksPort}</span>`
    : `<span>${escapeHtml(t.username)}</span>@${escapeHtml(t.host)}:${t.sshPort}<br>Remote <span>:${t.remotePort}</span> &rarr; Local <span>${escapeHtml(t.localHost || 'localhost')}:${t.localPort}</span>`;
  return `
    <div class="card ${cardClass}" data-id="${t.id}">
      <div class="card-row">
        <div>
          <div class="card-title">${escapeHtml(t.name)}${typeBadge}${routeBadge}</div>
          <div class="card-meta">
            ${metaLine}
          </div>
        </div>
        <div class="card-actions">
          ${isActive
            ? `<button class="btn btn-red btn-touch" onclick="stopTunnel('${t.id}')">Stop</button>`
            : `<button class="btn btn-green btn-touch" onclick="handleStart('${t.id}')">Start</button>`
          }
          ${!isActive ? `
            <button class="btn-icon" onclick="editTunnel('${t.id}')" title="Edit">
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
            </button>
            <button class="btn-icon danger" onclick="deleteTunnel('${t.id}')" title="Delete">
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/></svg>
            </button>
          ` : ''}
        </div>
      </div>
      <div class="card-foot ${isActive ? statusColor(state.status) : ''}">
        ${isActive ? escapeHtml(state.statusMessage || statusLabel(state.status)) : (t.hasPassword ? 'Password saved' : '')}
      </div>
    </div>
  `;
}

function removeSkeleton(id) {
  const el = $(id);
  if (el) el.remove();
}

function renderTunnels() {
  removeSkeleton('tunnels-skeleton');
  $tunnels.querySelectorAll('.card').forEach(el => el.remove());
  if (tunnels.length === 0) { $emptyState.classList.remove('hidden'); return; }
  $emptyState.classList.add('hidden');
  tunnels.forEach(t => {
    const isActive = state.activeTunnelId === t.id;
    $tunnels.insertAdjacentHTML('beforeend', buildCardHTML(t, isActive));
  });
}

function updateTunnelCards() {
  tunnels.forEach(t => {
    const card = $tunnels.querySelector(`.card[data-id="${t.id}"]`);
    if (!card) return;
    const isActive = state.activeTunnelId === t.id;
    const wasActive = card.querySelector('.btn-red') !== null;
    if (isActive !== wasActive) {
      card.outerHTML = buildCardHTML(t, isActive);
      return;
    }
    card.className = `card ${isActive ? 'card-' + statusColor(state.status) : ''}`;
    const footer = card.querySelector('.card-foot');
    if (footer) {
      footer.className = `card-foot ${isActive ? statusColor(state.status) : ''}`;
      footer.textContent = isActive ? (state.statusMessage || statusLabel(state.status)) : (t.hasPassword ? 'Password saved' : '');
    }
  });
}

// ─── Global Status ───────────────────────────────────
function updateGlobalStatus() {
  const c = statusColor(state.status);
  $globalPill.className = `pill ${c}`;
  $globalPillText.textContent = statusLabel(state.status);

  // Nav dots (desktop + mobile)
  if ($navDotTunnel) $navDotTunnel.className = `nav-dot ${c}`;

  // Panel URL
  if (state.panelUrl) {
    $panelUrl.href = state.panelUrl;
    $panelUrlText.textContent = state.panelUrl.replace('http://', '');
    $panelUrl.classList.remove('hidden');
    $noRemote.classList.add('hidden');
  } else {
    $panelUrl.classList.add('hidden');
    $noRemote.classList.remove('hidden');
  }

  // Stats
  if (state.activeTunnelId) {
    $statsBar.classList.remove('hidden');
    $uptime.textContent = formatUptime(state.stats.uptime);
    $bytesIn.textContent = formatBytes(state.stats.bytesIn);
    $bytesOut.textContent = formatBytes(state.stats.bytesOut);
    $connections.textContent = state.stats.activeConnections;
    $reconnects.textContent = state.stats.reconnectCount;
    // Bandwidth
    $bwIn.textContent = formatBytes(state.stats.bandwidthIn || 0);
    $bwOut.textContent = formatBytes(state.stats.bandwidthOut || 0);
    // Health / latency
    if (state.health && state.health.latency !== null) {
      $latency.textContent = state.health.latency + 'ms';
      $latency.style.color = state.health.latency < 100 ? 'var(--green)' : state.health.latency < 300 ? 'var(--amber)' : 'var(--red)';
    } else {
      $latency.textContent = '--';
      $latency.style.color = '';
    }
    // Sparkline
    if (state.health && state.health.history && state.health.history.length >= 2) {
      drawSparkline(state.health.history);
    } else {
      $latencyGraph.classList.add('hidden');
    }
  } else {
    $statsBar.classList.add('hidden');
  }
}

async function refreshSshTunnelLogs() {
  try {
    const logs = await api('/tunnel/logs');
    renderTimedLogs('ssh-tunnel-logs-output', logs, 'No SSH tunnel logs yet');
  } catch (e) {
    renderTextLogs('ssh-tunnel-logs-output', [`Failed to load SSH tunnel logs: ${e.message}`], 'No SSH tunnel logs yet');
  }
}

function copySshTunnelLogs() {
  copyLogOutput('ssh-tunnel-logs-output', 'No SSH tunnel logs yet');
}

async function clearSshTunnelLogs() {
  await clearLogOutput('/tunnel/logs/clear', 'ssh-tunnel-logs-output', 'No SSH tunnel logs yet');
}

// ─── Tunnel Actions ──────────────────────────────────
async function handleStart(id) {
  const tunnel = tunnels.find(t => t.id === id);
  if (!tunnel) return;
  if (tunnel.hasPassword) {
    await api(`/tunnels/${id}/start`, { method: 'POST', body: {} });
    return;
  }
  $pwTunnelId.value = id;
  $pwInfo.textContent = `${tunnel.username}@${tunnel.host}:${tunnel.sshPort}`;
  $pwInput.value = '';
  $pwModal.classList.remove('hidden');
  setTimeout(() => $pwInput.focus(), 100);
}

async function stopTunnel(id) { await api(`/tunnels/${id}/stop`, { method: 'POST' }); }

async function deleteTunnel(id) {
  if (!confirm('Delete this tunnel?')) return;
  await api(`/tunnels/${id}`, { method: 'DELETE' });
  await loadTunnels();
}

async function editTunnel(id) {
  const t = tunnels.find(x => x.id === id);
  if (!t) return;
  if (!cachedInterfaces) await loadTunnelInterfaces();
  $modalTitle.textContent = 'Edit Tunnel';
  $editId.value = t.id;
  $('input-tunnel-type').value = t.type || 'reverse';
  $('input-name').value = t.name;
  $('input-host').value = t.host;
  $('input-ssh-port').value = t.sshPort;
  $('input-username').value = t.username;
  $('input-remote-port').value = t.remotePort || '';
  $('input-local-port').value = t.localPort || '';
  $('input-local-host').value = t.localHost || 'localhost';
  $('input-socks-port').value = t.socksPort || '';
  populateRouteInterfaceSelect(t.routeInterface || 'tun0');
  toggleTunnelTypeFields();
  $tunnelModal.classList.remove('hidden');
}

// ─── Modal: Add/Edit ─────────────────────────────────
$('btn-add-tunnel').addEventListener('click', async () => {
  if (!cachedInterfaces) await loadTunnelInterfaces();
  $modalTitle.textContent = 'Add Tunnel';
  $editId.value = '';
  $tunnelForm.reset();
  $('input-tunnel-type').value = 'reverse';
  $('input-ssh-port').value = '22';
  $('input-local-host').value = 'localhost';
  populateRouteInterfaceSelect(cachedInterfaces?.default || 'tun0');
  toggleTunnelTypeFields();
  $tunnelModal.classList.remove('hidden');
});

function closeModal(el) { el.classList.add('hidden'); }

$('btn-modal-close').addEventListener('click', () => closeModal($tunnelModal));
$('btn-cancel').addEventListener('click', () => closeModal($tunnelModal));
$tunnelModal.addEventListener('click', e => { if (e.target === $tunnelModal) closeModal($tunnelModal); });

$tunnelForm.addEventListener('submit', async e => {
  e.preventDefault();
  const type = $('input-tunnel-type').value;
  const body = {
    type,
    name: $('input-name').value,
    host: $('input-host').value,
    sshPort: $('input-ssh-port').value,
    username: $('input-username').value,
    routeInterface: $('input-route-interface').value,
  };
  if (type === 'socks') {
    body.socksPort = $('input-socks-port').value;
  } else {
    body.remotePort = $('input-remote-port').value;
    body.localPort = $('input-local-port').value;
    body.localHost = $('input-local-host').value;
  }
  const editId = $editId.value;
  if (editId) await api(`/tunnels/${editId}`, { method: 'PUT', body });
  else await api('/tunnels', { method: 'POST', body });
  closeModal($tunnelModal);
  await loadTunnels();
});

// ─── Modal: Password ─────────────────────────────────
$('btn-pw-close').addEventListener('click', () => closeModal($pwModal));
$('btn-pw-cancel').addEventListener('click', () => closeModal($pwModal));
$pwModal.addEventListener('click', e => { if (e.target === $pwModal) closeModal($pwModal); });

$pwForm.addEventListener('submit', async e => {
  e.preventDefault();
  const id = $pwTunnelId.value;
  const password = $pwInput.value;
  closeModal($pwModal);
  await api(`/tunnels/${id}/start`, { method: 'POST', body: { password } });
});

// ─── WebSocket ───────────────────────────────────────
function connectWs() {
  const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(`${protocol}//${location.host}`);
  updateWsIndicator('connecting');

  ws.onopen = () => updateWsIndicator('connected');

  ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    if (data.type === 'state') {
      const prevActive = state.activeTunnelId;
      state = {
        activeTunnelId: data.activeTunnelId,
        status: data.status,
        statusMessage: data.statusMessage,
        panelUrl: data.panelUrl,
        stats: data.stats,
        health: data.health || null,
      };
      updateGlobalStatus();
      updateConnections(data.connections, data.remoteClients);
      if (prevActive !== data.activeTunnelId) loadTunnels();
      else updateTunnelCards();
      // Update dashboard on state changes
      if (currentPage === 'dashboard') updateDashboard();
    } else if (data.type === 'oc_state') {
      ocState = {
        mode: data.mode || 'network',
        agent: data.agent,
        status: data.status,
        statusMessage: data.statusMessage,
        uptime: data.uptime,
        connectedSince: data.connectedSince,
        vpnConfig: data.vpnConfig,
      };
      applyVpnUpstreamState(data.upstreamRouting);
      updateOcUI();
      if (currentPage === 'dashboard') updateDashboard();
    } else if (data.type === 'ovpn_state') {
      ovpnState = {
        status: data.status || 'disconnected',
        statusMessage: data.statusMessage || 'Idle',
        uptime: data.uptime || 0,
        connectedSince: data.connectedSince || null,
        tunDevice: data.tunDevice || null,
        routedHosts: data.routedHosts || [],
        profiles: data.profiles || [],
      };
      applyVpnUpstreamState(data.upstreamRouting);
      updateOvpnUI();
      if (currentPage === 'vpn' && ovpnLogsVisible) loadOvpnLogs();
    } else if (data.type === 'oc_log') {
      appendOcLog(data.line);
    } else if (data.type === 'notification') {
      showPushNotification(data.event, data.message);
    } else if (data.type === 'audit') {
      appendAuditEntry(data.entry);
    } else if (data.type === 'vpn_server_state') {
      if (currentPage === 'ovpn-server') {
        updateOvpnServerUI({
          status: data.openvpn.status,
          connectedClients: data.openvpn.connectedClients,
          redsocks: data.routing.redsocks,
          socksPort: data.routing.socksPort,
        });
      }
      if (currentPage === 'l2tp') {
        updateL2tpUI({ status: data.l2tp.status, connectedClients: data.l2tp.connectedClients });
      }
    } else if (data.type === 'vpn_install_log') {
      const logEl = $('vpn-install-log');
      if (logEl) { logEl.classList.remove('hidden'); logEl.textContent += data.line + '\n'; logEl.scrollTop = logEl.scrollHeight; }
    } else if (data.type === 'vpn_install_done') {
      showToast(data.code === 0 ? 'VPN servers installed!' : 'Install failed (code ' + data.code + ')', data.code === 0 ? 'success' : 'error');
      if (data.code === 0) { loadOvpnServerPage(); loadL2tpPage(); }
    } else if (data.type === 'routing_log') {
      appendTimedLogEntry('routing-logs-output', data.entry, 'No routing logs yet');
    } else if (data.type === 'ssh_tunnel_log') {
      appendTimedLogEntry('ssh-tunnel-logs-output', data.entry, 'No SSH tunnel logs yet');
    } else if (data.type === 'ssh_tunnel_logs') {
      renderTimedLogs('ssh-tunnel-logs-output', data.entries, 'No SSH tunnel logs yet');
    } else if (data.type === 'ovpn_server_log') {
      if (currentPage === 'ovpn-server') refreshOvpnServerLogs();
    }
  };

  ws.onclose = () => { updateWsIndicator(''); setTimeout(connectWs, 2000); };
  ws.onerror = () => ws.close();
}

// ─── OpenConnect VPN ─────────────────────────────────
let ocState = {
  mode: 'network', agent: null, status: 'searching', statusMessage: 'Searching for agent on LAN...',
  uptime: 0, connectedSince: null, vpnConfig: null,
};

const $ocPill = $('oc-pill');
const $ocPillText = $('oc-pill-text');
const $ocStatusMsg = $('oc-status-msg');
const $ocUptime = $('oc-uptime');
const $ocModeInfo = $('oc-mode-info');
const $ocAgentLabel = $('oc-agent-label');
const $ocAgentInfo = $('oc-agent-info');
const $ocVpnServer = $('oc-vpn-server');
const $ocVpnGroup = $('oc-vpn-group');
const $ocVpnUser = $('oc-vpn-user');
const $ocBtnStart = $('oc-btn-start');
const $ocBtnStop = $('oc-btn-stop');
const $ocBtnRetry = $('oc-btn-retry');
const $ocLogsContainer = $('oc-logs-container');
const $ocLogs = $('oc-logs');
const $ocLogsToggleText = $('oc-logs-toggle-text');
const $ocCard = $('oc-card');

function ocColor(s) {
  return { searching: 'amber', connected: 'green', connecting: 'purple', reconnecting: 'purple', error: 'red', disconnected: '' }[s] || '';
}

function ocLabel(s) {
  return { searching: 'Searching', disconnected: 'Disconnected', connecting: 'Connecting', connected: 'Connected', reconnecting: 'Reconnecting', error: 'Error' }[s] || s;
}

function updateOcUI() {
  removeSkeleton('vpn-skeleton');
  $ocCard.classList.remove('hidden');
  const s = ocState.status;
  const c = ocColor(s);
  const isLocal = ocState.mode === 'local';

  $ocPill.className = `pill ${c}`;
  $ocPillText.textContent = ocLabel(s);
  $ocStatusMsg.textContent = ocState.statusMessage || '';
  $ocStatusMsg.className = `card-foot ${c}`;
  $ocUptime.textContent = ocState.connectedSince ? formatUptime(ocState.uptime) : '--:--:--';
  $ocCard.className = `card ${c ? 'card-' + c : ''}`;

  // Nav dots (desktop + mobile)
  if ($navDotVpn) $navDotVpn.className = `nav-dot ${c}`;

  // Mode info
  $ocModeInfo.textContent = isLocal ? 'Local' : 'Network (Remote Agent)';

  if (isLocal) {
    $ocAgentLabel.textContent = 'Host';
    $ocAgentInfo.textContent = 'localhost';
  } else {
    $ocAgentLabel.textContent = 'Agent';
    $ocAgentInfo.textContent = ocState.agent
      ? `${ocState.agent.hostname} (${ocState.agent.ip}:${ocState.agent.port})`
      : 'Scanning LAN...';
  }

  if (ocState.vpnConfig) {
    $ocVpnServer.textContent = ocState.vpnConfig.url || '--';
    $ocVpnGroup.textContent = ocState.vpnConfig.authgroup || '--';
    $ocVpnUser.textContent = ocState.vpnConfig.username || '--';
  }

  const isRunning = s === 'connected' || s === 'connecting' || s === 'reconnecting';
  const isStopped = s === 'disconnected' || s === 'error';
  const canControl = isLocal || !!ocState.agent;

  $ocBtnStart.classList.toggle('hidden', !canControl || !isStopped);
  $ocBtnRetry.classList.toggle('hidden', !canControl || !isStopped);
  $ocBtnStop.classList.toggle('hidden', !canControl || !isRunning);
}

async function ocSendCommand(cmd) {
  try { await api(`/openconnect/${cmd}`, { method: 'POST' }); }
  catch (e) { console.error(`[OC] ${cmd} failed:`, e); }
}

let ocLogsVisible = false;
async function toggleOcLogs() {
  ocLogsVisible = !ocLogsVisible;
  $ocLogsContainer.classList.toggle('hidden', !ocLogsVisible);
  $ocLogsToggleText.textContent = ocLogsVisible ? 'Hide Logs' : 'Show Logs';
  if (ocLogsVisible) {
    try {
      const logs = await api('/openconnect/logs');
      if (Array.isArray(logs)) {
        $ocLogs.innerHTML = '';
        logs.forEach(l => appendOcLog(l.text || l));
      }
    } catch (e) { /* ignore */ }
    $ocLogs.scrollTop = $ocLogs.scrollHeight;
  }
}

function appendOcLog(line) {
  const el = document.createElement('div');
  el.className = 'log-line';
  el.textContent = line;
  $ocLogs.appendChild(el);
  while ($ocLogs.children.length > 200) $ocLogs.removeChild($ocLogs.firstChild);
  if (ocLogsVisible) $ocLogs.scrollTop = $ocLogs.scrollHeight;
}

// ─── OpenConnect Settings ────────────────────────────
async function loadOcSettings() {
  try {
    const data = await api('/openconnect/settings');
    const $mode = $('oc-mode-select');
    const $localCfg = $('oc-local-config');
    $mode.value = data.mode || 'network';
    $localCfg.classList.toggle('hidden', data.mode !== 'local');
    if (data.config) {
      $('oc-cfg-url').value = data.config.url || '';
      $('oc-cfg-authgroup').value = data.config.authgroup || '';
      $('oc-cfg-username').value = data.config.username || '';
      $('oc-cfg-password').value = data.config.hasPassword ? '••••••••' : '';
    }
    $('oc-settings-status').textContent = data.mode === 'local' ? 'Running locally' : 'Scanning network for agent';
  } catch (e) {
    $('oc-settings-status').textContent = 'Failed to load settings';
  }
}

function onOcModeChange() {
  const mode = $('oc-mode-select').value;
  $('oc-local-config').classList.toggle('hidden', mode !== 'local');
}

async function saveOcSettings() {
  const mode = $('oc-mode-select').value;
  const body = { mode };
  if (mode === 'local') {
    body.config = {
      url: $('oc-cfg-url').value.trim(),
      authgroup: $('oc-cfg-authgroup').value.trim(),
      username: $('oc-cfg-username').value.trim(),
      password: $('oc-cfg-password').value,
    };
  }
  try {
    await api('/openconnect/settings', { method: 'POST', body });
    $('oc-settings-status').textContent = mode === 'local' ? 'Saved — running locally' : 'Saved — scanning network';
    $('oc-settings-status').className = 'card-foot green';
    setTimeout(() => { $('oc-settings-status').className = 'card-foot'; }, 2000);
  } catch (e) {
    $('oc-settings-status').textContent = 'Failed to save';
    $('oc-settings-status').className = 'card-foot red';
  }
}

function normalizeDnsServerRows(servers = []) {
  const rows = Array.isArray(servers)
    ? servers.map(value => String(value || '').trim())
    : [];
  while (rows.length < 2) rows.push('');
  return rows;
}

function collectDnsServerInputs() {
  return [...document.querySelectorAll('#dns-server-fields .dns-server-input')]
    .map(input => input.value.trim());
}

function renderDnsServerFields(servers = null) {
  const container = $('dns-server-fields');
  if (!container) return;
  const rows = normalizeDnsServerRows(Array.isArray(servers) ? servers : collectDnsServerInputs());
  container.innerHTML = '';
  rows.forEach((value, index) => {
    container.insertAdjacentHTML('beforeend', `
      <div class="field-row" style="gap:8px;align-items:center">
        <input type="text" class="input dns-server-input" value="${escapeHtml(value)}" placeholder="1.1.1.1" style="flex:1" />
        <button class="btn btn-ghost btn-touch" type="button" onclick="removeDnsServerField(${index})" title="Remove server" style="padding:0 12px">&times;</button>
      </div>
    `);
  });
}

function addDnsServerField(value = '') {
  const rows = collectDnsServerInputs();
  rows.push(String(value || '').trim());
  renderDnsServerFields(rows);
}

function removeDnsServerField(index) {
  const rows = collectDnsServerInputs().filter((_, i) => i !== index);
  renderDnsServerFields(rows);
}

function setDnsSettingsStatus(message, tone = '') {
  const el = $('dns-settings-status');
  if (!el) return;
  el.textContent = message;
  el.className = `card-foot${tone ? ' ' + tone : ''}`;
}

async function loadDnsSettings() {
  renderDnsServerFields([]);
  try {
    const data = await api('/dns/settings');
    if (data.error) throw new Error(data.error);
    renderDnsServerFields(data.servers || []);
    $('dns-update-url').value = data.updateUrl || '';
    setDnsSettingsStatus('DNS override is ready');
  } catch (e) {
    $('dns-update-url').value = '';
    setDnsSettingsStatus('Failed to load DNS settings', 'red');
  }
}

async function saveDnsSettings() {
  const body = {
    servers: collectDnsServerInputs(),
    updateUrl: $('dns-update-url').value.trim(),
  };
  try {
    const res = await api('/dns/settings', { method: 'POST', body });
    if (res.error) throw new Error(res.error);
    renderDnsServerFields((res.settings || {}).servers || body.servers);
    $('dns-update-url').value = (res.settings || {}).updateUrl || body.updateUrl;
    setDnsSettingsStatus('DNS settings saved', 'green');
  } catch (e) {
    setDnsSettingsStatus('Failed to save DNS settings: ' + e.message, 'red');
  }
}

async function runDnsUpdateUrl() {
  const url = $('dns-update-url').value.trim();
  if (!url) {
    setDnsSettingsStatus('Enter a DNS update URL first', 'red');
    return;
  }
  try {
    const res = await api('/dns/update-ip', { method: 'POST', body: { url } });
    if (res.error) throw new Error(res.error);
    const output = res.output ? `Curl OK: ${String(res.output).slice(0, 180)}` : 'Curl completed successfully';
    setDnsSettingsStatus(output, 'green');
  } catch (e) {
    setDnsSettingsStatus('Curl failed: ' + e.message, 'red');
  }
}

// ─── Admin Credentials ───────────────────────────────
async function loadAdminInfo() {
  try {
    const data = await api('/admin');
    $('admin-username').value = data.username || '';
  } catch {}
}

async function saveAdminCredentials() {
  const currentPassword = $('admin-current-pass').value;
  const newUsername = $('admin-username').value.trim();
  const newPassword = $('admin-new-pass').value;
  if (!currentPassword) { showToast('Enter current password'); return; }
  try {
    const r = await api('/admin', { method: 'POST', body: { currentPassword, newUsername, newPassword: newPassword || undefined } });
    $('admin-cred-status').textContent = r.message || 'Saved';
    $('admin-cred-status').className = 'card-foot green';
    $('admin-current-pass').value = '';
    $('admin-new-pass').value = '';
    setTimeout(() => { window.location.href = '/login'; }, 1500);
  } catch (e) {
    $('admin-cred-status').textContent = 'Error: wrong password';
    $('admin-cred-status').className = 'card-foot red';
  }
}

// ─── Connections ─────────────────────────────────────
let connections = [];
let remoteClients = [];
const $connList = $('conn-list');
const $connEmpty = $('conn-empty');
const $connCountPill = $('conn-count-pill');
const $connCountText = $('conn-count-text');
const $navDotConn = $('nav-dot-conn');

// V2Ray client state
let v2rayClients = [];          // from /api/local-xray/client-stats
let accessLogEntries = [];      // from /api/local-xray/access-log
let accessLogFilter = '';
let connRefreshTimer = null;

function isPrivateOrLocalIp(ip) {
  const value = String(ip || '').trim();
  if (!value) return true;
  if (value === '::1' || /^fe80:/i.test(value) || /^fc/i.test(value) || /^fd/i.test(value)) return true;
  if (/^10\./.test(value) || /^127\./.test(value) || /^169\.254\./.test(value) || /^192\.168\./.test(value)) return true;
  const octets = value.split('.').map(part => parseInt(part, 10));
  if (octets.length === 4 && octets[0] === 172 && octets[1] >= 16 && octets[1] <= 31) return true;
  return false;
}

function hasGeoInfo(ip) {
  return Object.prototype.hasOwnProperty.call(geoIpCache, ip);
}

function getClientHistoryForIp(ip) {
  const matches = (v2rayClients || []).filter(client => client.ip === ip);
  if (!matches.length) return null;
  const recentDests = matches
    .flatMap(client => client.recentDests || [])
    .sort((a, b) => (b.t || 0) - (a.t || 0));
  const deduped = [];
  const seen = new Set();
  for (const item of recentDests) {
    const key = `${item.dest || ''}|${item.outbound || ''}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(item);
    if (deduped.length >= 5) break;
  }
  return {
    accountName: matches.map(client => client.accountName).find(Boolean) || '',
    totalAccesses: matches.reduce((sum, client) => sum + (client.totalAccesses || 0), 0),
    lastSeen: matches.reduce((max, client) => Math.max(max, client.lastSeen || 0), 0),
    recentDests: deduped,
  };
}

function updateConnections(conns, clients) {
  removeSkeleton('conn-skeleton');
  connections = (conns || []).filter(c => c.srcIP !== '127.0.0.1');
  remoteClients = (clients || []).filter(c => c.clientIP !== '127.0.0.1');

  const hasRemote = remoteClients.length > 0;
  const count = hasRemote ? remoteClients.length : connections.length;
  const uniqueIPs = hasRemote
    ? [...new Set(remoteClients.map(c => c.clientIP))]
    : [...new Set(connections.map(c => c.srcIP))];

  if ($navDotConn) $navDotConn.className = `nav-dot ${count > 0 ? 'green' : ''}`;

  // Update SSH stat counter
  const sshEl = $('cs-ssh-count');
  if (sshEl) sshEl.textContent = count;

  // Update pill only if no V2Ray clients (pill managed by updateConnSummary otherwise)
  if (!v2rayClients.length) {
    $connCountText.textContent = `${uniqueIPs.length} IP${uniqueIPs.length !== 1 ? 's' : ''} · ${count} conn`;
    $connCountPill.className = `pill ${count > 0 ? 'green' : ''}`;
  }

  // Update geo-IP map
  if (currentPage === 'connections') updateGeoIpMap();

  if (uniqueIPs.length === 0) {
    $connList.querySelectorAll('.ip-row').forEach(el => el.remove());
    $connEmpty.classList.remove('hidden');
    return;
  }
  $connEmpty.classList.add('hidden');

  const ipSet = new Set(uniqueIPs);
  $connList.querySelectorAll('.ip-row').forEach(el => {
    if (!ipSet.has(el.dataset.ip)) el.remove();
  });

  if (hasRemote) {
    const byIp = {};
    for (const rc of remoteClients) {
      if (!byIp[rc.clientIP]) byIp[rc.clientIP] = { clients: [], ...rc };
      byIp[rc.clientIP].clients.push(rc);
    }
    for (const [ip, group] of Object.entries(byIp)) {
      renderIpRow(ip, {
        rdns: group.rdns,
        connCount: group.clients.length,
        totalIn: group.totalBytesIn,
        totalOut: group.totalBytesOut,
        speedLimit: group.speedLimit,
      });
    }
  } else {
    const byIp = {};
    for (const c of connections) {
      if (!byIp[c.srcIP]) byIp[c.srcIP] = { conns: [], rdns: c.rdns, speedLimit: c.speedLimit, ipTotalBytesIn: c.ipTotalBytesIn, ipTotalBytesOut: c.ipTotalBytesOut };
      byIp[c.srcIP].conns.push(c);
    }
    for (const [ip, group] of Object.entries(byIp)) {
      const totalIn = group.conns.reduce((s, c) => s + c.bytesIn, 0);
      const totalOut = group.conns.reduce((s, c) => s + c.bytesOut, 0);
      renderIpRow(ip, {
        rdns: group.rdns,
        connCount: group.conns.length,
        totalIn: group.ipTotalBytesIn || totalIn,
        totalOut: group.ipTotalBytesOut || totalOut,
        speedLimit: group.speedLimit,
      });
    }
  }
}

function renderIpRow(ip, data) {
  let row = $connList.querySelector(`.ip-row[data-ip="${ip}"]`);
  if (!row) {
    row = document.createElement('div');
    row.className = 'ip-row';
    row.dataset.ip = ip;
    $connList.appendChild(row);
  }

  const dlLimit = data.speedLimit ? Math.round(data.speedLimit.download / 1024) : '';
  const ulLimit = data.speedLimit ? Math.round(data.speedLimit.upload / 1024) : '';
  const hasLimit = data.speedLimit && (data.speedLimit.download > 0 || data.speedLimit.upload > 0);
  const clientHistory = getClientHistoryForIp(ip);
  const historyHtml = clientHistory
    ? `<div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--border)">
        <div style="display:flex;gap:10px;flex-wrap:wrap;font-size:10px;color:var(--t3)">
          ${clientHistory.accountName ? `<span>Account: <strong style="color:var(--t2)">${escapeHtml(clientHistory.accountName)}</strong></span>` : ''}
          ${clientHistory.totalAccesses ? `<span>${clientHistory.totalAccesses} reqs</span>` : ''}
          ${clientHistory.lastSeen ? `<span>last ${fmtRelTime(clientHistory.lastSeen)}</span>` : ''}
        </div>
        ${clientHistory.recentDests.length ? `<div style="display:flex;flex-wrap:wrap;gap:6px;margin-top:8px">${clientHistory.recentDests.map(item => {
          const via = item.outbound && item.outbound !== 'direct' ? ` <span style="color:var(--accent)">[${escapeHtml(item.outbound)}]</span>` : '';
          return `<span style="font-size:10px;padding:4px 8px;border-radius:999px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.06);max-width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escapeHtml(item.dest || 'unknown')}${via}</span>`;
        }).join('')}</div>` : ''}
      </div>`
    : '';

  row.innerHTML = `
    <div class="ip-row-main">
      <div class="ip-row-ip">
        <span class="ip-addr">${escapeHtml(ip)}</span>
        ${data.rdns ? `<span class="ip-rdns">${escapeHtml(data.rdns)}</span>` : ''}
      </div>
      <div class="ip-row-stats">
        <span class="ip-row-stat">${data.connCount} conn</span>
        <span class="ip-row-stat">&darr; ${formatBytes(data.totalIn)}</span>
        <span class="ip-row-stat">&uarr; ${formatBytes(data.totalOut)}</span>
      </div>
      <div class="ip-row-controls">
        <input type="number" class="speed-input-sm" value="${dlLimit}" min="0" placeholder="DL" title="Download KB/s" data-ip="${escapeHtml(ip)}" data-dir="dl" onchange="setSpeedLimit(this)" />
        <input type="number" class="speed-input-sm" value="${ulLimit}" min="0" placeholder="UL" title="Upload KB/s" data-ip="${escapeHtml(ip)}" data-dir="ul" onchange="setSpeedLimit(this)" />
        ${hasLimit ? '<span class="limit-badge">Limited</span>' : ''}
      </div>
      <div class="ip-row-actions">
        <button class="btn-icon danger" onclick="dropIp('${escapeHtml(ip)}')" title="Drop all">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
        </button>
      </div>
    </div>
    ${historyHtml}
  `;
}

// ─── V2Ray Client Cards ───────────────────────────────
async function refreshConnectionsTab() {
  try {
    if (!localAccountsList.length) await loadLocalAccounts();
    [v2rayClients, accessLogEntries] = await Promise.all([
      api('/local-xray/client-stats'),
      api('/local-xray/access-log?limit=500'),
    ]);
  } catch { return; }

  // Fetch geo for all unique IPs
  const allIps = [...new Set([
    ...v2rayClients.map(c => c.ip),
    ...connections.map(c => c.srcIP),
    ...remoteClients.map(c => c.clientIP),
  ])];
  fetchGeoForIps(allIps, () => {
    renderV2RayClients();
    renderAccessLog();
    updateConnSummary();
    updateGeoIpMap();
    updateConnections(connections, remoteClients);
  });

  renderV2RayClients();
  renderAccessLog();
  updateConnSummary();
  updateConnections(connections, remoteClients);
}

function updateConnSummary() {
  const activeIps = [...new Set(v2rayClients.map(c => c.ip))];
  const sshIps = [...new Set([...connections.map(c => c.srcIP), ...remoteClients.map(c => c.clientIP)])];
  const allIps = [...new Set([...activeIps, ...sshIps])];
  const countries = new Set(allIps.map(ip => geoIpCache[ip]?.cc).filter(Boolean));

  const elIps = $('cs-active-ips');
  const elLog = $('cs-log-count');
  const elCtr = $('cs-countries');
  if (elIps) elIps.textContent = activeIps.length;
  if (elLog) elLog.textContent = accessLogEntries.length;
  if (elCtr) elCtr.textContent = countries.size;

  const total = allIps.length;
  $connCountText.textContent = total ? `${total} IP${total !== 1 ? 's' : ''}` : '–';
  $connCountPill.className = `pill ${total > 0 ? 'green' : ''}`;
  if ($navDotConn) $navDotConn.className = `nav-dot ${total > 0 ? 'green' : ''}`;
}

function renderV2RayClients() {
  const grid = $('v2ray-clients-grid');
  const countEl = $('v2ray-client-count');
  if (!grid) return;

  if (!v2rayClients.length) {
    grid.innerHTML = '<div class="empty"><p>No V2Ray clients connected</p><p class="hint">Clients appear after they connect and generate traffic</p></div>';
    if (countEl) countEl.textContent = '';
    return;
  }

  if (countEl) countEl.textContent = `${v2rayClients.length} client${v2rayClients.length !== 1 ? 's' : ''}`;

  const rows = v2rayClients.map(c => {
    const geo = geoIpCache[c.ip];
    const flag = geo?.cc ? String.fromCodePoint(...[...geo.cc.toUpperCase()].map(ch => 127397 + ch.charCodeAt(0))) : '';
    const geoStr = geo ? `${flag}${geo.city ? ' ' + geo.city : ''}${geo.country ? ', ' + geo.country : ''}` : '';
    const lastSeen = c.lastSeen ? fmtRelTime(c.lastSeen) : '–';
    const isOnline = c.lastSeen && Date.now() - c.lastSeen < 120000;
    const recentDomains = (c.recentDests || []).slice(0, 3).map(d => escapeHtml(d.dest)).join(' · ');
    return `<div class="vc-row${isOnline ? ' online' : ''}">
      <div class="vc-dot"><div class="dash-svc-dot${isOnline ? ' green' : ''}"></div></div>
      <div class="vc-body">
        <div class="vc-primary">
          <span class="vc-ip">${escapeHtml(c.ip)}</span>
          ${geoStr ? `<span class="vc-geo">${geoStr}</span>` : ''}
          ${c.accountName ? `<span class="vc-acct">${escapeHtml(c.accountName)}</span>` : ''}
        </div>
        <div class="vc-secondary">
          ${c.totalAccesses ? `<span>${c.totalAccesses} req</span>` : ''}
          <span>last ${lastSeen}</span>
          ${recentDomains ? `<span class="vc-dests">${recentDomains}</span>` : ''}
        </div>
      </div>
      <button class="btn-icon" onclick="filterAccessLogByIp('${escapeHtml(c.ip)}')" title="View access log">
        <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
      </button>
    </div>`;
  }).join('');
  grid.innerHTML = `<div class="vc-list">${rows}</div>`;
}

function fmtRelTime(ts) {
  const diff = Date.now() - ts;
  if (diff < 60000) return `${Math.floor(diff / 1000)}s ago`;
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
  return `${Math.floor(diff / 86400000)}d ago`;
}

function fmtTime(ts) {
  const d = new Date(ts);
  return d.toTimeString().slice(0, 8);
}

// ─── Access Log ───────────────────────────────────────
function filterAccessLog() {
  accessLogFilter = ($('access-log-filter')?.value || '').toLowerCase();
  renderAccessLog();
}

function filterAccessLogByIp(ip) {
  const el = $('access-log-filter');
  if (el) { el.value = ip; accessLogFilter = ip.toLowerCase(); }
  renderAccessLog();
  $('conn-access-log')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

async function clearLocalAccessLog() {
  await api('/local-xray/access-log', { method: 'DELETE' });
  accessLogEntries = [];
  v2rayClients = [];
  renderAccessLog();
  renderV2RayClients();
  updateConnSummary();
}

function renderAccessLog() {
  const container = $('conn-access-log');
  const countEl = $('access-log-count');
  if (!container) return;

  const filtered = accessLogFilter
    ? accessLogEntries.filter(e => e.srcIp?.includes(accessLogFilter) || e.dest?.toLowerCase().includes(accessLogFilter) || e.email?.includes(accessLogFilter))
    : accessLogEntries;

  if (countEl) countEl.textContent = `(${filtered.length})`;

  if (!filtered.length) {
    container.innerHTML = '<div class="empty" style="padding:20px"><p>No access log entries</p><p class="hint">Enable xray server and connect clients to see access log</p></div>';
    return;
  }

  const accountsById = {};
  (localAccountsList || []).forEach(a => { accountsById[a.id] = a.name; });

  const rows = [...filtered].reverse().slice(0, 500).map(e => {
    const geo = geoIpCache[e.srcIp];
    const flag = geo?.cc ? String.fromCodePoint(...[...geo.cc.toUpperCase()].map(ch => 127397 + ch.charCodeAt(0))) : '';
    const acctName = e.accountName || (e.accountId ? (accountsById[e.accountId] || e.accountId) : '');
    const isRejected = e.status === 'rejected';
    return `<tr>
      <td class="al-time">${fmtTime(e.t)}</td>
      <td class="al-ip" style="cursor:pointer" onclick="filterAccessLogByIp('${escapeHtml(e.srcIp)}')" title="Filter by this IP">${flag} ${escapeHtml(e.srcIp)}</td>
      <td style="font-size:10px;color:var(--t3)">${escapeHtml(acctName)}</td>
      <td class="al-dest ${isRejected ? 'al-rejected' : ''}" title="${escapeHtml(e.dest || '')}">${escapeHtml(e.dest || '–')}</td>
      <td class="al-via">${escapeHtml(e.outbound || '')}</td>
    </tr>`;
  }).join('');

  container.innerHTML = `<div class="access-log-wrap"><table class="access-log-table">
    <thead><tr><th>Time</th><th>IP</th><th>Account</th><th>Destination</th><th>Via</th></tr></thead>
    <tbody>${rows}</tbody>
  </table></div>`;
}

async function dropConnection(id) {
  try { await api(`/connections/${id}/drop`, { method: 'POST' }); }
  catch (e) { console.error('[Connections] Drop failed:', e); }
}

async function dropIp(ip) {
  try { await api(`/connections/drop-ip/${ip}`, { method: 'POST' }); }
  catch (e) { console.error('[Connections] Drop IP failed:', e); }
}

async function setSpeedLimit(el) {
  const ip = el.dataset.ip;
  const dir = el.dataset.dir;
  const bps = (parseInt(el.value) || 0) * 1024;
  const container = el.closest('.ip-row-controls');
  const otherInput = container.querySelector(`.speed-input-sm[data-dir="${dir === 'dl' ? 'ul' : 'dl'}"]`);
  const otherBps = (parseInt(otherInput.value) || 0) * 1024;
  try {
    await api(`/connections/limit/${ip}`, {
      method: 'POST',
      body: { download: dir === 'dl' ? bps : otherBps, upload: dir === 'ul' ? bps : otherBps }
    });
  } catch (e) { console.error('[Connections] Speed limit failed:', e); }
}

// ─── Geo-IP Map ──────────────────────────────────────
const geoIpCache = {};
let geoIpMapData = []; // [{ip, lat, lon, country}]

function updateGeoIpMap() {
  const sshIps = remoteClients.length > 0
    ? [...new Set(remoteClients.map(c => c.clientIP))]
    : [...new Set(connections.map(c => c.srcIP))];
  const v2Ips = [...new Set(v2rayClients.map(c => c.ip))];
  const uniqueIPs = [...new Set([...sshIps, ...v2Ips])];
  fetchGeoForIps(uniqueIPs, () => {
    buildGeoMapData(uniqueIPs);
    renderGeoMap();
  });
}

function buildGeoMapData(ips) {
  geoIpMapData = [];
  const seen = new Set();
  for (const ip of ips) {
    if (seen.has(ip)) continue;
    seen.add(ip);
    const geo = geoIpCache[ip];
    if (geo && geo.lat != null) geoIpMapData.push({ ip, ...geo });
  }
}

function fetchGeoForIps(ips, callback) {
  const uniqueIps = [...new Set((ips || []).map(ip => String(ip || '').trim()).filter(Boolean))];
  const toFetch = uniqueIps.filter(ip => !hasGeoInfo(ip) && !isPrivateOrLocalIp(ip));
  if (toFetch.length === 0) { if (callback) callback(); return; }
  api('/geoip/batch', {
    method: 'POST',
    body: { ips: toFetch.slice(0, 50) },
  }).then(results => {
    for (const [ip, geo] of Object.entries(results || {})) {
      geoIpCache[ip] = geo;
    }
    if (callback) callback();
  }).catch(() => { if (callback) callback(); });
}

function geoTag(ip) {
  const g = geoIpCache[ip];
  if (!g) return '';
  const flag = g.cc ? String.fromCodePoint(...[...g.cc.toUpperCase()].map(c => 127397 + c.charCodeAt(0))) : '';
  return ` <span style="color:var(--t3);font-size:10px">${flag} ${g.city || g.country || ''}</span>`;
}

function renderGeoMap() {
  const canvas = $('geoip-canvas');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth;
  const h = Math.round(w * 320 / 700);
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  canvas.style.height = h + 'px';
  ctx.scale(dpr, dpr);

  // Background
  const bgColor = getComputedStyle(document.body).getPropertyValue('--bg-2') || '#1a1a2e';
  ctx.fillStyle = bgColor;
  ctx.fillRect(0, 0, w, h);

  // Simple world outline (mercator-like grid)
  ctx.strokeStyle = getComputedStyle(document.body).getPropertyValue('--border') || '#333';
  ctx.lineWidth = 0.5;
  // Meridians and parallels
  for (let lon = -180; lon <= 180; lon += 30) {
    const x = lonToX(lon, w);
    ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, h); ctx.stroke();
  }
  for (let lat = -60; lat <= 80; lat += 20) {
    const y = latToY(lat, h);
    ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke();
  }

  // Draw equator slightly brighter
  ctx.strokeStyle = (getComputedStyle(document.body).getPropertyValue('--border') || '#333');
  ctx.lineWidth = 1;
  const eqY = latToY(0, h);
  ctx.beginPath(); ctx.moveTo(0, eqY); ctx.lineTo(w, eqY); ctx.stroke();

  // Plot dots
  const countLabel = $('geoip-count');
  if (countLabel) countLabel.textContent = geoIpMapData.length ? `${geoIpMapData.length} location${geoIpMapData.length !== 1 ? 's' : ''}` : '';

  for (const pt of geoIpMapData) {
    const x = lonToX(pt.lon, w);
    const y = latToY(pt.lat, h);

    // Glow
    ctx.beginPath();
    ctx.arc(x, y, 8, 0, Math.PI * 2);
    ctx.fillStyle = 'rgba(34, 197, 94, 0.15)';
    ctx.fill();

    // Dot
    ctx.beginPath();
    ctx.arc(x, y, 3.5, 0, Math.PI * 2);
    ctx.fillStyle = '#22c55e';
    ctx.fill();

    // Label
    ctx.fillStyle = getComputedStyle(document.body).getPropertyValue('--t2') || '#ccc';
    ctx.font = '9px sans-serif';
    ctx.textAlign = 'left';
    const label = pt.city ? `${pt.ip} · ${pt.city}` : `${pt.ip} (${pt.cc || '?'})`;
    ctx.fillText(label, x + 6, y + 3);
  }
}

function lonToX(lon, w) { return ((lon + 180) / 360) * w; }
function latToY(lat, h) {
  // Simple equirectangular with slight vertical compress
  const clampLat = Math.max(-80, Math.min(85, lat));
  return ((85 - clampLat) / 165) * h;
}

// ─── OpenVPN ─────────────────────────────────────────
let ovpnState = {
  status: 'disconnected', statusMessage: 'Not connected',
  uptime: 0, connectedSince: null, tunDevice: null,
  routedHosts: [], profiles: [],
};
let vpnUpstreamState = {
  preferred: 'openvpn',
  effective: 'direct',
  openvpnReady: false,
  openconnectReady: false,
  openconnectMode: 'network',
};

const $ovpnPill = $('ovpn-pill');
const $ovpnPillText = $('ovpn-pill-text');
const $ovpnStatusText = $('ovpn-status-text');
const $ovpnTunDevice = $('ovpn-tun-device');
const $ovpnRoutedHosts = $('ovpn-routed-hosts');
const $ovpnUptime = $('ovpn-uptime');
const $ovpnStatusMsg = $('ovpn-status-msg');
const $ovpnBtnConnect = $('ovpn-btn-connect');
const $ovpnBtnStop = $('ovpn-btn-stop');
const $ovpnProfileSelect = $('ovpn-profile-select');
const $vpnUpstreamMode = $('vpn-upstream-mode');
const $vpnUpstreamSummary = $('vpn-upstream-summary');
const $vpnUpstreamDetail = $('vpn-upstream-detail');

let ovpnLogsVisible = false;

function vpnModeLabel(mode) {
  return {
    openvpn: 'OpenVPN',
    openconnect: 'OpenConnect',
    both: 'Both (Load Balance)',
    direct: 'Direct',
  }[mode] || mode;
}

function applyVpnUpstreamState(data) {
  if (!data) return;
  vpnUpstreamState = {
    preferred: data.preferred || vpnUpstreamState.preferred,
    effective: data.effective || vpnUpstreamState.effective,
    openvpnReady: !!data.openvpnReady,
    openconnectReady: !!data.openconnectReady,
    openconnectMode: data.openconnectMode || vpnUpstreamState.openconnectMode,
  };
  updateVpnUpstreamUI();
}

function updateVpnUpstreamUI() {
  if (!$vpnUpstreamMode) return;
  $vpnUpstreamMode.value = vpnUpstreamState.preferred || 'openvpn';
  $vpnUpstreamSummary.textContent = `Preferred: ${vpnModeLabel(vpnUpstreamState.preferred)}`;

  const notes = [];
  notes.push(vpnUpstreamState.openvpnReady ? 'OpenVPN ready' : 'OpenVPN not connected');
  if (vpnUpstreamState.openconnectReady) {
    notes.push('OpenConnect ready');
  } else if (vpnUpstreamState.openconnectMode !== 'local') {
    notes.push('OpenConnect routing requires local mode');
  } else {
    notes.push('OpenConnect not connected');
  }

  $vpnUpstreamDetail.textContent = `Effective: ${vpnModeLabel(vpnUpstreamState.effective)}. ${notes.join(' | ')}`;
}

async function saveVpnUpstreamMode() {
  try {
    const res = await api('/vpn/upstream-mode', {
      method: 'POST',
      body: { mode: $vpnUpstreamMode.value },
    });
    if (res.error) {
      showToast(res.error, 'error');
      return;
    }
    applyVpnUpstreamState(res);
    showToast(`VPN routing preference set to ${vpnModeLabel(res.preferred)}`, 'success');
    loadVpnPage();
  } catch (e) {
    console.error('[VPN] Failed to save upstream mode:', e);
    showToast('Failed to save VPN routing preference: ' + e.message, 'error');
  }
}

async function loadVpnPage() {
  try {
    const data = await api('/openvpn/status');
    ovpnState = {
      status: data.status || 'disconnected',
      statusMessage: data.statusMessage || 'Idle',
      uptime: data.uptime || 0,
      connectedSince: data.connectedSince || null,
      tunDevice: data.tunDevice || null,
      routedHosts: data.routedHosts || [],
      profiles: data.profiles || [],
    };
    applyVpnUpstreamState(data.upstreamRouting);
    updateOvpnUI();
    if (ovpnLogsVisible) loadOvpnLogs();
    // Load saved credentials
    loadOvpnSavedCredentials();
  } catch (e) {
    console.error('[OpenVPN] Status load failed:', e);
  }
}

async function loadOvpnSavedCredentials() {
  try {
    const creds = await api('/openvpn/saved-credentials');
    if (creds.saved) {
      const uEl = $('ovpn-username');
      const pEl = $('ovpn-password');
      if (!uEl.value) uEl.value = creds.username || '';
      if (!pEl.value) pEl.value = creds.password || '';
      $('ovpn-save-creds').checked = true;
    }
  } catch {}
}

function updateOvpnUI() {
  removeSkeleton('vpn-skeleton');
  const s = ovpnState;
  const colorMap = { connected: 'green', connecting: 'amber', reconnecting: 'amber', error: 'red', disconnected: '' };
  const c = colorMap[s.status] || '';

  $ovpnPill.className = `pill ${c}`;
  $ovpnPillText.textContent = s.status.charAt(0).toUpperCase() + s.status.slice(1);
  $ovpnStatusText.textContent = s.statusMessage || s.status;
  $ovpnTunDevice.textContent = s.tunDevice || '--';
  $ovpnRoutedHosts.textContent = s.routedHosts.length > 0 ? s.routedHosts.join(', ') : 'None';
  $ovpnUptime.textContent = s.uptime > 0 ? formatUptime(s.uptime) : '--:--:--';
  $ovpnStatusMsg.textContent = s.statusMessage || 'Not connected';

  const isActive = s.status === 'connected' || s.status === 'connecting' || s.status === 'reconnecting';
  $ovpnBtnConnect.classList.toggle('hidden', isActive);
  $ovpnBtnStop.classList.toggle('hidden', !isActive);

  // Update profile dropdown
  const currentVal = $ovpnProfileSelect.value;
  const opts = '<option value="">-- Select --</option>' +
    (s.profiles || []).map(p => `<option value="${escapeHtml(p)}"${p === currentVal ? ' selected' : ''}>${escapeHtml(p)}</option>`).join('');
  if ($ovpnProfileSelect.innerHTML !== opts) $ovpnProfileSelect.innerHTML = opts;
  if (currentVal) $ovpnProfileSelect.value = currentVal;
}

async function ovpnConnect() {
  const profile = $ovpnProfileSelect.value;
  const profilePath = $('ovpn-profile-path').value.trim();
  const username = $('ovpn-username').value.trim();
  const password = $('ovpn-password').value;
  const saveCreds = $('ovpn-save-creds').checked;

  if (!profile && !profilePath) {
    showToast('Select a profile or enter a file path', 'error');
    return;
  }

  const body = { username: username || undefined, password: password || undefined, saveCredentials: saveCreds || undefined };
  if (profilePath) body.profilePath = profilePath;
  else body.profile = profile;

  try {
    const res = await api('/openvpn/start', { method: 'POST', body });
    if (res.error) {
      showToast(res.error, 'error');
      return;
    }
    showToast('OpenVPN connection started', 'success');
    loadVpnPage();
  } catch (e) {
    console.error('[OpenVPN] Connect failed:', e);
    showToast('Failed to connect: ' + e.message, 'error');
  }
}

async function ovpnStop() {
  try {
    const res = await api('/openvpn/stop', { method: 'POST' });
    if (res.error) {
      showToast(res.error, 'error');
      return;
    }
    loadVpnPage();
  } catch (e) {
    console.error('[OpenVPN] Stop failed:', e);
    showToast('Failed to stop OpenVPN: ' + e.message, 'error');
  }
}

async function ovpnUploadProfile(input) {
  const file = input.files[0];
  if (!file) return;
  const formData = new FormData();
  formData.append('profile', file);
  try {
    const resp = await fetch('/api/openvpn/upload', {
      method: 'POST',
      body: formData,
      credentials: 'same-origin',
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || 'Upload failed');
    const profiles = await api('/openvpn/profiles');
    ovpnState.profiles = profiles;
    updateOvpnUI();
    $ovpnProfileSelect.value = data.name;
    showToast(`Profile uploaded: ${data.name}`, 'success');
  } catch (e) {
    console.error('[OpenVPN] Upload failed:', e);
    showToast('Upload failed: ' + e.message, 'error');
  }
  input.value = '';
}

function toggleOvpnLogs() {
  ovpnLogsVisible = !ovpnLogsVisible;
  $('ovpn-logs-container').classList.toggle('hidden', !ovpnLogsVisible);
  $('ovpn-logs-toggle-text').textContent = ovpnLogsVisible ? 'Hide Logs' : 'Show Logs';
  if (ovpnLogsVisible) loadOvpnLogs();
}

async function loadOvpnLogs() {
  try {
    const logs = await api('/openvpn/logs');
    const $logs = $('ovpn-logs');
    $logs.innerHTML = '';
    for (const entry of logs) {
      const div = document.createElement('div');
      div.className = 'log-line';
      const time = new Date(entry.time).toLocaleTimeString();
      div.textContent = `[${time}] ${entry.text}`;
      $logs.appendChild(div);
    }
    $logs.scrollTop = $logs.scrollHeight;
  } catch (e) {
    console.error('[OpenVPN] Logs fetch failed:', e);
  }
}

// ─── Latency Sparkline ───────────────────────────────
function drawSparkline(history) {
  const canvas = $latencyGraph;
  if (!canvas || history.length < 2) return;
  canvas.classList.remove('hidden');
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.parentElement.offsetWidth;
  const h = 36;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  canvas.style.width = w + 'px';
  canvas.style.height = h + 'px';
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);

  const values = history.map(d => d.latency);
  const valid = values.filter(v => v !== null);
  const maxVal = Math.max(...valid, 1);
  const step = w / (values.length - 1);

  ctx.clearRect(0, 0, w, h);

  // Fill area
  ctx.beginPath();
  ctx.moveTo(0, h);
  for (let i = 0; i < values.length; i++) {
    const x = i * step;
    const y = values[i] !== null ? h - (values[i] / maxVal) * (h - 6) - 3 : h;
    ctx.lineTo(x, y);
  }
  ctx.lineTo(w, h);
  ctx.closePath();
  ctx.fillStyle = 'rgba(124,127,249,0.08)';
  ctx.fill();

  // Line
  ctx.beginPath();
  let started = false;
  for (let i = 0; i < values.length; i++) {
    if (values[i] === null) { started = false; continue; }
    const x = i * step;
    const y = h - (values[i] / maxVal) * (h - 6) - 3;
    if (!started) { ctx.moveTo(x, y); started = true; }
    else ctx.lineTo(x, y);
  }
  ctx.strokeStyle = '#7c7ff9';
  ctx.lineWidth = 1.5;
  ctx.stroke();

  // Red dots for failed checks
  for (let i = 0; i < values.length; i++) {
    if (values[i] === null) {
      ctx.beginPath();
      ctx.arc(i * step, h - 4, 2.5, 0, Math.PI * 2);
      ctx.fillStyle = '#f87171';
      ctx.fill();
    }
  }

  // Label max/min
  if (valid.length > 0) {
    const minVal = Math.min(...valid);
    ctx.font = '9px Inter, sans-serif';
    ctx.fillStyle = 'rgba(255,255,255,0.3)';
    ctx.textAlign = 'right';
    ctx.fillText(maxVal + 'ms', w - 2, 10);
    ctx.fillText(minVal + 'ms', w - 2, h - 2);
  }
}

// ─── Audit Log ───────────────────────────────────────
const $auditList = $('audit-list');
let auditLoaded = false;

const AUDIT_LABELS = {
  tunnel_connected: ['Connected', 'green'],
  tunnel_reconnected: ['Reconnected', 'green'],
  tunnel_reconnecting: ['Reconnecting', 'amber'],
  tunnel_stopped: ['Stopped', ''],
  tunnel_error: ['Error', 'red'],
  vpn_connected: ['VPN Up', 'green'],
  vpn_disconnected: ['VPN Down', ''],
  vpn_reconnecting: ['VPN Reconnecting', 'amber'],
  deploy: ['Deploy', 'purple'],
  config_imported: ['Config Import', 'purple'],
};

function formatAuditTime(ts) {
  const d = new Date(ts);
  const now = new Date();
  const time = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  if (d.toDateString() === now.toDateString()) return time;
  return d.toLocaleDateString([], { month: 'short', day: 'numeric' }) + ' ' + time;
}

function renderAuditEntry(entry) {
  const [label, color] = AUDIT_LABELS[entry.event] || [entry.event, ''];
  return `<div class="audit-entry">
    <span class="audit-time">${formatAuditTime(entry.time)}</span>
    <span class="audit-badge ${color}">${escapeHtml(label)}</span>
    <span class="audit-detail">${escapeHtml(entry.detail || '')}</span>
  </div>`;
}

function appendAuditEntry(entry) {
  if (!$auditList) return;
  $auditList.insertAdjacentHTML('afterbegin', renderAuditEntry(entry));
  while ($auditList.children.length > 100) $auditList.removeChild($auditList.lastChild);
}

async function loadAuditLog() {
  try {
    const logs = await api('/audit');
    if (!$auditList) return;
    $auditList.innerHTML = logs.slice().reverse().map(renderAuditEntry).join('');
    auditLoaded = true;
  } catch (e) {
    console.error('[Audit] Load failed:', e);
  }
}

// ─── Push Notifications ──────────────────────────────
let notificationsEnabled = localStorage.getItem('notif') === '1';

function initNotifToggle() {
  const el = $('notif-toggle');
  if (el) el.checked = notificationsEnabled;
  updateNotifStatus();
}

async function toggleNotifications(on) {
  if (on) {
    if (!('Notification' in window)) { alert('Notifications not supported'); $('notif-toggle').checked = false; return; }
    const perm = await Notification.requestPermission();
    if (perm !== 'granted') { $('notif-toggle').checked = false; updateNotifStatus(); return; }
    notificationsEnabled = true;
    localStorage.setItem('notif', '1');
  } else {
    notificationsEnabled = false;
    localStorage.removeItem('notif');
  }
  updateNotifStatus();
}

function updateNotifStatus() {
  const el = $('notif-status');
  if (el) el.textContent = notificationsEnabled ? 'Enabled' : 'Disabled';
}

function showPushNotification(event, message) {
  if (!notificationsEnabled || !('Notification' in window) || Notification.permission !== 'granted') return;
  const titles = {
    tunnel_connected: 'Tunnel Connected',
    tunnel_reconnected: 'Tunnel Reconnected',
    tunnel_reconnecting: 'Tunnel Reconnecting',
    tunnel_disconnected: 'Tunnel Disconnected',
    tunnel_error: 'Tunnel Error',
    vpn_connected: 'VPN Connected',
    vpn_reconnecting: 'VPN Reconnecting',
    vpn_disconnected: 'VPN Disconnected',
    health_warning: 'Health Warning',
  };
  try {
    new Notification(titles[event] || 'Tunnel Manager', { body: message, tag: event });
  } catch {
    if (navigator.serviceWorker && navigator.serviceWorker.ready) {
      navigator.serviceWorker.ready.then(reg => reg.showNotification(titles[event] || 'Tunnel Manager', { body: message, tag: event }));
    }
  }
}

// ─── Config Export/Import ────────────────────────────
function exportConfig() {
  window.location.href = '/api/config/export';
}

async function importConfig(input) {
  const file = input.files[0];
  if (!file) return;
  try {
    const text = await file.text();
    const config = JSON.parse(text);
    const res = await api('/config/import', { method: 'POST', body: config });
    if (res.ok) {
      const parts = [];
      if (res.tunnels) parts.push(`${res.tunnels} tunnels`);
      if (res.profiles) parts.push(`${res.profiles} profiles`);
      if (res.localAccounts) parts.push(`${res.localAccounts} accounts`);
      if (res.xrayLocalConfigs) parts.push(`${res.xrayLocalConfigs} xray configs`);
      alert(`Imported: ${parts.join(', ') || 'no changes'}`);
      loadTunnels();
      loadLocalAccounts();
    } else {
      alert('Import failed: ' + (res.error || 'Unknown error'));
    }
  } catch (e) {
    alert('Import failed: ' + e.message);
  }
  input.value = '';
}

// ─── Deploy ──────────────────────────────────────────
async function deployArchive(input) {
  const file = input.files[0];
  if (!file) return;
  if (!confirm(`Deploy ${file.name}? The server will restart.`)) { input.value = ''; return; }
  const status = $('deploy-status');
  status.textContent = 'Uploading...';
  try {
    const formData = new FormData();
    formData.append('archive', file);
    const resp = await fetch('/api/deploy', { method: 'POST', body: formData, credentials: 'same-origin' });
    const data = await resp.json();
    if (data.ok) {
      status.textContent = `Updated: ${data.updated.join(', ')}. Restarting...`;
      setTimeout(() => location.reload(), 4000);
    } else {
      status.textContent = 'Failed: ' + (data.error || 'Unknown error');
    }
  } catch (e) {
    status.textContent = 'Deploy error: ' + e.message;
  }
  input.value = '';
}

// ─── V2Ray Sub-tabs ─────────────────────────────────
let v2rayCurrentTab = 'local';

function switchV2rayTab(tab) {
  v2rayCurrentTab = tab;
  $('v2ray-tab-local').classList.toggle('hidden', tab !== 'local');
  $('v2ray-tab-remote').classList.toggle('hidden', tab !== 'remote');
  document.querySelectorAll('.sub-tab[data-subtab]').forEach(el => {
    el.classList.toggle('active', el.dataset.subtab === 'v2ray-' + tab);
  });
  if (tab === 'remote' && !xuiLoaded) loadXuiData();
}

// ─── Local Account Manager ──────────────────────────
let localAccountsList = [];
let localAccountsLoaded = false;
let localXrayOutbound = 'direct'; // updated from status API
let accPollTimer = null;

function startAccPolling() {
  stopAccPolling();
  accPollTimer = setInterval(() => {
    if (currentPage === 'v2ray' && v2rayCurrentTab === 'local') loadLocalAccounts();
  }, 5000);
}

function stopAccPolling() {
  if (accPollTimer) { clearInterval(accPollTimer); accPollTimer = null; }
}

async function loadLocalAccountsPage() {
  if (!localAccountsLoaded) {
    await Promise.all([loadLocalAccounts(), loadLocalAccountInterfaces(), loadLocalXraySettings(), loadLocalRoutes(), loadXrayLocalConfigs()]);
    localAccountsLoaded = true;
  }
  refreshLocalXrayStatus();
  startAccPolling();
}

async function loadLocalXraySettings() {
  try {
    const s = await api('/local-xray/settings');
    const sel = $('local-xray-outbound');
    sel.innerHTML = '<option value="direct">Direct (Freedom)</option><option value="socks">SOCKS Proxy</option>';
    const socksFields = $('local-xray-socks-fields');
    const socksNote = $('local-xray-socks-note');
    if (s.outbound && s.outbound.startsWith('socks:')) {
      sel.value = 'socks';
      const parts = s.outbound.split(':');
      $('local-xray-socks-host').value = parts[1] || '127.0.0.1';
      $('local-xray-socks-port').value = parts[2] || '10808';
      socksFields.style.display = '';
      if (socksNote) socksNote.style.display = '';
    } else {
      sel.value = 'direct';
      socksFields.style.display = 'none';
      if (socksNote) socksNote.style.display = 'none';
    }
    // Load domain strategy
    const dsEl = $('domain-strategy');
    if (dsEl && s.domainStrategy) dsEl.value = s.domainStrategy;
  } catch {}
}

async function saveLocalXrayOutbound() {
  const sel = $('local-xray-outbound');
  const socksFields = $('local-xray-socks-fields');
  const socksNote = $('local-xray-socks-note');
  let outbound;
  if (sel.value === 'socks') {
    socksFields.style.display = '';
    if (socksNote) socksNote.style.display = '';
    const host = $('local-xray-socks-host').value.trim() || '127.0.0.1';
    const port = $('local-xray-socks-port').value.trim() || '10808';
    outbound = `socks:${host}:${port}`;
  } else {
    socksFields.style.display = 'none';
    if (socksNote) socksNote.style.display = 'none';
    outbound = 'direct';
  }
  await api('/local-xray/settings', { method: 'POST', body: { outbound } });
}

// ─── Local Routing Rules ─────────────────────────────
let localRoutes = [];

async function loadLocalRoutes() {
  try {
    localRoutes = await api('/local-xray/routes');
    renderLocalRoutes();
  } catch {}
}

function renderLocalRoutes() {
  const el = $('local-routes-list');
  const countEl = $('route-count');
  if (!localRoutes.length) {
    el.innerHTML = '<div class="empty"><p>No routing rules yet</p></div>';
    if (countEl) countEl.style.display = 'none';
    return;
  }
  if (countEl) { countEl.style.display = 'inline'; countEl.textContent = localRoutes.length; }

  const obLabels = { direct: 'Direct', proxy: 'Proxy', block: 'Block' };
  const obCls = { direct: 'rt-direct', proxy: 'rt-proxy', block: 'rt-block' };

  let rows = '';
  for (let i = 0; i < localRoutes.length; i++) {
    const r = localRoutes[i];
    const typeLabel = r.type === 'ip' ? 'IP' : 'Domain';
    const typeCls = r.type === 'ip' ? 'rt-type-ip' : 'rt-type-domain';
    const patterns = r.pattern.split(',').map(s => s.trim()).filter(Boolean);
    const patStr = patterns.length > 2
      ? patterns.slice(0, 2).map(p => escapeHtml(p)).join(', ') + ` <span class="rt-more">+${patterns.length - 2}</span>`
      : patterns.map(p => escapeHtml(p)).join(', ');

    rows += `<tr class="rt-row" data-id="${r.id}">
      <td class="rt-cell rt-num">${i + 1}</td>
      <td class="rt-cell"><span class="rt-type ${typeCls}">${typeLabel}</span></td>
      <td class="rt-cell rt-pattern">${patStr}</td>
      <td class="rt-cell"><span class="rt-action ${obCls[r.outboundTag] || ''}">${obLabels[r.outboundTag] || r.outboundTag}</span></td>
      <td class="rt-cell rt-actions">
        <button class="btn-icon" onclick="editRoute('${r.id}')" title="Edit">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
        </button>
        <button class="btn-icon danger" onclick="deleteRoute('${r.id}')" title="Delete">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/></svg>
        </button>
      </td>
    </tr>`;
  }

  el.innerHTML = `
    <table class="rt-table">
      <thead>
        <tr>
          <th class="rt-th rt-num">#</th>
          <th class="rt-th">Type</th>
          <th class="rt-th">Pattern</th>
          <th class="rt-th">Action</th>
          <th class="rt-th rt-actions"></th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>`;
}

const ROUTE_PRESETS = {
  'ir-direct':     { type: 'domain', pattern: 'geosite:category-ir', outboundTag: 'direct', label: 'Iran Domains → Direct' },
  'ir-ip-direct':  { type: 'ip',     pattern: 'geoip:ir',   outboundTag: 'direct', label: 'Iran IPs → Direct' },
  'cn-direct':     { type: 'domain', pattern: 'geosite:cn', outboundTag: 'direct', label: 'China Domains → Direct' },
  'private-direct':{ type: 'ip',     pattern: 'geoip:private', outboundTag: 'direct', label: 'Private IPs → Direct' },
  'ads-block':     { type: 'domain', pattern: 'geosite:category-ads-all', outboundTag: 'block', label: 'Ads → Block' },
};

async function applyRoutePreset(key) {
  const preset = ROUTE_PRESETS[key];
  if (!preset) return;
  // Check if this rule already exists
  const exists = localRoutes.some(r => r.pattern === preset.pattern && r.outboundTag === preset.outboundTag);
  if (exists) { alert(`Rule "${preset.label}" already exists`); return; }
  try {
    const res = await api('/local-xray/routes', { method: 'POST', body: preset });
    if (res.error) { alert('Failed: ' + res.error); return; }
    $('route-modal').classList.add('hidden');
    await loadLocalRoutes();
  } catch (e) { alert('Error: ' + e.message); }
}

function openAddRoute() {
  $('route-edit-id').value = '';
  $('route-type').value = 'domain';
  $('route-pattern').value = '';
  $('route-outbound').value = 'direct';
  $('route-modal-title').textContent = 'Add Rule';
  updateRoutePatternHint();
  $('route-modal').classList.remove('hidden');
}

function editRoute(id) {
  const r = localRoutes.find(x => x.id === id);
  if (!r) return;
  $('route-edit-id').value = r.id;
  $('route-type').value = r.type;
  $('route-pattern').value = r.pattern;
  $('route-outbound').value = r.outboundTag;
  $('route-modal-title').textContent = 'Edit Rule';
  updateRoutePatternHint();
  $('route-modal').classList.remove('hidden');
}

function updateRoutePatternHint() {
  const type = $('route-type').value;
  const el = $('route-pattern-hint');
  if (type === 'ip') {
    el.innerHTML = `
      <span>Examples (click to insert):</span>
      <code class="route-example" onclick="insertRoutePattern('geoip:ir')">geoip:ir</code>
      <code class="route-example" onclick="insertRoutePattern('geoip:private')">geoip:private</code>
      <code class="route-example" onclick="insertRoutePattern('geoip:cn')">geoip:cn</code>
      <code class="route-example" onclick="insertRoutePattern('10.0.0.0/8')">10.0.0.0/8</code>
      <code class="route-example" onclick="insertRoutePattern('192.168.0.0/16')">192.168.0.0/16</code>`;
    $('route-pattern').placeholder = 'geoip:ir, 10.0.0.0/8';
  } else {
    el.innerHTML = `
      <span>Examples (click to insert):</span>
      <code class="route-example" onclick="insertRoutePattern('geosite:category-ir')">geosite:category-ir</code>
      <code class="route-example" onclick="insertRoutePattern('geosite:cn')">geosite:cn</code>
      <code class="route-example" onclick="insertRoutePattern('geosite:category-ads-all')">geosite:category-ads-all</code>
      <code class="route-example" onclick="insertRoutePattern('domain:google.com')">domain:google.com</code>
      <code class="route-example" onclick="insertRoutePattern('keyword:facebook')">keyword:facebook</code>`;
    $('route-pattern').placeholder = 'geosite:category-ir, domain:google.com';
  }
}

function insertRoutePattern(text) {
  const el = $('route-pattern');
  const current = el.value.trim();
  if (current && current.includes(text)) return; // already there
  el.value = current ? current + ', ' + text : text;
  el.focus();
}

async function saveDomainStrategy() {
  const val = $('domain-strategy').value;
  try {
    await api('/local-xray/settings', { method: 'PUT', body: { domainStrategy: val } });
  } catch {}
}

async function submitRoute() {
  const id = $('route-edit-id').value;
  const type = $('route-type').value;
  const pattern = $('route-pattern').value.trim();
  const outboundTag = $('route-outbound').value;
  if (!pattern) { alert('Pattern is required'); return; }
  try {
    if (id) {
      await api(`/local-xray/routes/${id}`, { method: 'PUT', body: { type, pattern, outboundTag } });
    } else {
      const res = await api('/local-xray/routes', { method: 'POST', body: { type, pattern, outboundTag } });
      if (!res.ok) { alert('Failed: ' + (res.error || 'Unknown')); return; }
    }
    $('route-modal').classList.add('hidden');
    await loadLocalRoutes();
  } catch (e) { alert('Error: ' + e.message); }
}

async function deleteRoute(id) {
  if (!confirm('Delete this routing rule?')) return;
  await api(`/local-xray/routes/${id}`, { method: 'DELETE' });
  await loadLocalRoutes();
}

async function loadLocalAccountInterfaces() {
  try {
    const addrs = await api('/xray-local/interfaces');
    const sel = $('local-acc-listen');
    sel.innerHTML = '';
    for (const addr of addrs) {
      const label = addr === '0.0.0.0' ? '0.0.0.0 (all interfaces)' : addr;
      sel.insertAdjacentHTML('beforeend', `<option value="${addr}">${label}</option>`);
    }
  } catch {}
}

async function loadLocalAccounts() {
  try {
    localAccountsList = await api('/local-accounts');
    renderLocalAccounts();
    if (currentPage === 'dashboard') updateDashboard();
  } catch {}
}

function fmtExpiry(ts) {
  if (!ts) return 'Never';
  const d = new Date(ts);
  const now = Date.now();
  if (ts <= now) return 'Expired';
  const diff = ts - now;
  const days = Math.floor(diff / (24*60*60*1000));
  if (days > 0) return `${days}d left (${d.toLocaleDateString()})`;
  const hrs = Math.floor(diff / (60*60*1000));
  return `${hrs}h left`;
}

function fmtUsage(used, limit) {
  const u = used || 0;
  const fmt = (b) => {
    if (!b) return '0 B';
    const units = ['B','KB','MB','GB','TB'];
    const i = Math.floor(Math.log(b) / Math.log(1024));
    return (b / Math.pow(1024, i)).toFixed(1) + ' ' + units[i];
  };
  if (!limit) return `${fmt(u)} / Unlimited`;
  const pct = Math.min(100, (u / limit * 100)).toFixed(0);
  return `${fmt(u)} / ${fmt(limit)} (${pct}%)`;
}

// SVG icons for account card buttons
const IC = {
  copy: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>',
  qr: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/><rect x="14" y="14" width="3" height="3"/><line x1="21" y1="14" x2="21" y2="21"/><line x1="14" y1="21" x2="21" y2="21"/></svg>',
  edit: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/><path d="M18.5 2.5a2.12 2.12 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>',
  toggle: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="10"/><line x1="8" y1="12" x2="16" y2="12"/></svg>',
  reset: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><polyline points="1 4 1 10 7 10"/><path d="M3.51 15a9 9 0 105.64-12.36L1 10"/></svg>',
  key: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 11-7.78 7.78 5.5 5.5 0 017.78-7.78zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4"/></svg>',
  trash: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/></svg>',
  online: '<svg width="10" height="10" viewBox="0 0 10 10"><circle cx="5" cy="5" r="5" fill="#22c55e"/></svg>',
  offline: '<svg width="10" height="10" viewBox="0 0 10 10"><circle cx="5" cy="5" r="5" fill="#64748b"/></svg>',
};

function renderLocalAccounts() {
  const el = $('local-accounts-list');
  if (!localAccountsList.length) {
    el.innerHTML = '<div class="empty"><p>No accounts yet</p></div>';
    return;
  }

  // Filter
  let list = localAccountsList;
  if (accSearchTerm) {
    list = list.filter(a => a.name.toLowerCase().includes(accSearchTerm) || a.protocol.toLowerCase().includes(accSearchTerm));
  }

  // Sort
  list = [...list].sort((a, b) => {
    let va, vb;
    switch (accSortCol) {
      case 'name': va = a.name.toLowerCase(); vb = b.name.toLowerCase(); break;
      case 'status': va = a.online ? 1 : 0; vb = b.online ? 1 : 0; break;
      case 'traffic': va = a.usedTraffic || 0; vb = b.usedTraffic || 0; break;
      case 'port': va = a.port; vb = b.port; break;
      case 'expiry': va = a.expiresAt || Infinity; vb = b.expiresAt || Infinity; break;
      default: va = a.name; vb = b.name;
    }
    if (va < vb) return accSortAsc ? -1 : 1;
    if (va > vb) return accSortAsc ? 1 : -1;
    return 0;
  });

  const arrow = (col) => accSortCol === col ? `<span class="sort-arrow">${accSortAsc ? '\u25B2' : '\u25BC'}</span>` : '';

  let html = `<table class="acc-table">
    <thead><tr>
      <th onclick="sortAccounts('status')" style="width:32px">St${arrow('status')}</th>
      <th onclick="sortAccounts('name')">Name${arrow('name')}</th>
      <th onclick="sortAccounts('port')">Port${arrow('port')}</th>
      <th onclick="sortAccounts('traffic')">Traffic${arrow('traffic')}</th>
      <th>Conn</th>
      <th onclick="sortAccounts('expiry')">Expiry${arrow('expiry')}</th>
      <th style="width:80px">Actions</th>
    </tr></thead>
    <tbody>`;

  for (const acc of list) {
    const isOnline = acc.online && acc.enabled;
    const connCount = acc.activeConnections || 0;
    const speed = acc.speed || '';
    const isExpanded = expandedAccId === acc.id;
    const portDisplay = acc.portDisplay || String(acc.port || '--');
    const listenHost = escapeHtml(acc.listen || '0.0.0.0');
    const listenDisplay = Array.isArray(acc.ports) && acc.ports.length > 1
      ? acc.ports.map(port => `${listenHost}:${port}`).join('<br>')
      : `${listenHost}:${acc.port}`;

    let statusClass = acc.enabled ? 'green' : '';
    if (acc.disabledReason === 'bandwidth') statusClass = 'red';
    else if (acc.disabledReason === 'expired') statusClass = 'red';

    // Usage bar mini
    let usageBarMini = '';
    if (acc.bandwidthLimit > 0) {
      const pct = Math.min(100, ((acc.usedTraffic || 0) / acc.bandwidthLimit * 100));
      const barColor = pct > 90 ? '#ef4444' : pct > 70 ? '#f59e0b' : '#22c55e';
      usageBarMini = `<span class="usage-bar-mini"><span class="usage-bar-mini-fill" style="width:${pct}%;background:${barColor}"></span></span>`;
    }

    html += `<tr class="${isExpanded ? 'expanded' : ''}" onclick="toggleAccDetail('${acc.id}')" style="cursor:pointer">
      <td><span class="dash-svc-dot ${isOnline ? 'green' : ''}" style="display:inline-block"></span></td>
      <td>
        <div class="online-cell">
          <strong>${escapeHtml(acc.name)}</strong>
          <span class="ob-tag proto" style="font-size:9px;padding:0 5px">${acc.protocol.toUpperCase()}</span>
          ${!acc.enabled ? `<span class="pill ${statusClass}" style="font-size:8px;padding:0 6px"><span class="pill-dot"></span>${acc.disabledReason || 'Off'}</span>` : ''}
        </div>
      </td>
      <td style="font-family:var(--mono);font-size:11px;color:var(--t2)">${escapeHtml(portDisplay)}</td>
      <td style="font-family:var(--mono);font-size:11px">${formatBytes(acc.usedTraffic || 0)}${usageBarMini}</td>
      <td style="font-family:var(--mono);font-size:11px">${isOnline ? connCount + (speed ? ' <span style="color:var(--t3);font-size:10px">' + speed + '</span>' : '') : '<span style="color:var(--t3)">-</span>'}</td>
      <td style="font-size:11px;color:var(--t2)">${fmtExpiry(acc.expiresAt)}</td>
      <td onclick="event.stopPropagation()">
        <div style="display:flex;gap:1px">
          <button class="btn-icon" onclick="copyLocalAccountLink('${acc.id}')" title="Copy link">${IC.copy}</button>
          <button class="btn-icon" onclick="showQrCode('${acc.id}')" title="QR Code">${IC.qr}</button>
        </div>
      </td>
    </tr>`;

    // Expandable detail row
    if (isExpanded) {
      const usageText = fmtUsage(acc.usedTraffic, acc.bandwidthLimit);
      const expiryText = fmtExpiry(acc.expiresAt);
      const maxConn = acc.maxConnections || 0;
      const activeIps = acc.activeIps || [];
      const blockedIps = new Set(acc.blockedIps || []);
      const ipsHtml = activeIps.length === 0
        ? (isOnline ? '<span style="color:var(--t3);font-size:11px">IPs not tracked (no ss/conntrack)</span>' : '')
        : activeIps.map(ip => {
            const isBlocked = blockedIps.has(ip);
            return `<span style="font-family:var(--mono);font-size:11px;${isBlocked ? 'color:#f87171' : ''}">${ip}${geoTag(ip)}${isBlocked ? ' <span style="color:#f87171;font-size:9px">blocked</span>' : ''}</span>`;
          }).join('');

      html += `<tr class="acc-detail-row"><td colspan="7">
        <div class="acc-detail-content">
          <div class="acc-detail-info">
            <div class="kv-row"><span class="kv-label">Listen</span><span class="kv-val">${listenDisplay}</span></div>
            <div class="kv-row"><span class="kv-label">Protocol</span><span class="kv-val">${acc.protocol.toUpperCase()}</span></div>
            <div class="kv-row"><span class="kv-label">Traffic</span><span class="kv-val">${usageText}</span></div>
            ${maxConn ? `<div class="kv-row"><span class="kv-label">Max Unique IPs</span><span class="kv-val">${maxConn}</span></div>` : ''}
            <div class="kv-row"><span class="kv-label">Expiry</span><span class="kv-val">${expiryText}</span></div>
            <div class="kv-row"><span class="kv-label">UUID</span><span class="kv-val" style="font-size:10px">${acc.uuid || '--'}</span></div>
            ${isOnline ? `<div class="kv-row"><span class="kv-label">Speed</span><span class="kv-val">${speed || '--'}</span></div>` : ''}
            <div class="kv-row"><span class="kv-label">Assigned Tunnel</span><span class="kv-val">${escapeHtml(acc.effectiveTunnelLabel || 'Legacy default')}</span></div>
            ${acc.assignedXrayConfigId ? `<div class="kv-row"><span class="kv-label">Tunnel State</span><span class="kv-val">${acc.assignedXrayRunning ? '<span style="color:#22c55e">Running</span>' : '<span style="color:#f87171">Stopped (fail-closed)</span>'}</span></div>` : (isOnline ? `<div class="kv-row"><span class="kv-label">Legacy Outbound</span><span class="kv-val">${localXrayOutbound === 'direct' ? 'Direct' : localXrayOutbound.startsWith('socks:') ? '<span style="color:#22c55e">SSH SOCKS</span>' : escapeHtml(localXrayOutbound)}</span></div>` : '')}
            ${ipsHtml ? `<div class="kv-row"><span class="kv-label">Connected IPs</span><span class="kv-val" style="display:flex;flex-direction:column;gap:2px">${ipsHtml}</span></div>` : ''}
            ${acc.uuid ? `<div class="kv-row"><span class="kv-label">Sub Link</span><span class="kv-val" style="font-size:10px"><a href="/sub/${acc.uuid}" target="_blank" style="color:var(--accent)">/sub/${acc.uuid.substring(0,8)}...</a></span></div>` : ''}
          </div>
          <div class="acc-detail-actions" onclick="event.stopPropagation()">
            ${acc.uuid ? `<button class="btn btn-sm" onclick="copySubLink('${acc.uuid}')" title="Copy subscription link">${IC.copy} Sub Link</button>` : ''}
            <button class="btn btn-sm" onclick="openEditLocalAccount('${acc.id}')" title="Edit">${IC.edit} Edit</button>
            <button class="btn btn-sm" onclick="toggleLocalAccount('${acc.id}', ${!acc.enabled})">${acc.enabled ? 'Disable' : 'Enable'}</button>
            <button class="btn btn-sm" onclick="resetLocalAccountTraffic('${acc.id}')" title="Reset traffic">${IC.reset} Reset</button>
            <button class="btn btn-sm" onclick="regenLocalAccountUuid('${acc.id}')" title="New UUID">${IC.key} Regen</button>
            <button class="btn btn-sm btn-red" onclick="deleteLocalAccount('${acc.id}')" title="Delete">${IC.trash} Delete</button>
          </div>
        </div>
      </td></tr>`;
    }
  }

  html += '</tbody></table>';
  el.innerHTML = html;

  // Update dashboard if visible
  if (currentPage === 'dashboard') updateDashboard();
}

function copyToClipboard(text) {
  if (navigator.clipboard && window.isSecureContext) {
    navigator.clipboard.writeText(text).then(() => showToast('Copied!')).catch(() => fallbackCopy(text));
  } else {
    fallbackCopy(text);
  }
}

function fallbackCopy(text) {
  const ta = document.createElement('textarea');
  ta.value = text;
  ta.style.cssText = 'position:fixed;left:-9999px;top:-9999px';
  document.body.appendChild(ta);
  ta.select();
  try { document.execCommand('copy'); showToast('Copied!'); }
  catch { showToast('Copy failed'); }
  document.body.removeChild(ta);
}

function showToast(msg) {
  let t = document.getElementById('toast-msg');
  if (!t) {
    t = document.createElement('div');
    t.id = 'toast-msg';
    t.style.cssText = 'position:fixed;bottom:24px;right:24px;background:var(--bg-3);color:var(--t1);padding:10px 20px;border-radius:10px;font-size:13px;z-index:9999;opacity:0;transition:opacity .2s;backdrop-filter:blur(8px);border:1px solid var(--border)';
    document.body.appendChild(t);
  }
  t.textContent = msg;
  t.style.opacity = '1';
  clearTimeout(t._timer);
  t._timer = setTimeout(() => { t.style.opacity = '0'; }, 1500);
}

function copyText(text) {
  copyToClipboard(text);
}

function copyLocalAccountLink(id) {
  const acc = localAccountsList.find(a => a.id === id);
  if (acc && acc.link) copyToClipboard(acc.link);
}

function copySubLink(uuid) {
  const url = `${location.origin}/sub/${uuid}`;
  copyToClipboard(url);
  showToast('Subscription link copied!');
}

function showQrCode(id) {
  const acc = localAccountsList.find(a => a.id === id);
  if (!acc || !acc.link) { showToast('No link to generate QR'); return; }
  const text = acc.link;
  const qr = qrcode(0, 'M');
  qr.addData(text);
  qr.make();

  const canvas = $('qr-canvas');
  const size = 280;
  const cellSize = Math.floor(size / qr.getModuleCount());
  const actualSize = cellSize * qr.getModuleCount();
  canvas.width = actualSize;
  canvas.height = actualSize;
  const ctx = canvas.getContext('2d');
  ctx.fillStyle = '#ffffff';
  ctx.fillRect(0, 0, actualSize, actualSize);
  ctx.fillStyle = '#000000';
  for (let r = 0; r < qr.getModuleCount(); r++) {
    for (let c = 0; c < qr.getModuleCount(); c++) {
      if (qr.isDark(r, c)) {
        ctx.fillRect(c * cellSize, r * cellSize, cellSize, cellSize);
      }
    }
  }
  $('qr-modal-title').textContent = acc.name || 'QR Code';
  $('qr-modal').classList.remove('hidden');
}

function downloadQr() {
  const canvas = $('qr-canvas');
  const a = document.createElement('a');
  a.download = 'config-qr.png';
  a.href = canvas.toDataURL('image/png');
  a.click();
}

function parseBwToBytes(value, unit) {
  const v = parseFloat(value) || 0;
  if (unit === 'tb') return Math.round(v * 1024 * 1024 * 1024 * 1024);
  if (unit === 'gb') return Math.round(v * 1024 * 1024 * 1024);
  if (unit === 'mb') return Math.round(v * 1024 * 1024);
  return 0;
}

function bytesToUnit(bytes) {
  if (!bytes) return { value: 0, unit: 'gb' };
  const tb = bytes / (1024 * 1024 * 1024 * 1024);
  if (tb >= 1) return { value: +tb.toFixed(1), unit: 'tb' };
  const gb = bytes / (1024 * 1024 * 1024);
  if (gb >= 1) return { value: +gb.toFixed(1), unit: 'gb' };
  const mb = bytes / (1024 * 1024);
  return { value: +mb.toFixed(0), unit: 'mb' };
}

function expToMs(value, unit) {
  const v = parseFloat(value) || 0;
  if (unit === 'months') return Math.round(v * 30 * 24 * 60 * 60 * 1000);
  if (unit === 'days') return Math.round(v * 24 * 60 * 60 * 1000);
  if (unit === 'hours') return Math.round(v * 60 * 60 * 1000);
  return 0;
}

async function openAddLocalAccount() {
  await loadXrayLocalConfigs();
  $('local-acc-edit-id').value = '';
  $('local-acc-name').value = '';
  $('local-acc-protocol').value = 'vless';
  $('local-acc-listen').value = '0.0.0.0';
  try {
    const r = await api('/local-accounts/random-port');
    $('local-acc-port').value = r.port;
  } catch { $('local-acc-port').value = '10000'; }
  $('local-acc-bw-value').value = '0';
  $('local-acc-bw-unit').value = 'gb';
  $('local-acc-max-conn').value = '0';
  populateAssignedXraySelect('');
  $('local-acc-assigned-xray').value = '';
  $('local-acc-exp-value').value = '0';
  $('local-acc-exp-unit').value = 'days';
  $('local-account-modal-title').textContent = 'Add Account';
  $('local-account-modal').classList.remove('hidden');
}

async function openEditLocalAccount(id) {
  await loadXrayLocalConfigs();
  const acc = localAccountsList.find(a => a.id === id);
  if (!acc) return;
  $('local-acc-edit-id').value = acc.id;
  $('local-acc-name').value = acc.name;
  $('local-acc-protocol').value = acc.protocol;
  $('local-acc-port').value = acc.port;
  $('local-acc-listen').value = acc.listen || '0.0.0.0';

  const bw = bytesToUnit(acc.bandwidthLimit || 0);
  $('local-acc-bw-value').value = bw.value;
  $('local-acc-bw-unit').value = bw.unit;
  $('local-acc-max-conn').value = acc.maxConnections || 0;
  populateAssignedXraySelect(acc.assignedXrayConfigId || '');
  $('local-acc-assigned-xray').value = acc.assignedXrayConfigId || '';

  // Calculate remaining expiry as duration from now
  if (acc.expiresAt && acc.expiresAt > Date.now()) {
    const remaining = acc.expiresAt - Date.now();
    const days = remaining / (24 * 60 * 60 * 1000);
    if (days >= 30) {
      $('local-acc-exp-value').value = Math.round(days / 30);
      $('local-acc-exp-unit').value = 'months';
    } else if (days >= 1) {
      $('local-acc-exp-value').value = Math.round(days);
      $('local-acc-exp-unit').value = 'days';
    } else {
      $('local-acc-exp-value').value = Math.round(remaining / (60 * 60 * 1000));
      $('local-acc-exp-unit').value = 'hours';
    }
  } else {
    $('local-acc-exp-value').value = '0';
    $('local-acc-exp-unit').value = 'days';
  }

  $('local-account-modal-title').textContent = 'Edit Account';
  $('local-account-modal').classList.remove('hidden');
}

async function submitLocalAccount() {
  const editId = $('local-acc-edit-id').value;
  const name = $('local-acc-name').value.trim();
  const protocol = $('local-acc-protocol').value;
  const port = parseInt($('local-acc-port').value);
  const listen = $('local-acc-listen').value;
  if (!name) { alert('Name required'); return; }

  const bandwidthLimit = parseBwToBytes($('local-acc-bw-value').value, $('local-acc-bw-unit').value);
  const maxConnections = parseInt($('local-acc-max-conn').value) || 0;
  const assignedXrayConfigId = $('local-acc-assigned-xray').value || null;
  const expVal = parseFloat($('local-acc-exp-value').value) || 0;
  const expUnit = $('local-acc-exp-unit').value;
  const expiresAt = expVal > 0 ? Date.now() + expToMs(expVal, expUnit) : 0;

  try {
    if (editId) {
      await api(`/local-accounts/${editId}`, { method: 'PUT', body: { name, port, listen, bandwidthLimit, maxConnections, expiresAt, assignedXrayConfigId } });
    } else {
      await api('/local-accounts', { method: 'POST', body: { name, protocol, port, listen, bandwidthLimit, maxConnections, expiresAt, assignedXrayConfigId } });
    }
    $('local-account-modal').classList.add('hidden');
    await loadLocalAccounts();
  } catch (e) {
    alert('Error: ' + e.message);
  }
}

async function deleteLocalAccount(id) {
  if (!confirm('Delete this account?')) return;
  await api(`/local-accounts/${id}`, { method: 'DELETE' });
  await loadLocalAccounts();
}

async function toggleLocalAccount(id, enabled) {
  await api(`/local-accounts/${id}`, { method: 'PUT', body: { enabled } });
  await loadLocalAccounts();
}

async function resetLocalAccountTraffic(id) {
  if (!confirm('Reset traffic counter for this account?')) return;
  await api(`/local-accounts/reset-traffic/${id}`, { method: 'POST' });
  await loadLocalAccounts();
}

async function regenLocalAccountUuid(id) {
  if (!confirm('Regenerate UUID? The old link will stop working.')) return;
  await api(`/local-accounts/regenerate-uuid/${id}`, { method: 'POST' });
  await loadLocalAccounts();
}

async function refreshLocalXrayStatus() {
  try {
    const s = await api('/local-xray/status');
    const pill = $('local-xray-pill');
    const pillText = $('local-xray-pill-text');
    const pagePill = $('xui-pill');
    const pagePillText = $('xui-pill-text');
    const ob = s.outbound || 'direct';
    localXrayOutbound = ob;
    const obLabel = ob === 'direct' ? 'Direct' : ob.startsWith('socks:') ? `SSH SOCKS (${ob.split(':').slice(1).join(':')})` : ob;
    if (s.running) {
      pill.className = 'pill green';
      pillText.textContent = `Running (${s.accounts} accs · ${obLabel})`;
      $('local-xray-start-btn').classList.add('hidden');
      $('local-xray-stop-btn').classList.remove('hidden');
      $('local-xray-logs-section').classList.remove('hidden');
      refreshLocalXrayLogs();
      if (v2rayCurrentTab === 'local') { pagePill.className = 'pill green'; pagePillText.textContent = `${s.accounts} accs · ${obLabel}`; }
    } else {
      pill.className = 'pill';
      pillText.textContent = 'Stopped';
      $('local-xray-start-btn').classList.remove('hidden');
      $('local-xray-stop-btn').classList.add('hidden');
      $('local-xray-logs-section').classList.add('hidden');
      if (v2rayCurrentTab === 'local') { pagePill.className = 'pill'; pagePillText.textContent = 'Stopped'; }
    }
    if (!s.binary) { pillText.textContent = 'xray not installed'; if (v2rayCurrentTab === 'local') pagePillText.textContent = 'xray not installed'; }
    // Update dashboard xray status
    const xrayDot = $('dash-svc-xray-dot');
    const xrayStatus = $('dash-svc-xray-status');
    if (xrayDot) xrayDot.className = 'dash-svc-dot ' + (s.running ? 'green' : '');
    if (xrayStatus) xrayStatus.textContent = s.running ? `Running (${s.accounts} accs)` : (s.binary ? 'Stopped' : 'Not installed');
  } catch {}
}

async function startLocalXray() {
  const btn = $('local-xray-start-btn');
  if (btn) { btn.disabled = true; btn.textContent = 'Starting...'; }
  try {
    const res = await api('/local-xray/start', { method: 'POST' });
    if (!res.ok) { alert('Failed: ' + (res.error || 'Unknown')); }
  } catch (e) { alert('Error: ' + e.message); }
  finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Start'; }
    refreshLocalXrayStatus();
  }
}

async function stopLocalXray() {
  const btn = $('local-xray-stop-btn');
  if (btn) { btn.disabled = true; btn.textContent = 'Stopping...'; }
  try { await api('/local-xray/stop', { method: 'POST' }); } catch {}
  finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Stop'; }
    refreshLocalXrayStatus();
  }
}

async function restartLocalXray() {
  try {
    const res = await api('/local-xray/restart', { method: 'POST' });
    if (!res.ok) { alert('Failed: ' + (res.error || 'Unknown')); return; }
    refreshLocalXrayStatus();
  } catch (e) { alert('Error: ' + e.message); }
}

async function refreshLocalXrayLogs() {
  try {
    const logs = await api('/local-xray/logs');
    const el = $('local-xray-logs-output');
    if (el && Array.isArray(logs)) {
      el.textContent = logs.map(l => l.m || '').join('\n');
      el.scrollTop = el.scrollHeight;
    }
  } catch {}
}

// ─── 3X-UI / V2Ray (Remote) ────────────────────────────
let xuiLoaded = false;
let xuiInbounds = [];
let xuiOnlines = [];

async function loadXuiConfig() {
  try {
    const data = await api('/xui/config');
    if (data.url) $('xui-url').value = data.url;
    if (data.username) $('xui-username').value = data.username;
    $('xui-config-status').textContent = data.configured ? 'Configured' : 'Not configured';
    return data.configured;
  } catch { return false; }
}

async function saveXuiConfig() {
  const url = $('xui-url').value.trim();
  const username = $('xui-username').value.trim();
  const password = $('xui-password').value;
  await api('/xui/config', { method: 'POST', body: { url, username, password } });
  $('xui-config-status').textContent = 'Saved';
  $('xui-password').value = '';
  xuiLoaded = false;
}

async function testXuiConnection() {
  const status = $('xui-config-status');
  status.textContent = 'Testing...';
  try {
    const res = await api('/xui/test', { method: 'POST' });
    status.textContent = res.ok ? 'Connected successfully' : 'Failed: ' + (res.error || 'Unknown');
    status.style.color = res.ok ? 'var(--green)' : 'var(--red)';
    setTimeout(() => { status.style.color = ''; }, 3000);
  } catch (e) {
    status.textContent = 'Error: ' + e.message;
    status.style.color = 'var(--red)';
  }
}

async function loadXuiData() {
  const notConfigured = $('xui-not-configured');
  const skeleton = $('xui-skeleton');
  const inboundsEl = $('xui-inbounds');
  const pill = $('xui-pill');
  const pillText = $('xui-pill-text');

  // Check if configured
  try {
    const cfg = await api('/xui/config');
    if (!cfg.configured) {
      notConfigured.classList.remove('hidden');
      if (skeleton) skeleton.classList.add('hidden');
      $('xui-outbounds-section').classList.add('hidden');
      pill.className = 'pill';
      pillText.textContent = 'Not configured';
      return;
    }
  } catch { return; }

  notConfigured.classList.add('hidden');
  if (skeleton) skeleton.classList.remove('hidden');
  inboundsEl.innerHTML = '';

  try {
    const [inbRes, onlRes, srvRes] = await Promise.all([
      api('/xui/inbounds'),
      api('/xui/onlines', { method: 'POST' }),
      api('/xui/server-status'),
    ]);

    removeSkeleton('xui-skeleton');

    if (!inbRes.success) {
      pill.className = 'pill red';
      pillText.textContent = 'Error';
      inboundsEl.innerHTML = `<div class="empty"><p>${escapeHtml(inbRes.msg || 'Failed to connect')}</p></div>`;
      return;
    }

    xuiInbounds = inbRes.obj || [];
    xuiOnlines = (onlRes.success && onlRes.obj) ? onlRes.obj : [];
    xuiLoaded = true;

    pill.className = 'pill green';
    pillText.textContent = xuiInbounds.length + ' inbounds';

    // Server stats
    if (srvRes.success && srvRes.obj) {
      const srv = srvRes.obj;
      $('xui-server-stats').classList.remove('hidden');
      $('xui-srv-cpu').textContent = Math.round(srv.cpu || 0) + '%';
      $('xui-srv-mem').textContent = Math.round((srv.mem?.current || 0) / (srv.mem?.total || 1) * 100) + '%';
      const upSec = srv.uptime || 0;
      $('xui-srv-uptime').textContent = formatUptimeLong(upSec);
      $('xui-srv-xray').textContent = srv.xray?.state === 'running' ? 'Running' : 'Stopped';
    }

    renderXuiInbounds();
    if (!xrayFullSettings) loadOutbounds();
  } catch (e) {
    removeSkeleton('xui-skeleton');
    pill.className = 'pill red';
    pillText.textContent = 'Error';
    inboundsEl.innerHTML = `<div class="empty"><p>Connection failed: ${escapeHtml(e.message)}</p></div>`;
  }
}

function renderXuiInbounds() {
  const container = $('xui-inbounds');
  container.innerHTML = '';

  for (const inb of xuiInbounds) {
    const clients = parseXuiClients(inb);
    const proto = (inb.protocol || '').toUpperCase();
    const enableClass = inb.enable ? 'card-green' : '';
    const statusText = inb.enable ? 'Enabled' : 'Disabled';

    let clientsHtml = '';
    for (const client of clients) {
      const isOnline = xuiOnlines.includes(client.email);
      const up = client.up || 0;
      const down = client.down || 0;
      const totalLimit = client.totalGB ? formatBytes(client.totalGB * 1024 * 1024 * 1024) : 'Unlimited';
      const expiryText = client.expiryTime && client.expiryTime > 0
        ? new Date(client.expiryTime).toLocaleDateString()
        : 'Never';

      clientsHtml += `
        <div class="xui-client ${isOnline ? 'online' : ''}">
          <div class="xui-client-main">
            <div class="xui-client-info">
              <span class="xui-client-dot ${isOnline ? 'green' : ''}"></span>
              <span class="xui-client-email">${escapeHtml(client.email || 'unnamed')}</span>
              ${!client.enable ? '<span class="limit-badge" style="background:var(--red-dim);color:var(--red)">Disabled</span>' : ''}
            </div>
            <div class="xui-client-stats">
              <span>&darr; ${formatBytes(down)}</span>
              <span>&uarr; ${formatBytes(up)}</span>
              <span>Limit: ${totalLimit}</span>
              <span>Exp: ${expiryText}</span>
            </div>
          </div>
          <div class="xui-client-actions">
            <button class="btn-icon" onclick="openEditClient(${inb.id}, '${escapeHtml(client.id)}')" title="Edit">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
            </button>
            <button class="btn-icon ${client.enable ? 'danger' : ''}" onclick="toggleXuiClient(${inb.id}, '${escapeHtml(client.id)}', '${escapeHtml(client.email)}', ${!client.enable})" title="${client.enable ? 'Disable' : 'Enable'}">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M18.36 6.64a9 9 0 11-12.73 0"/><line x1="12" y1="2" x2="12" y2="12"/></svg>
            </button>
            <button class="btn-icon" onclick="resetXuiClientTraffic(${inb.id}, '${escapeHtml(client.email)}')" title="Reset traffic">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0114.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0020.49 15"/></svg>
            </button>
            <button class="btn-icon danger" onclick="deleteXuiClient(${inb.id}, '${escapeHtml(client.id)}', '${escapeHtml(client.email)}')" title="Delete">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/></svg>
            </button>
          </div>
        </div>`;
    }

    container.insertAdjacentHTML('beforeend', `
      <div class="card ${enableClass}" style="margin-bottom:12px">
        <div class="card-row">
          <div>
            <div class="card-title">${escapeHtml(inb.remark || 'Inbound #' + inb.id)}</div>
            <div class="card-meta">
              <span>${proto}</span> &middot; Port <span>:${inb.port}</span> &middot; ${statusText} &middot; ${clients.length} client${clients.length !== 1 ? 's' : ''}
            </div>
          </div>
          <div class="card-actions">
            <button class="btn btn-sm btn-primary" onclick="openAddClient(${inb.id}, '${inb.protocol}')">+ Client</button>
            <button class="btn btn-sm ${inb.enable ? 'btn-red' : 'btn-green'}" onclick="toggleXuiInbound(${inb.id}, ${!inb.enable})">
              ${inb.enable ? 'Disable' : 'Enable'}
            </button>
          </div>
        </div>
        ${clients.length > 0 ? '<div class="xui-clients">' + clientsHtml + '</div>' : '<div class="card-foot">No clients</div>'}
      </div>
    `);
  }

  if (xuiInbounds.length === 0) {
    container.innerHTML = '<div class="empty"><p>No inbounds found</p></div>';
  }

  // Show xray settings section when configured
  $('xui-outbounds-section').classList.remove('hidden');
}

function parseXuiClients(inbound) {
  const clients = [];
  try {
    const settings = JSON.parse(inbound.settings || '{}');
    const clientStats = inbound.clientStats || [];
    const rawClients = settings.clients || [];

    for (const c of rawClients) {
      const stats = clientStats.find(s => s.email === c.email) || {};
      clients.push({
        id: c.id || c.password || '',
        email: c.email || '',
        enable: c.enable !== false,
        up: stats.up || 0,
        down: stats.down || 0,
        totalGB: c.totalGB || 0,
        expiryTime: c.expiryTime || 0,
        limitIp: c.limitIp || 0,
        flow: c.flow || '',
        subId: c.subId || '',
        tgId: c.tgId || '',
        comment: c.comment || '',
        password: c.password || '',
      });
    }
  } catch {}
  return clients;
}

async function toggleXuiInbound(id, enable) {
  try {
    await api(`/xui/inbound/enable/${id}`, { method: 'POST', body: { enable } });
    xuiLoaded = false;
    loadXuiData();
  } catch (e) {
    alert('Failed: ' + e.message);
  }
}

async function toggleXuiClient(inboundId, clientId, email, enable) {
  try {
    const getRes = await api(`/xui/inbound/${inboundId}`);
    if (!getRes.success) { alert(getRes.msg); return; }
    const inbound = getRes.obj;
    const settings = JSON.parse(inbound.settings || '{}');
    const client = (settings.clients || []).find(c => (c.id || c.password || '') === clientId || c.email === email);
    if (!client) { alert('Client not found'); return; }
    client.enable = enable;
    await api(`/xui/client/update/${clientId}`, { method: 'POST', body: { id: inboundId, settings: JSON.stringify({ clients: [client] }) } });
    xuiLoaded = false;
    loadXuiData();
  } catch (e) {
    alert('Failed: ' + e.message);
  }
}

async function resetXuiClientTraffic(inboundId, email) {
  if (!confirm(`Reset traffic for ${email}?`)) return;
  try {
    await api(`/xui/client/reset-traffic/${inboundId}/${email}`, { method: 'POST' });
    xuiLoaded = false;
    loadXuiData();
  } catch (e) {
    alert('Failed: ' + e.message);
  }
}

async function deleteXuiClient(inboundId, clientId, email) {
  if (!confirm(`Delete client "${email}"? This cannot be undone.`)) return;
  try {
    await api(`/xui/client/delete/${inboundId}/${clientId}`, { method: 'POST' });
    xuiLoaded = false;
    loadXuiData();
  } catch (e) {
    alert('Failed: ' + e.message);
  }
}

// ─── Client Add/Edit Modal ──────────────────────────
function openAddClient(inboundId, protocol) {
  $('client-modal-title').textContent = 'Add Client';
  $('client-save-btn').textContent = 'Add Client';
  $('client-edit-inbound-id').value = inboundId;
  $('client-edit-id').value = '';
  $('client-edit-protocol').value = protocol;
  $('client-email').value = '';
  $('client-uuid').value = crypto.randomUUID();
  $('client-pw').value = '';
  $('client-total-gb').value = '0';
  $('client-limit-ip').value = '0';
  $('client-expiry').value = '';
  $('client-flow').value = '';
  $('client-enable').checked = true;

  // Show/hide fields based on protocol
  const isVmessVless = ['vmess', 'vless'].includes(protocol);
  const isTrojan = protocol === 'trojan';
  $('client-uuid-field').classList.toggle('hidden', !isVmessVless);
  $('client-password-field').classList.toggle('hidden', !isTrojan);
  if (isTrojan) $('client-pw').value = crypto.randomUUID();
  $('client-flow-field').style.display = protocol === 'vless' ? '' : 'none';

  $('client-modal').classList.remove('hidden');
}

function openEditClient(inboundId, clientId) {
  const inb = xuiInbounds.find(i => i.id === inboundId);
  if (!inb) return;
  const clients = parseXuiClients(inb);
  const client = clients.find(c => c.id === clientId);
  if (!client) return;

  const protocol = inb.protocol || '';
  $('client-modal-title').textContent = 'Edit Client';
  $('client-save-btn').textContent = 'Save Changes';
  $('client-edit-inbound-id').value = inboundId;
  $('client-edit-id').value = clientId;
  $('client-edit-protocol').value = protocol;
  $('client-email').value = client.email;
  $('client-uuid').value = client.id;
  $('client-pw').value = client.password;
  $('client-total-gb').value = client.totalGB || 0;
  $('client-limit-ip').value = client.limitIp || 0;
  $('client-enable').checked = client.enable;
  $('client-flow').value = client.flow || '';

  if (client.expiryTime && client.expiryTime > 0) {
    const d = new Date(client.expiryTime);
    $('client-expiry').value = d.toISOString().split('T')[0];
  } else {
    $('client-expiry').value = '';
  }

  const isVmessVless = ['vmess', 'vless'].includes(protocol);
  const isTrojan = protocol === 'trojan';
  $('client-uuid-field').classList.toggle('hidden', !isVmessVless);
  $('client-password-field').classList.toggle('hidden', !isTrojan);
  $('client-flow-field').style.display = protocol === 'vless' ? '' : 'none';

  $('client-modal').classList.remove('hidden');
}

function closeClientModal() {
  $('client-modal').classList.add('hidden');
}

async function submitClient(e) {
  e.preventDefault();
  const inboundId = parseInt($('client-edit-inbound-id').value);
  const editId = $('client-edit-id').value;
  const protocol = $('client-edit-protocol').value;
  const isEdit = !!editId;

  const email = $('client-email').value.trim();
  const totalGB = parseInt($('client-total-gb').value) || 0;
  const limitIp = parseInt($('client-limit-ip').value) || 0;
  const enable = $('client-enable').checked;
  const flow = $('client-flow').value;
  const expiryStr = $('client-expiry').value;
  const expiryTime = expiryStr ? new Date(expiryStr).getTime() : 0;

  // Build client object matching the protocol
  const clientObj = {
    email,
    enable,
    expiryTime,
    limitIp,
    totalGB,
    flow: protocol === 'vless' ? flow : '',
    tgId: '',
    subId: isEdit ? '' : generateSubId(),
    comment: '',
    reset: 0,
  };

  if (protocol === 'trojan') {
    clientObj.password = $('client-pw').value.trim() || crypto.randomUUID();
  } else {
    clientObj.id = $('client-uuid').value.trim() || crypto.randomUUID();
  }

  // For vmess, keep security field
  if (protocol === 'vmess') {
    clientObj.security = 'auto';
  }

  const settingsPayload = JSON.stringify({ clients: [clientObj] });

  try {
    let res;
    if (isEdit) {
      res = await api(`/xui/client/update/${editId}`, { method: 'POST', body: { id: inboundId, settings: settingsPayload } });
    } else {
      res = await api('/xui/client/add', { method: 'POST', body: { id: inboundId, settings: settingsPayload } });
    }
    if (res.success === false) {
      alert('Failed: ' + (res.msg || 'Unknown error'));
      return;
    }
    closeClientModal();
    xuiLoaded = false;
    loadXuiData();
  } catch (e) {
    alert('Error: ' + e.message);
  }
}

function generateSubId() {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let result = '';
  for (let i = 0; i < 16; i++) result += chars[Math.floor(Math.random() * chars.length)];
  return result;
}

// ─── Outbounds Management ───────────────────────────
let xrayFullSettings = null;

async function loadOutbounds() {
  try {
    const res = await api('/xui/xray-settings');
    if (!res.success) return;
    const parsed = JSON.parse(res.obj);
    xrayFullSettings = parsed.xraySetting;
    renderOutbounds(xrayFullSettings.outbounds || []);
  } catch (e) {
    $('xui-outbounds-list').innerHTML = `<div class="empty"><p>Failed to load: ${escapeHtml(e.message)}</p></div>`;
  }
}

function findOutboundAddress(ob) {
  const p = ob.protocol;
  if (p === 'vless' || p === 'dns') {
    return ob.settings?.address ? [ob.settings.address + ':' + (ob.settings.port || '')] : [];
  }
  const arr = ob.settings?.vnext || ob.settings?.servers || [];
  return arr.map(s => (s.address || '') + ':' + (s.port || ''));
}

function renderOutbounds(outbounds) {
  const container = $('xui-outbounds-list');
  if (!outbounds.length) {
    container.innerHTML = '<div class="empty"><p>No outbounds</p></div>';
    return;
  }
  let html = '';
  outbounds.forEach((ob, i) => {
    const addrs = findOutboundAddress(ob);
    const addrText = addrs.length ? addrs.join(', ') : '--';
    const net = ob.streamSettings?.network || '';
    const sec = ob.streamSettings?.security || '';
    const tags = [];
    tags.push(`<span class="ob-tag proto">${escapeHtml(ob.protocol)}</span>`);
    if (net) tags.push(`<span class="ob-tag net">${escapeHtml(net)}</span>`);
    if (sec && sec !== 'none') tags.push(`<span class="ob-tag sec">${escapeHtml(sec)}</span>`);

    html += `
      <div class="card ob-card" style="margin-bottom:8px">
        <div class="card-row">
          <div style="min-width:0;flex:1">
            <div class="card-title" style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
              <span class="ob-index">${i + 1}</span>
              <span>${escapeHtml(ob.tag || 'untagged')}</span>
              ${tags.join('')}
            </div>
            <div class="card-meta" style="margin-top:4px">${escapeHtml(addrText)}</div>
          </div>
          <div class="card-actions" style="gap:4px">
            ${i > 0 ? `<button class="btn-icon" onclick="moveOutbound(${i}, 0)" title="Move to first"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><polyline points="17 11 12 6 7 11"/><polyline points="17 18 12 13 7 18"/></svg></button>` : ''}
            <button class="btn-icon" onclick="openEditOutbound(${i})" title="Edit"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/></svg></button>
            <button class="btn-icon danger" onclick="deleteOutbound(${i})" title="Delete"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/></svg></button>
          </div>
        </div>
      </div>`;
  });
  container.innerHTML = html;
}

function openAddOutbound() {
  $('outbound-modal-title').textContent = 'Add Outbound';
  $('outbound-save-btn').textContent = 'Add Outbound';
  $('outbound-edit-index').value = '-1';
  $('outbound-tag').value = '';
  $('outbound-protocol').value = 'freedom';
  $('outbound-address').value = '';
  $('outbound-port').value = '';
  $('outbound-id').value = '';
  $('outbound-password').value = '';
  $('outbound-network').value = 'tcp';
  $('outbound-security').value = 'none';
  $('outbound-json').value = '';
  onOutboundProtocolChange();
  $('outbound-modal').classList.remove('hidden');
}

function openEditOutbound(index) {
  if (!xrayFullSettings) return;
  const ob = xrayFullSettings.outbounds[index];
  if (!ob) return;

  $('outbound-modal-title').textContent = 'Edit Outbound #' + (index + 1);
  $('outbound-save-btn').textContent = 'Save Changes';
  $('outbound-edit-index').value = index;
  $('outbound-tag').value = ob.tag || '';
  $('outbound-protocol').value = ob.protocol || 'freedom';
  onOutboundProtocolChange();

  const addrs = findOutboundAddress(ob);
  if (addrs.length) {
    const last = addrs[0].lastIndexOf(':');
    $('outbound-address').value = last > 0 ? addrs[0].substring(0, last) : addrs[0];
    $('outbound-port').value = last > 0 ? addrs[0].substring(last + 1) : '';
  } else {
    $('outbound-address').value = '';
    $('outbound-port').value = '';
  }

  const p = ob.protocol;
  if (p === 'vless') {
    $('outbound-id').value = ob.settings?.id || '';
  } else if (p === 'vmess') {
    $('outbound-id').value = ob.settings?.vnext?.[0]?.users?.[0]?.id || '';
  } else if (p === 'trojan' || p === 'shadowsocks') {
    $('outbound-password').value = ob.settings?.servers?.[0]?.password || '';
  } else if (p === 'socks' || p === 'http') {
    $('outbound-id').value = ob.settings?.servers?.[0]?.users?.[0]?.user || '';
    $('outbound-password').value = ob.settings?.servers?.[0]?.users?.[0]?.pass || '';
  }

  $('outbound-network').value = ob.streamSettings?.network || 'tcp';
  $('outbound-security').value = ob.streamSettings?.security || 'none';
  $('outbound-json').value = JSON.stringify(ob, null, 2);

  $('outbound-modal').classList.remove('hidden');
}

function closeOutboundModal() {
  $('outbound-modal').classList.add('hidden');
}

function onOutboundProtocolChange() {
  const proto = $('outbound-protocol').value;
  const proxyProtos = ['vmess', 'vless', 'trojan', 'shadowsocks', 'socks', 'http', 'wireguard', 'dns'];
  $('outbound-proxy-fields').style.display = proxyProtos.includes(proto) ? '' : 'none';
  const hasUuid = ['vmess', 'vless'].includes(proto);
  const hasUser = ['socks', 'http'].includes(proto);
  const hasPw = ['trojan', 'shadowsocks', 'socks', 'http'].includes(proto);
  $('outbound-id-field').classList.toggle('hidden', !hasUuid && !hasUser);
  $('outbound-pw-field').classList.toggle('hidden', !hasPw);
  $('outbound-id-field').querySelector('label').textContent = hasUser ? 'Username' : 'UUID / ID';
}

function buildOutboundFromFields() {
  const tag = $('outbound-tag').value.trim();
  const protocol = $('outbound-protocol').value;
  const address = $('outbound-address').value.trim();
  const port = parseInt($('outbound-port').value) || 443;
  const uuid = $('outbound-id').value.trim();
  const password = $('outbound-password').value.trim();
  const network = $('outbound-network').value;
  const security = $('outbound-security').value;

  const ob = { protocol, tag };

  if (protocol === 'freedom') {
    ob.settings = { domainStrategy: 'AsIs' };
  } else if (protocol === 'blackhole') {
    ob.settings = {};
  } else if (protocol === 'vless') {
    ob.settings = { address, port, id: uuid, flow: '', encryption: 'none' };
    ob.streamSettings = { network, security };
  } else if (protocol === 'vmess') {
    ob.settings = { vnext: [{ address, port, users: [{ id: uuid, security: 'auto' }] }] };
    ob.streamSettings = { network, security };
  } else if (protocol === 'trojan') {
    ob.settings = { servers: [{ address, port, password }] };
    ob.streamSettings = { network, security };
  } else if (protocol === 'shadowsocks') {
    ob.settings = { servers: [{ address, port, password, method: 'aes-256-gcm' }] };
    ob.streamSettings = { network, security };
  } else if (protocol === 'socks' || protocol === 'http') {
    const srv = { address, port };
    if (uuid || password) srv.users = [{ user: uuid, pass: password }];
    ob.settings = { servers: [srv] };
  } else if (protocol === 'dns') {
    ob.settings = { address, port };
  } else if (protocol === 'wireguard') {
    ob.settings = { address, port };
  }

  return ob;
}

async function submitOutbound(e) {
  e.preventDefault();
  if (!xrayFullSettings) { alert('Settings not loaded'); return; }

  const index = parseInt($('outbound-edit-index').value);
  const jsonText = $('outbound-json').value.trim();
  let outbound;

  if (jsonText) {
    try { outbound = JSON.parse(jsonText); }
    catch (err) { alert('Invalid JSON: ' + err.message); return; }
  } else {
    outbound = buildOutboundFromFields();
  }
  if (!outbound.tag) { alert('Tag is required'); return; }

  if (index >= 0) {
    xrayFullSettings.outbounds[index] = outbound;
  } else {
    xrayFullSettings.outbounds.push(outbound);
  }

  closeOutboundModal();
  renderOutbounds(xrayFullSettings.outbounds);
}

function deleteOutbound(index) {
  if (!xrayFullSettings) return;
  const ob = xrayFullSettings.outbounds[index];
  if (!confirm(`Delete outbound "${ob?.tag || index}"?`)) return;
  xrayFullSettings.outbounds.splice(index, 1);
  renderOutbounds(xrayFullSettings.outbounds);
}

function moveOutbound(from, to) {
  if (!xrayFullSettings) return;
  const arr = xrayFullSettings.outbounds;
  arr.splice(to, 0, arr.splice(from, 1)[0]);
  renderOutbounds(arr);
}

async function saveAndRestartXray() {
  if (!xrayFullSettings) { alert('Settings not loaded'); return; }
  if (!confirm('Save outbounds and restart Xray?')) return;
  try {
    const res = await api('/xui/xray-settings', {
      method: 'POST',
      body: { xraySetting: JSON.stringify(xrayFullSettings, null, 2) }
    });
    if (!res.success) { alert('Save failed: ' + (res.msg || 'Unknown')); return; }
    const r2 = await api('/xui/restart-xray', { method: 'POST' });
    if (r2.success) {
      alert('Saved and Xray restarted');
    } else {
      alert('Saved but restart failed: ' + (r2.msg || 'Unknown'));
    }
  } catch (e) {
    alert('Error: ' + e.message);
  }
}

// ─── Xray Local ─────────────────────────────────────
let xrayLocalConfigs = [];
let xrayLocalLoaded = false;
let xrayRunningConfigId = null;
let xrayStatusSnapshot = { running: false, runs: [] };
let xrayInterfaceOptions = ['127.0.0.1', '0.0.0.0'];
let scanWorkingResults = []; // { ip, latency }
let scanConfigId = null;

async function loadXrayLocalPage() {
  if (!xrayLocalLoaded) {
    await loadXrayInterfaces();
    xrayLocalLoaded = true;
  }
  await Promise.all([loadXrayLocalConfigs(), refreshXrayStatus(false)]);
}

async function loadXrayInterfaces() {
  try {
    const addrs = await api('/xray-local/interfaces');
    xrayInterfaceOptions = Array.isArray(addrs) && addrs.length ? addrs : xrayInterfaceOptions;
    renderXrayConfigs();
  } catch {}
}

async function loadXrayLocalConfigs() {
  try {
    xrayLocalConfigs = await api('/xray-local/configs');
    renderXrayConfigs();
    populateScanSelect();
    populateAssignedXraySelect();
  } catch {}
}

function getXrayRouteModeLabel(mode) {
  return {
    direct: 'Direct',
    openconnect: 'OpenConnect',
    openvpn: 'OpenVPN',
  }[mode] || 'Direct';
}

function getXrayBindingInputs(id) {
  return {
    listenAddr: $(`xray-bind-listen-${id}`)?.value || '127.0.0.1',
    socksPort: parseInt($(`xray-bind-socks-${id}`)?.value, 10) || 10808,
    httpPort: parseInt($(`xray-bind-http-${id}`)?.value, 10) || 0,
  };
}

function renderXrayListenOptions(selectedValue) {
  const values = xrayInterfaceOptions.includes(selectedValue)
    ? xrayInterfaceOptions
    : [...xrayInterfaceOptions, selectedValue].filter(Boolean);
  return values.map(addr =>
    `<option value="${escapeHtml(addr)}"${addr === selectedValue ? ' selected' : ''}>${escapeHtml(addr)}</option>`
  ).join('');
}

function getXrayConfigById(id) {
  return xrayLocalConfigs.find(cfg => cfg.id === id) || null;
}

function populateAssignedXraySelect(selectedValue) {
  const sel = $('local-acc-assigned-xray');
  if (!sel) return;
  const current = selectedValue !== undefined ? selectedValue : sel.value;
  sel.innerHTML = '<option value="">Legacy default</option>';
  for (const cfg of xrayLocalConfigs) {
    const bindings = cfg.localBindings || {};
    const state = cfg.runState && cfg.runState.running ? 'running' : 'stopped';
    sel.insertAdjacentHTML('beforeend',
      `<option value="${cfg.id}">${escapeHtml(cfg.remark)} (${bindings.socksPort || '--'} / ${state})</option>`);
  }
  if (current && !xrayLocalConfigs.some(cfg => cfg.id === current)) {
    sel.insertAdjacentHTML('beforeend', `<option value="${current}">Missing tunnel (${escapeHtml(current.slice(0, 8))}...)</option>`);
  }
  sel.value = current || '';
}

function renderXrayConfigs() {
  const el = $('xray-configs-list');
  if (!xrayLocalConfigs.length) {
    el.innerHTML = '<div class="empty"><p>No configs added yet</p></div>';
    return;
  }
  let html = '';
  for (const cfg of xrayLocalConfigs) {
    const proto = (cfg.outbound?.protocol || '').toUpperCase();
    const addr = getOutboundAddr(cfg.outbound);
    const net = cfg.outbound?.streamSettings?.network || '';
    const sec = cfg.outbound?.streamSettings?.security || '';
    const bindings = cfg.localBindings || {};
    const isActive = !!(cfg.runState && cfg.runState.running);
    const tags = [];
    tags.push(`<span class="ob-tag proto">${escapeHtml(proto)}</span>`);
    if (net) tags.push(`<span class="ob-tag net">${escapeHtml(net)}</span>`);
    if (sec && sec !== 'none') tags.push(`<span class="ob-tag sec">${escapeHtml(sec)}</span>`);
    if (isActive) tags.push(`<span class="ob-tag" style="background:rgba(34,197,94,.15);color:#22c55e">&#9679; Active</span>`);

    html += `
      <div class="card xray-cfg-card${isActive ? ' xray-cfg-active' : ''}" id="xcfg-${cfg.id}">
        <div class="card-row">
          <div style="min-width:0;flex:1">
            <div class="card-title" style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
              <span>${escapeHtml(cfg.remark)}</span>
              ${tags.join('')}
              <span class="xray-test-badge" id="xtest-${cfg.id}"></span>
            </div>
            <div class="card-meta" style="margin-top:4px">${escapeHtml(addr)}</div>
            <div class="field-row" style="margin-top:10px;gap:8px;flex-wrap:wrap;align-items:flex-end">
              <div class="field" style="flex:0 0 150px">
                <label>Listen</label>
                <select id="xray-bind-listen-${cfg.id}" class="input" ${isActive ? 'disabled' : ''}>
                  ${renderXrayListenOptions(bindings.listenAddr || '127.0.0.1')}
                </select>
              </div>
              <div class="field" style="flex:0 0 120px">
                <label>SOCKS Port</label>
                <input type="number" id="xray-bind-socks-${cfg.id}" class="input" min="1" max="65535" value="${bindings.socksPort || 10808}" ${isActive ? 'disabled' : ''} />
              </div>
              <div class="field" style="flex:0 0 120px">
                <label>HTTP Port</label>
                <input type="number" id="xray-bind-http-${cfg.id}" class="input" min="0" max="65535" value="${bindings.httpPort ?? 0}" ${isActive ? 'disabled' : ''} />
              </div>
            </div>
            <div class="card-meta" style="margin-top:8px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
              <span>Route via</span>
              <select class="input" style="width:160px" onchange="updateXrayConfigRouteMode('${cfg.id}', this.value)">
                <option value="direct"${(cfg.routeMode || 'direct') === 'direct' ? ' selected' : ''}>Direct</option>
                <option value="openconnect"${cfg.routeMode === 'openconnect' ? ' selected' : ''}>OpenConnect</option>
                <option value="openvpn"${cfg.routeMode === 'openvpn' ? ' selected' : ''}>OpenVPN</option>
              </select>
              <span>${isActive ? `Running on ${bindings.listenAddr}:${bindings.socksPort}${bindings.httpPort > 0 ? ` | HTTP ${bindings.httpPort}` : ' | HTTP off'}` : `Saved bindings: ${bindings.listenAddr}:${bindings.socksPort}${bindings.httpPort > 0 ? ` | HTTP ${bindings.httpPort}` : ' | HTTP off'}`}</span>
            </div>
          </div>
          <div class="card-actions" style="gap:4px">
            <button class="btn btn-sm" onclick="saveXrayBindings('${cfg.id}')" ${isActive ? 'disabled' : ''}>Save</button>
            <button class="btn btn-sm" onclick="editXrayOutbound('${cfg.id}')" title="Edit outbound config">${IC.edit}</button>
            <button class="btn btn-sm" onclick="testXrayConfig('${cfg.id}')" id="xtest-btn-${cfg.id}">Test</button>
            ${isActive
              ? `<button class="btn btn-sm btn-red" id="xcfg-stop-${cfg.id}" onclick="stopXrayLocal('${cfg.id}')">Stop</button>`
              : `<button class="btn btn-sm btn-green" id="xcfg-run-${cfg.id}" onclick="startXrayWithConfig('${cfg.id}')">Run</button>`
            }
            <button class="btn-icon danger" onclick="deleteXrayConfig('${cfg.id}')" title="Delete">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/></svg>
            </button>
          </div>
        </div>
      </div>`;
  }
  el.innerHTML = html;
}

function getOutboundAddr(ob) {
  if (!ob) return '--';
  const arr = ob.settings?.vnext || ob.settings?.servers || [];
  return arr.length ? arr[0].address + ':' + arr[0].port : '--';
}

function populateScanSelect() {
  const sel = $('scan-config-select');
  const current = sel.value;
  sel.innerHTML = '<option value="">Select a saved config...</option>';
  for (const cfg of xrayLocalConfigs) {
    sel.insertAdjacentHTML('beforeend', `<option value="${cfg.id}">${escapeHtml(cfg.remark)}</option>`);
  }
  if (current) sel.value = current;
}

async function addXrayConfig() {
  const input = $('xray-config-input');
  const link = input.value.trim();
  if (!link) return;
  try {
    const res = await api('/xray-local/configs', { method: 'POST', body: { link } });
    if (!res.ok) { alert('Failed: ' + (res.error || 'Parse error')); return; }
    input.value = '';
    await loadXrayLocalConfigs();
  } catch (e) {
    alert('Error: ' + e.message);
  }
}

async function deleteXrayConfig(id) {
  if (!confirm('Delete this config?')) return;
  await api(`/xray-local/configs/${id}`, { method: 'DELETE' });
  await loadXrayLocalConfigs();
  await refreshXrayStatus(false);
  if (localAccountsLoaded) await loadLocalAccounts();
}

async function editXrayOutbound(id) {
  try {
    const cfg = await api(`/xray-local/configs/${id}`);
    if (!cfg || !cfg.outbound) { showToast('Config not found', 'error'); return; }
    $('xray-edit-id').value = id;
    $('xray-edit-json').value = JSON.stringify(cfg.outbound, null, 2);
    $('xray-edit-status').textContent = cfg.remark || '';
    $('xray-edit-modal').classList.remove('hidden');
  } catch (e) {
    showToast('Failed to load config: ' + e.message, 'error');
  }
}

async function saveXrayOutbound() {
  const id = $('xray-edit-id').value;
  const jsonStr = $('xray-edit-json').value.trim();
  let outbound;
  try {
    outbound = JSON.parse(jsonStr);
  } catch (e) {
    $('xray-edit-status').textContent = 'Invalid JSON: ' + e.message;
    return;
  }
  try {
    const res = await api(`/xray-local/configs/${id}/outbound`, { method: 'PUT', body: { outbound } });
    if (res.ok) {
      showToast('Outbound config saved');
      $('xray-edit-modal').classList.add('hidden');
      await loadXrayLocalConfigs();
    } else {
      $('xray-edit-status').textContent = res.error || 'Save failed';
    }
  } catch (e) {
    $('xray-edit-status').textContent = 'Error: ' + e.message;
  }
}

function formatXrayEditJson() {
  const ta = $('xray-edit-json');
  try {
    const obj = JSON.parse(ta.value);
    ta.value = JSON.stringify(obj, null, 2);
  } catch (e) {
    $('xray-edit-status').textContent = 'Invalid JSON: ' + e.message;
  }
}

async function updateXrayConfigRouteMode(id, routeMode) {
  try {
    const res = await api(`/xray-local/configs/${id}/route-mode`, {
      method: 'PUT',
      body: { routeMode },
    });
    if (!res.ok) {
      showToast(res.error || 'Failed to update route mode', 'error');
      await loadXrayLocalConfigs();
      return;
    }
    const idx = xrayLocalConfigs.findIndex(cfg => cfg.id === id);
    if (idx !== -1) xrayLocalConfigs[idx] = res.config;
    renderXrayConfigs();

    const routeState = res.routeState || {};
    const cfg = getXrayConfigById(id);
    if (routeState.willApplyOnRun || (cfg && !(cfg.runState && cfg.runState.running))) {
      showToast(`${getXrayRouteModeLabel(routeMode)} saved. It will apply when this tunnel runs.`, 'success');
    } else if (routeState.pending) {
      showToast(`${getXrayRouteModeLabel(routeMode)} saved. Route will apply when the tunnel connects.`, 'success');
    } else if (routeState.device) {
      showToast(`${getXrayRouteModeLabel(routeMode)} route applied via ${routeState.device}`, 'success');
    } else {
      showToast('Route mode set to Direct', 'success');
    }
  } catch (e) {
    showToast('Failed to update route mode: ' + e.message, 'error');
    await loadXrayLocalConfigs();
  }
}

async function saveXrayBindings(id) {
  const cfg = getXrayConfigById(id);
  if (!cfg) return;
  const bindings = getXrayBindingInputs(id);
  try {
    const res = await api(`/xray-local/configs/${id}/local-bindings`, { method: 'PUT', body: bindings });
    if (!res.ok) {
      showToast(res.error || 'Failed to save bindings', 'error');
      await loadXrayLocalConfigs();
      return;
    }
    const idx = xrayLocalConfigs.findIndex(entry => entry.id === id);
    if (idx !== -1) xrayLocalConfigs[idx] = res.config;
    renderXrayConfigs();
    showToast('Bindings saved');
  } catch (e) {
    showToast('Failed to save bindings: ' + e.message, 'error');
  }
}

async function startXrayWithConfig(configId) {
  const bindings = getXrayBindingInputs(configId);
  const runBtn = $('xcfg-run-' + configId);
  if (runBtn) { runBtn.disabled = true; runBtn.textContent = 'Starting...'; }
  try {
    const res = await api('/xray-local/start', { method: 'POST', body: { configId, ...bindings } });
    if (!res.ok) { alert('Failed: ' + (res.error || 'Unknown')); return; }
    const routeState = res.routeState || {};
    const routeText = routeState.device
      ? `${getXrayRouteModeLabel(routeState.mode)} via ${routeState.device}`
      : getXrayRouteModeLabel(routeState.mode || 'direct');
    showToast(`Xray started: ${routeText}`, 'success');
    await refreshXrayStatus(true);
  } catch (e) {
    alert('Error: ' + e.message);
  } finally {
    if (runBtn) { runBtn.disabled = false; runBtn.textContent = 'Run'; }
  }
}

async function startXrayLocal() {
  if (!xrayLocalConfigs.length) { alert('No configs added'); return; }
  const firstStopped = xrayLocalConfigs.find(cfg => !(cfg.runState && cfg.runState.running)) || xrayLocalConfigs[0];
  if (firstStopped) startXrayWithConfig(firstStopped.id);
}

async function stopXrayLocal(configId = null) {
  const btn = configId ? $(`xcfg-stop-${configId}`) : $('xray-stop-all-btn');
  if (btn) { btn.disabled = true; btn.textContent = 'Stopping...'; }
  try { await api('/xray-local/stop', { method: 'POST', body: configId ? { configId } : {} }); } catch {}
  finally {
    if (btn) { btn.disabled = false; btn.textContent = configId ? 'Stop' : 'Stop All'; }
    await refreshXrayStatus(true);
  }
}

async function refreshXrayStatus(reloadConfigs = true) {
  try {
    const s = await api('/xray-local/status');
    xrayStatusSnapshot = s || { running: false, runs: [] };
    const pill = $('xray-local-pill');
    const pillText = $('xray-local-pill-text');
    const stopAllBtn = $('xray-stop-all-btn');
    const summary = $('xray-local-summary');
    const prevConfigId = xrayRunningConfigId;
    xrayRunningConfigId = s.running ? (s.configId || null) : null;
    if (s.running) {
      pill.className = 'pill green';
      pillText.textContent = `${(s.runs || []).length} running`;
      if (summary) {
        summary.textContent = (s.runs || []).map(run => {
          const cfg = getXrayConfigById(run.configId);
          return `${cfg ? cfg.remark : run.configId} -> ${run.listenAddr}:${run.socksPort}${run.httpPort > 0 ? ` | HTTP ${run.httpPort}` : ''}`;
        }).join(' | ');
      }
      if (stopAllBtn) stopAllBtn.classList.remove('hidden');
    } else {
      pill.className = 'pill';
      pillText.textContent = 'Stopped';
      if (summary) summary.textContent = 'No Xray tunnels running.';
      if (stopAllBtn) stopAllBtn.classList.add('hidden');
    }
    if (reloadConfigs) await loadXrayLocalConfigs();
    else if (prevConfigId !== xrayRunningConfigId) renderXrayConfigs();
    const logsSection = $('xray-logs-section');
    if (logsSection) {
      if (s.running) {
        logsSection.classList.remove('hidden');
        refreshXrayLogs();
      } else {
        logsSection.classList.add('hidden');
      }
    }
    if (!s.binary) {
      pillText.textContent = 'xray not installed';
      if (summary) summary.textContent = 'Install or configure the Xray binary first.';
    }
    if (localAccountsLoaded) {
      await loadLocalAccounts();
    }
  } catch {}
}

async function refreshXrayLogs() {
  try {
    const logs = await api('/xray-local/logs');
    const el = $('xray-logs-output');
    if (el && Array.isArray(logs)) {
      el.textContent = logs.map(l => l.m || '').join('\n');
      el.scrollTop = el.scrollHeight;
    }
  } catch {}
}

async function testXrayConfig(id) {
  const badge = $('xtest-' + id);
  const btn = $('xtest-btn-' + id);
  if (badge) { badge.className = 'xray-test-badge testing'; badge.textContent = '...'; }
  if (btn) btn.disabled = true;
  try {
    const res = await api('/xray-local/test', { method: 'POST', body: { configId: id } });
    if (badge) {
      if (res.ok) {
        badge.className = 'xray-test-badge ok';
        badge.textContent = res.latency + 'ms';
      } else {
        badge.className = 'xray-test-badge fail';
        badge.textContent = 'fail';
      }
    }
  } catch (e) {
    if (badge) { badge.className = 'xray-test-badge fail'; badge.textContent = 'err'; }
  }
  if (btn) btn.disabled = false;
}

// --- Scanner ---
let scanEstimateTimer = null;
function scheduleScanEstimate() {
  clearTimeout(scanEstimateTimer);
  scanEstimateTimer = setTimeout(updateScanEstimate, 400);
}
async function updateScanEstimate() {
  const cidrsRaw = $('scan-cidrs').value.trim();
  const hint = $('scan-ip-hint');
  if (!cidrsRaw) { if (hint) hint.textContent = ''; return; }
  const cidrs = cidrsRaw.split('\n').map(s => s.trim()).filter(Boolean);
  const maxIps = parseInt($('scan-max').value) || 100;
  try {
    const res = await api('/xray-local/scan/estimate', { method: 'POST', body: { cidrs, maxIps } });
    if (hint) hint.textContent = `${res.totalIps} IPs to scan`;
    $('scan-concurrency').value = res.suggestedConcurrency;
  } catch {}
}

async function startScan() {
  const configId = $('scan-config-select').value;
  if (!configId) { alert('Select a config to test'); return; }

  const cidrsRaw = $('scan-cidrs').value.trim();
  if (!cidrsRaw) { alert('Enter CIDR ranges'); return; }
  const cidrs = cidrsRaw.split('\n').map(s => s.trim()).filter(Boolean);

  const concurrency = parseInt($('scan-concurrency').value) || 10;
  const timeout = parseInt($('scan-timeout').value) || 5;
  const maxIps = parseInt($('scan-max').value) || 100;

  scanWorkingResults = [];
  scanConfigId = configId;
  $('scan-progress').classList.remove('hidden');
  $('scan-summary').classList.add('hidden');
  $('scan-fill').style.width = '0%';
  $('scan-status').textContent = 'Starting scan...';
  $('scan-stop-btn').classList.remove('hidden');

  try {
    const resp = await fetch('/api/xray-local/scan', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ configId, cidrs, concurrency, timeout, maxIps }),
    });

    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let totalTested = 0;
    let totalCount = 0;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const evt = JSON.parse(line.slice(6));
          if (evt.type === 'progress') {
            totalTested = evt.tested;
            totalCount = evt.total;
            const pct = Math.round(evt.tested / evt.total * 100);
            $('scan-fill').style.width = pct + '%';
            if (evt.result?.success) {
              scanWorkingResults.push({ ip: evt.result.ip, latency: evt.result.latency });
            }
            $('scan-status').textContent = `${evt.tested}/${evt.total} scanned, ${scanWorkingResults.length} working`;
          } else if (evt.type === 'done') {
            totalTested = evt.tested;
            $('scan-fill').style.width = '100%';
            $('scan-status').textContent = `${evt.tested} scanned, ${scanWorkingResults.length} working`;
          }
        } catch {}
      }
    }
  } catch (e) {
    $('scan-status').textContent = 'Error: ' + e.message;
  }
  $('scan-stop-btn').classList.add('hidden');
  if (scanWorkingResults.length > 0) {
    $('scan-summary').classList.remove('hidden');
    $('scan-summary-text').textContent = `${scanWorkingResults.length} working IPs found`;
  }
}

async function stopScan() {
  await api('/xray-local/scan/stop', { method: 'POST' });
  $('scan-stop-btn').classList.add('hidden');
  $('scan-status').textContent = 'Stopped';
  if (scanWorkingResults.length > 0) {
    $('scan-summary').classList.remove('hidden');
    $('scan-summary-text').textContent = `${scanWorkingResults.length} working IPs found`;
  }
}

function showScanResults() {
  const el = $('scan-results-list');
  el.innerHTML = scanWorkingResults.map(r =>
    `<div class="scan-result ok"><span class="scan-ip">${escapeHtml(r.ip)}</span><span class="scan-latency">${r.latency}ms</span></div>`
  ).join('');
  $('scan-results-modal').classList.remove('hidden');
}

function replaceAddressInLink(link, newIp) {
  link = link.trim();
  if (link.startsWith('vmess://')) {
    try {
      const raw = link.replace('vmess://', '');
      const cfg = JSON.parse(atob(raw));
      cfg.add = newIp;
      return 'vmess://' + btoa(JSON.stringify(cfg));
    } catch { return link; }
  }
  // vless:// trojan:// ss:// — all URI-style: proto://user@HOST:port?...
  try {
    const protoEnd = link.indexOf('://');
    const proto = link.slice(0, protoEnd);
    const rest = link.slice(protoEnd + 3);
    const fakeUrl = new URL('https://' + rest);
    fakeUrl.hostname = newIp;
    return proto + '://' + fakeUrl.href.slice(8); // strip https://
  } catch { return link; }
}

function generateScanConfigs() {
  const cfg = xrayLocalConfigs.find(c => c.id === scanConfigId);
  if (!cfg || !cfg.link) return '';
  return scanWorkingResults
    .map(r => replaceAddressInLink(cfg.link, r.ip))
    .join('\n');
}

function copyScanConfigs() {
  const text = generateScanConfigs();
  if (!text) { alert('No configs to copy'); return; }
  copyToClipboard(text);
}

function downloadScanConfigs() {
  const text = generateScanConfigs();
  if (!text) { alert('No configs to download'); return; }
  const blob = new Blob([text], { type: 'text/plain' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'working-configs.txt';
  a.click();
  URL.revokeObjectURL(a.href);
}

// ─── System Statistics ───────────────────────────────
let statsTimer = null;
let lastNetRx = 0;
let lastNetTx = 0;
let lastNetTime = 0;
let peakConnections = 0;

function startStatsPolling() {
  if (statsTimer) return;
  fetchStats();
  statsTimer = setInterval(fetchStats, 5000);
}

function stopStatsPolling() {
  if (statsTimer) { clearInterval(statsTimer); statsTimer = null; }
}

// Pause polling when tab is not visible
document.addEventListener('visibilitychange', () => {
  if (document.hidden) stopStatsPolling();
  else if (currentPage === 'statistics') startStatsPolling();
});

async function fetchStats() {
  try {
    const data = await api('/system-stats');
    removeSkeleton('stats-skeleton');
    $('stats-system-card').classList.remove('hidden');
    $('stats-network-card').classList.remove('hidden');
    $('stats-conn-card').classList.remove('hidden');
    // CPU
    const cpuPct = Math.round(data.cpu.loadPercent);
    $('sys-cpu').textContent = cpuPct + '%';
    $('sys-cpu-bar').style.width = cpuPct + '%';
    // Memory
    const memPct = Math.round(data.memory.usedPercent);
    $('sys-mem').textContent = formatBytes(data.memory.used) + ' / ' + formatBytes(data.memory.total);
    $('sys-mem-bar').style.width = memPct + '%';
    // Battery
    if (data.battery && data.battery.percent !== null) {
      const bPct = Math.round(data.battery.percent);
      const charging = data.battery.charging ? ' (charging)' : '';
      $('sys-battery').textContent = bPct + '%' + charging;
      $('sys-battery-bar').style.width = bPct + '%';
      const bFill = $('sys-battery-bar');
      bFill.style.background = bPct <= 20 ? 'var(--red)' : bPct <= 50 ? 'var(--amber)' : 'var(--green)';
    } else {
      $('sys-battery').textContent = 'N/A';
      $('sys-battery-bar').style.width = '0%';
    }
    // System info
    $('sys-platform').textContent = data.platform;
    $('sys-hostname').textContent = data.hostname;
    $('sys-os-uptime').textContent = formatUptimeLong(data.osUptime);
    $('sys-cpus').textContent = data.cpu.cores + ' cores';
    // Network
    const now = Date.now();
    $('sys-net-rx').textContent = formatBytes(data.network.rx);
    $('sys-net-tx').textContent = formatBytes(data.network.tx);
    if (lastNetTime > 0) {
      const dt = (now - lastNetTime) / 1000;
      const rxRate = Math.max(0, (data.network.rx - lastNetRx) / dt);
      const txRate = Math.max(0, (data.network.tx - lastNetTx) / dt);
      $('sys-net-rx-rate').textContent = formatBytes(rxRate) + '/s';
      $('sys-net-tx-rate').textContent = formatBytes(txRate) + '/s';
    }
    lastNetRx = data.network.rx;
    lastNetTx = data.network.tx;
    lastNetTime = now;
    // Connections
    const total = data.connections.total;
    const unique = data.connections.uniqueIPs;
    if (total > peakConnections) peakConnections = total;
    $('sys-conn-total').textContent = total;
    $('sys-conn-unique').textContent = unique;
    $('sys-conn-peak').textContent = peakConnections;
    // Server uptime pill
    $('sys-uptime-text').textContent = 'Up ' + formatUptimeLong(data.serverUptime);
  } catch (e) {
    console.error('[Stats] Fetch failed:', e);
  }
}

function formatUptimeLong(sec) {
  if (!sec || sec <= 0) return '--';
  const d = Math.floor(sec / 86400);
  const h = Math.floor((sec % 86400) / 3600);
  const m = Math.floor((sec % 3600) / 60);
  if (d > 0) return d + 'd ' + h + 'h ' + m + 'm';
  if (h > 0) return h + 'h ' + m + 'm';
  return m + 'm';
}

// ─── Xray Binary Settings ────────────────────────────
let xrayBinaryInfo = null;

async function loadXrayBinaryInfo() {
  try {
    const info = await api('/xray/binary-info');
    xrayBinaryInfo = info;
    const sel = $('xray-binary-select');
    sel.innerHTML = '';

    // Auto-detect option
    const autoLabel = info.detected
      ? `Auto-detect (${info.platform}/${info.arch})`
      : 'Auto-detect (not found)';
    sel.insertAdjacentHTML('beforeend', `<option value="">${escapeHtml(autoLabel)}</option>`);

    // Bundled binaries
    for (const b of info.bundled) {
      sel.insertAdjacentHTML('beforeend', `<option value="${escapeHtml(b.path)}">${escapeHtml(b.label)} — ${escapeHtml(b.path)}</option>`);
    }

    // Custom option
    sel.insertAdjacentHTML('beforeend', '<option value="__custom__">Custom path...</option>');

    // Select current
    const customField = $('xray-binary-custom-field');
    if (info.configured && info.configured !== '') {
      const exists = info.bundled.some(b => b.path === info.configured);
      if (exists) {
        sel.value = info.configured;
        customField.classList.add('hidden');
      } else {
        sel.value = '__custom__';
        $('xray-binary-custom').value = info.configured;
        customField.classList.remove('hidden');
      }
    } else {
      sel.value = '';
      customField.classList.add('hidden');
    }

    const statusEl = $('xray-binary-status');
    if (info.current) {
      statusEl.textContent = 'Active: ' + info.current;
    } else {
      statusEl.textContent = 'No xray binary found';
    }
  } catch {}
}

function onXrayBinarySelectChange() {
  const sel = $('xray-binary-select');
  const customField = $('xray-binary-custom-field');
  if (sel.value === '__custom__') {
    customField.classList.remove('hidden');
  } else {
    customField.classList.add('hidden');
  }
}

async function saveXrayBinary() {
  const sel = $('xray-binary-select');
  let binaryPath = sel.value;
  if (binaryPath === '__custom__') {
    binaryPath = $('xray-binary-custom').value.trim();
    if (!binaryPath) { alert('Enter a custom path'); return; }
  }
  if (binaryPath === '') binaryPath = ''; // auto-detect
  try {
    const res = await api('/xray/binary-info', { method: 'POST', body: { xrayBinaryPath: binaryPath } });
    if (res.ok) {
      $('xray-binary-status').textContent = 'Saved. Active: ' + (res.current || 'not found');
      await loadXrayBinaryInfo();
    }
  } catch (e) { alert('Error: ' + e.message); }
}

// ─── Dashboard ───────────────────────────────────────
let dashTimer = null;

function updateDashboard() {
  // Time
  const now = new Date();
  const timeEl = $('dash-time');
  if (timeEl) timeEl.textContent = now.toLocaleString([], { weekday: 'short', hour: '2-digit', minute: '2-digit' });

  // User stats
  const total = localAccountsList.length;
  const online = localAccountsList.filter(a => a.online && a.enabled).length;
  const totalTraffic = localAccountsList.reduce((s, a) => s + (a.usedTraffic || 0), 0);
  const activeConns = localAccountsList.reduce((s, a) => s + (a.activeConnections || 0), 0);

  const el = (id, v) => { const e = $(id); if (e) e.textContent = v; };
  el('dash-total-users', total);
  el('dash-online-users', online);
  el('dash-total-traffic', formatBytes(totalTraffic));
  el('dash-active-conns', activeConns);

  // Service status
  const tunnelDot = $('dash-svc-tunnel-dot');
  const tunnelStatus = $('dash-svc-tunnel-status');
  if (tunnelDot && tunnelStatus) {
    tunnelDot.className = 'dash-svc-dot ' + statusColor(state.status);
    tunnelStatus.textContent = statusLabel(state.status);
  }
  const vpnDot = $('dash-svc-vpn-dot');
  const vpnStatus = $('dash-svc-vpn-status');
  if (vpnDot && vpnStatus) {
    vpnDot.className = 'dash-svc-dot ' + ocColor(ocState.status);
    vpnStatus.textContent = ocLabel(ocState.status);
  }

  // Recent users (top 5 by most recent traffic or creation)
  const recentEl = $('dash-recent-users');
  if (recentEl) {
    if (!localAccountsList.length) {
      recentEl.innerHTML = '<div class="empty" style="padding:24px"><p>No accounts yet</p></div>';
    } else {
      const sorted = [...localAccountsList].sort((a, b) => (b.usedTraffic || 0) - (a.usedTraffic || 0)).slice(0, 5);
      recentEl.innerHTML = sorted.map(acc => {
        const dot = acc.online && acc.enabled ? 'green' : '';
        return `<div class="dash-recent-row" onclick="switchPage('v2ray')">
          <span class="dash-svc-dot ${dot}"></span>
          <span class="dash-recent-name">${escapeHtml(acc.name)}</span>
          <span class="dash-recent-meta">${formatBytes(acc.usedTraffic || 0)}</span>
          <span class="dash-recent-meta">${acc.online ? acc.speed || '' : ''}</span>
        </div>`;
      }).join('');
    }
  }

  // Connection quality graph
  renderQualityGraph();

  // Load accounts if not loaded
  if (!localAccountsLoaded && total === 0) {
    loadLocalAccounts().then(() => updateDashboard());
  }
}

function renderQualityGraph() {
  const canvas = $('quality-canvas');
  if (!canvas) return;
  const history = (state.health && state.health.history) || [];
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth;
  const h = canvas.clientHeight;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, w, h);

  if (!history.length) {
    ctx.fillStyle = getComputedStyle(document.body).getPropertyValue('--t3') || '#888';
    ctx.font = '12px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText('No health data yet', w / 2, h / 2);
    return;
  }

  const pad = { top: 10, right: 10, bottom: 4, left: 40 };
  const gw = w - pad.left - pad.right;
  const gh = h - pad.top - pad.bottom;

  // Find max latency for scale
  const latencies = history.map(p => p.latency).filter(l => l != null);
  const maxLat = Math.max(200, ...latencies) * 1.1;

  // Grid lines
  ctx.strokeStyle = getComputedStyle(document.body).getPropertyValue('--border') || '#333';
  ctx.lineWidth = 0.5;
  for (let i = 0; i <= 4; i++) {
    const y = pad.top + (gh / 4) * i;
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(w - pad.right, y); ctx.stroke();
    ctx.fillStyle = getComputedStyle(document.body).getPropertyValue('--t3') || '#888';
    ctx.font = '9px sans-serif';
    ctx.textAlign = 'right';
    ctx.fillText(Math.round(maxLat - (maxLat / 4) * i) + 'ms', pad.left - 4, y + 3);
  }

  // Plot
  const step = gw / Math.max(history.length - 1, 1);
  ctx.beginPath();
  ctx.strokeStyle = '#22c55e';
  ctx.lineWidth = 1.5;
  let hasLine = false;
  for (let i = 0; i < history.length; i++) {
    const p = history[i];
    const x = pad.left + step * i;
    if (p.latency == null) {
      // Draw red marker for failed check
      ctx.stroke();
      ctx.beginPath();
      hasLine = false;
      ctx.fillStyle = '#ef4444';
      ctx.fillRect(x - 1.5, pad.top, 3, gh);
      continue;
    }
    const y = pad.top + gh - (p.latency / maxLat) * gh;
    if (!hasLine) { ctx.beginPath(); ctx.moveTo(x, y); hasLine = true; }
    else ctx.lineTo(x, y);
  }
  if (hasLine) ctx.stroke();

  // Fill area under curve
  ctx.globalAlpha = 0.1;
  ctx.beginPath();
  let started = false;
  let lastX = 0;
  for (let i = 0; i < history.length; i++) {
    const p = history[i];
    if (p.latency == null) { if (started) { ctx.lineTo(lastX, pad.top + gh); ctx.fill(); ctx.beginPath(); started = false; } continue; }
    const x = pad.left + step * i;
    const y = pad.top + gh - (p.latency / maxLat) * gh;
    if (!started) { ctx.moveTo(x, pad.top + gh); ctx.lineTo(x, y); started = true; }
    else ctx.lineTo(x, y);
    lastX = x;
  }
  if (started) { ctx.lineTo(lastX, pad.top + gh); ctx.fillStyle = '#22c55e'; ctx.fill(); }
  ctx.globalAlpha = 1;

  // Time labels
  if (history.length > 1) {
    const first = new Date(history[0].time);
    $('quality-label-left').textContent = first.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  }
}

// ─── Settings Tabs ───────────────────────────────────
let settingsInitialized = false;

function initSettingsPage() {
  if (!settingsInitialized) {
    removeSkeleton('settings-skeleton');
    $('settings-tabs').classList.remove('hidden');
    loadXuiConfig();
    loadXrayBinaryInfo();
    loadOcSettings();
    loadDnsSettings();
    loadAdminInfo();
    settingsInitialized = true;
  }
}

function switchSettingsTab(tab) {
  document.querySelectorAll('.settings-tab[data-stab]').forEach(el => {
    el.classList.toggle('active', el.dataset.stab === tab);
  });
  ['general', 'admin', 'vpn', 'dns', 'routing', 'xray', 'deploy'].forEach(t => {
    const pane = $('stab-' + t);
    if (pane) pane.classList.toggle('hidden', t !== tab);
  });
  if (tab === 'routing') loadLocalRoutes();
}

// ─── Accounts Table View ─────────────────────────────
let accSortCol = 'name';
let accSortAsc = true;
let accSearchTerm = '';
let expandedAccId = null;

function filterAccounts() {
  accSearchTerm = ($('acc-search')?.value || '').toLowerCase();
  renderLocalAccounts();
}

function sortAccounts(col) {
  if (accSortCol === col) accSortAsc = !accSortAsc;
  else { accSortCol = col; accSortAsc = true; }
  renderLocalAccounts();
}

function toggleAccDetail(id) {
  expandedAccId = expandedAccId === id ? null : id;
  renderLocalAccounts();
  if (expandedAccId) {
    const acc = localAccountsList.find(a => a.id === expandedAccId);
    if (acc && acc.activeIps && acc.activeIps.length > 0) {
      fetchGeoForIps(acc.activeIps, () => renderLocalAccounts());
    }
  }
}

// ─── WebSocket Indicator ─────────────────────────────
function updateWsIndicator(status) {
  const dot = $('ws-dot');
  if (!dot) return;
  dot.className = 'ws-dot ' + status;
  const ind = $('ws-indicator');
  if (ind) ind.title = 'WebSocket ' + status;
}

// ─── OpenVPN Server Page ─────────────────────────────
let ovpnServerInstalled = null;
let ovpnServerDebugTimer = null;

function formatDebugTime(ts) {
  return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function renderTimedLogs(outputId, entries, emptyText) {
  const el = $(outputId);
  if (!el) return;
  if (!entries || !entries.length) {
    el.textContent = emptyText;
    return;
  }
  el.textContent = entries.map(entry => `[${formatDebugTime(entry.time)}] ${entry.text}`).join('\n');
  el.scrollTop = el.scrollHeight;
}

function appendTimedLogEntry(outputId, entry, emptyText) {
  const el = $(outputId);
  if (!el || !entry) return;
  const line = `[${formatDebugTime(entry.time)}] ${entry.text}`;
  if (!el.textContent || el.textContent === emptyText) {
    el.textContent = line;
  } else {
    const lines = (el.textContent + '\n' + line).split('\n');
    el.textContent = lines.slice(-400).join('\n');
  }
  el.scrollTop = el.scrollHeight;
}

function renderTextLogs(outputId, lines, emptyText) {
  const el = $(outputId);
  if (!el) return;
  el.textContent = lines && lines.length ? lines.join('\n') : emptyText;
  el.scrollTop = el.scrollHeight;
}

function copyLogOutput(outputId, emptyText) {
  const el = $(outputId);
  if (!el) return;
  const text = (el.textContent || '').trim();
  if (!text || text === emptyText) {
    showToast('No logs to copy');
    return;
  }
  copyToClipboard(text);
}

async function clearLogOutput(path, outputId, emptyText) {
  try {
    const res = await api(path, { method: 'POST' });
    if (res?.error) {
      showToast(res.error, 'error');
      return;
    }
    renderTextLogs(outputId, [], emptyText);
    showToast('Logs cleared');
  } catch (e) {
    showToast(`Failed to clear logs: ${e.message}`);
  }
}

async function refreshRoutingLogs() {
  try {
    const logs = await api('/routing/logs');
    renderTimedLogs('routing-logs-output', logs, 'No routing logs yet');
  } catch (e) {
    renderTextLogs('routing-logs-output', [`Failed to load routing logs: ${e.message}`], 'No routing logs yet');
  }
}

function copyRoutingLogs() {
  copyLogOutput('routing-logs-output', 'No routing logs yet');
}

async function clearRoutingLogs() {
  await clearLogOutput('/routing/logs/clear', 'routing-logs-output', 'No routing logs yet');
}

async function refreshOvpnServerLogs() {
  try {
    const data = await api('/ovpn-server/logs');
    const lines = [];
    if (Array.isArray(data.runtime) && data.runtime.length) {
      lines.push(...data.runtime.map(entry => `[${formatDebugTime(entry.time)}] ${entry.text}`));
    }
    if (Array.isArray(data.fileTail) && data.fileTail.length) {
      if (lines.length) lines.push('', '--- /var/log/openvpn.log ---');
      lines.push(...data.fileTail);
    }
    renderTextLogs('ovpn-server-logs-output', lines, 'No OpenVPN log output yet');
  } catch (e) {
    renderTextLogs('ovpn-server-logs-output', [`Failed to load OpenVPN logs: ${e.message}`], 'No OpenVPN log output yet');
  }
}

function copyOvpnServerLogs() {
  copyLogOutput('ovpn-server-logs-output', 'No OpenVPN log output yet');
}

async function clearOvpnServerLogs() {
  await clearLogOutput('/ovpn-server/logs/clear', 'ovpn-server-logs-output', 'No OpenVPN log output yet');
}

function startOvpnServerDebugPolling() {
  stopOvpnServerDebugPolling();
  refreshRoutingLogs();
  refreshOvpnServerLogs();
  ovpnServerDebugTimer = setInterval(() => {
    if (currentPage === 'ovpn-server') {
      refreshRoutingLogs();
      refreshOvpnServerLogs();
    }
  }, 3000);
}

function stopOvpnServerDebugPolling() {
  if (ovpnServerDebugTimer) {
    clearInterval(ovpnServerDebugTimer);
    ovpnServerDebugTimer = null;
  }
}

function vpnUserStatusBadge(u) {
  if (u.online) return '<span style="color:var(--green);font-weight:600">ONLINE</span>';
  if (u.expired) return '<span style="color:var(--red)">EXPIRED</span>';
  if (u.overBandwidth) return '<span style="color:var(--red)">BW LIMIT</span>';
  if (!u.enabled) return '<span style="color:var(--amber)">DISABLED</span>';
  return '<span style="opacity:0.5">offline</span>';
}

function vpnUserMetaLine(u) {
  const parts = [];
  if (u.expiresAt) {
    const d = new Date(u.expiresAt);
    const left = u.expiresAt - Date.now();
    const days = Math.ceil(left / 86400000);
    parts.push(`Expires: ${d.toLocaleDateString()}${left > 0 ? ` (${days}d left)` : ' (expired)'}`);
  }
  if (u.bandwidthLimit) {
    parts.push(`Traffic: ${formatBytes(u.usedTraffic || 0)} / ${formatBytes(u.bandwidthLimit)}`);
  } else if (u.usedTraffic) {
    parts.push(`Traffic: ${formatBytes(u.usedTraffic)}`);
  }
  if (u.speedLimit) parts.push(`Speed: ${formatBytes(u.speedLimit)}/s`);
  if (u.online && u.clientIp) parts.push(`IP: ${u.clientIp}`);
  return parts.join(' | ');
}

async function loadOvpnServerPage() {
  const status = await api('/vpn-server/install-status');
  ovpnServerInstalled = status.installed;

  if (!status.installed) {
    $('ovpn-server-not-installed').classList.remove('hidden');
    $('ovpn-server-main').classList.add('hidden');
    return;
  }
  $('ovpn-server-not-installed').classList.add('hidden');
  $('ovpn-server-main').classList.remove('hidden');

  const [st, users, settings] = await Promise.all([
    api('/ovpn-server/status'),
    api('/ovpn-server/users'),
    api('/ovpn-server/settings'),
  ]);
  updateOvpnServerUI(st, settings);
  renderOvpnUsers(users);
  refreshRoutingLogs();
  refreshOvpnServerLogs();
}

function updateOvpnServerUI(st, settings) {
  const pill = $('ovpn-server-pill');
  const pillText = $('ovpn-server-pill-text');
  const statusText = $('ovpn-server-status-text');
  const btnStart = $('btn-ovpn-start');
  const btnStop = $('btn-ovpn-stop');

  pillText.textContent = st.status === 'running' ? 'Running' : 'Stopped';
  pill.className = 'pill ' + (st.status === 'running' ? 'green' : '');

  const maxInfo = settings && settings.maxUsers > 0 ? ` (max: ${settings.maxUsers})` : '';
  statusText.textContent = st.status === 'running'
    ? `Running — ${st.connectedClients.length} client(s) connected, ${st.usersCount} user(s)${maxInfo}`
    : 'Stopped';

  btnStart.classList.toggle('hidden', st.status === 'running');
  btnStop.classList.toggle('hidden', st.status !== 'running');

  // Routing status
  const routeText = $('routing-status-text');
  const btnRouteStart = $('btn-routing-start');
  const btnRouteStop = $('btn-routing-stop');
  const routeSelect = $('routing-socks-select');
  routeText.textContent = `Redsocks: ${st.redsocks || 'stopped'} | SOCKS port: ${st.socksPort || '--'}`;
  btnRouteStart.classList.toggle('hidden', st.redsocks === 'running');
  btnRouteStop.classList.toggle('hidden', st.redsocks !== 'running');
  if (routeSelect) {
    const current = routeSelect.value;
    routeSelect.innerHTML = '<option value="">Auto-detect</option>';
    for (const source of (st.socksSources || [])) {
      routeSelect.insertAdjacentHTML('beforeend', `<option value="${source.port}">${escapeHtml(source.label)} :${source.port}</option>`);
    }
    if (current && [...routeSelect.options].some(opt => opt.value === current)) routeSelect.value = current;
  }

  // Connected clients
  const clientsEl = $('ovpn-connected-clients');
  if (st.connectedClients && st.connectedClients.length > 0) {
    clientsEl.innerHTML = st.connectedClients.map(c =>
      `<div style="margin-bottom:4px"><strong>${escapeHtml(c.name)}</strong> — ${escapeHtml(c.realAddr)} — ↓${formatBytes(c.bytesRecv)} ↑${formatBytes(c.bytesSent)}</div>`
    ).join('');
  } else {
    clientsEl.textContent = 'No clients connected';
  }

  // Max users input
  const maxEl = $('ovpn-max-users');
  if (maxEl && settings) maxEl.value = settings.maxUsers || '';
}

function renderOvpnUsers(users) {
  const container = $('ovpn-users-list');
  if (!users.length) {
    container.innerHTML = '<div class="card" style="text-align:center;padding:16px;opacity:0.6">No users yet</div>';
    return;
  }
  container.innerHTML = users.map(u => `
    <div class="card" style="margin-bottom:8px;${u.expired || u.overBandwidth ? 'opacity:0.6;' : ''}${u.online ? 'border-left:3px solid var(--green);' : ''}">
      <div class="card-row">
        <div style="flex:1;min-width:0">
          <div class="card-title">${escapeHtml(u.name)} ${vpnUserStatusBadge(u)}</div>
          <div class="card-meta">${vpnUserMetaLine(u) || 'No limits set'}</div>
        </div>
        <div class="card-actions" style="flex-shrink:0">
          <button class="btn btn-primary btn-touch" onclick="downloadOvpnConfig('${u.id}', '${escapeHtml(u.name)}')" title="Download config" style="font-size:12px;padding:4px 8px">.ovpn</button>
          <button class="btn-icon" onclick="editOvpnUser('${u.id}')" title="Edit">
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
          </button>
          <button class="btn-icon danger" onclick="deleteOvpnUser('${u.id}')" title="Delete">
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/></svg>
          </button>
        </div>
      </div>
    </div>
  `).join('');
}

async function startOvpnServer() {
  await api('/ovpn-server/start', { method: 'POST' });
  loadOvpnServerPage();
}

async function stopOvpnServer() {
  await api('/ovpn-server/stop', { method: 'POST' });
  loadOvpnServerPage();
}

async function startRouting() {
  const socksPort = $('routing-socks-select')?.value;
  const body = socksPort ? { socksPort: parseInt(socksPort, 10) } : {};
  const res = await api('/routing/start', { method: 'POST', body });
  if (res.error) { showToast(res.error, 'error'); return; }
  loadOvpnServerPage();
}

async function stopRouting() {
  await api('/routing/stop', { method: 'POST' });
  loadOvpnServerPage();
}

async function saveOvpnMaxUsers() {
  const val = $('ovpn-max-users').value;
  await api('/ovpn-server/settings', { method: 'POST', body: { maxUsers: parseInt(val) || 0 } });
  showToast('Max users updated');
}

function showVpnUserModal(type, editUser) {
  const isEdit = !!editUser;
  const isL2tp = type === 'l2tp';
  const title = `${isEdit ? 'Edit' : 'Add'} ${isL2tp ? 'L2TP' : 'OpenVPN'} User`;

  // Build modal HTML dynamically
  let html = `<div class="modal-overlay" id="vpn-user-modal-overlay" onclick="if(event.target===this)closeVpnUserModal()">
    <div class="modal">
      <div class="modal-header">
        <h3>${title}</h3>
        <button class="btn-close" onclick="closeVpnUserModal()">&times;</button>
      </div>
      <form onsubmit="submitVpnUser(event,'${type}',${isEdit ? `'${editUser.id}'` : 'null'})">`;

  if (!isEdit) {
    html += `<div class="field">
        <label>${isL2tp ? 'Username' : 'Name'}</label>
        <input type="text" class="input" id="vpn-u-name" placeholder="${isL2tp ? 'username' : 'alphanumeric-name'}" required />
      </div>`;
    if (isL2tp) {
      html += `<div class="field">
          <label>Password</label>
          <input type="text" class="input" id="vpn-u-password" required />
        </div>`;
    }
  }

  const expVal = editUser && editUser.expiresAt ? new Date(editUser.expiresAt).toISOString().slice(0, 10) : '';
  const bwVal = editUser && editUser.bandwidthLimit ? (editUser.bandwidthLimit / (1024*1024*1024)).toFixed(2) : '';
  const spVal = editUser && editUser.speedLimit ? (editUser.speedLimit / (1024*1024)).toFixed(1) : '';

  html += `<div class="field">
        <label>Expiry Date <small>(leave empty = never)</small></label>
        <input type="date" class="input" id="vpn-u-expiry" value="${expVal}" />
      </div>
      <div class="field-row">
        <div class="field">
          <label>Bandwidth Limit (GB) <small>(0 = unlimited)</small></label>
          <input type="number" class="input" id="vpn-u-bandwidth" step="0.01" min="0" placeholder="0" value="${bwVal}" />
        </div>
        <div class="field">
          <label>Speed Limit (MB/s) <small>(0 = unlimited)</small></label>
          <input type="number" class="input" id="vpn-u-speed" step="0.1" min="0" placeholder="0" value="${spVal}" />
        </div>
      </div>`;

  if (isEdit) {
    html += `<div class="field" style="display:flex;align-items:center;gap:12px">
        <label style="margin:0"><input type="checkbox" id="vpn-u-enabled" ${editUser.enabled ? 'checked' : ''} /> Enabled</label>
        <label style="margin:0"><input type="checkbox" id="vpn-u-reset-traffic" /> Reset traffic counter</label>
      </div>`;
    if (isL2tp) {
      html += `<div class="field">
          <label>New Password <small>(leave empty to keep)</small></label>
          <input type="text" class="input" id="vpn-u-password" />
        </div>`;
    }
  }

  html += `<div class="modal-actions">
          <button type="button" class="btn btn-ghost" onclick="closeVpnUserModal()">Cancel</button>
          <button type="submit" class="btn btn-primary">${isEdit ? 'Save' : 'Create'}</button>
        </div>
      </form>
    </div>
  </div>`;

  // Remove old modal if any
  closeVpnUserModal();
  document.body.insertAdjacentHTML('beforeend', html);
}

function closeVpnUserModal() {
  const el = $('vpn-user-modal-overlay');
  if (el) el.remove();
}

async function submitVpnUser(e, type, editId) {
  e.preventDefault();
  const expiryVal = $('vpn-u-expiry').value;
  const bwVal = parseFloat($('vpn-u-bandwidth').value) || 0;
  const spVal = parseFloat($('vpn-u-speed').value) || 0;

  const body = {
    expiresAt: expiryVal || null,
    bandwidthLimit: bwVal > 0 ? Math.round(bwVal * 1024 * 1024 * 1024) : null,
    speedLimit: spVal > 0 ? Math.round(spVal * 1024 * 1024) : null,
  };

  if (editId) {
    // Edit mode
    const enabledEl = $('vpn-u-enabled');
    const resetEl = $('vpn-u-reset-traffic');
    body.enabled = enabledEl ? enabledEl.checked : true;
    body.resetTraffic = resetEl ? resetEl.checked : false;
    const pwEl = $('vpn-u-password');
    if (pwEl && pwEl.value) body.password = pwEl.value;

    const endpoint = type === 'l2tp' ? `/l2tp/users/${editId}` : `/ovpn-server/users/${editId}`;
    const res = await api(endpoint, { method: 'PUT', body });
    if (res.error) { showToast(res.error, 'error'); return; }
  } else {
    // Create mode
    const nameEl = $('vpn-u-name');
    if (type === 'l2tp') {
      body.username = nameEl.value;
      body.password = $('vpn-u-password').value;
    } else {
      body.name = nameEl.value;
    }
    const endpoint = type === 'l2tp' ? '/l2tp/users' : '/ovpn-server/users';
    const res = await api(endpoint, { method: 'POST', body });
    if (res.error) { showToast(res.error, 'error'); return; }
    showToast('User created');
  }

  closeVpnUserModal();
  if (type === 'l2tp') loadL2tpPage();
  else loadOvpnServerPage();
}

function addOvpnUser() { showVpnUserModal('ovpn', null); }

function editOvpnUser(id) {
  api('/ovpn-server/users').then(users => {
    const u = users.find(x => x.id === id);
    if (u) showVpnUserModal('ovpn', u);
  });
}

async function deleteOvpnUser(id) {
  if (!confirm('Delete this user? Their certificate will be revoked.')) return;
  await api(`/ovpn-server/users/${id}`, { method: 'DELETE' });
  showToast('User deleted');
  loadOvpnServerPage();
}

function downloadOvpnConfig(id, name) {
  const serverIp = prompt('Enter server public IP/hostname for the .ovpn config:');
  if (!serverIp) return;
  window.open(`/api/ovpn-server/users/${id}/config?serverIp=${encodeURIComponent(serverIp)}`, '_blank');
}

async function installVpnServers() {
  const logEl = $('vpn-install-log');
  if (logEl) { logEl.classList.remove('hidden'); logEl.textContent = 'Starting installation...\n'; }
  await api('/vpn-server/install', { method: 'POST' });
}

// ─── L2TP Page ───────────────────────────────────────

async function loadL2tpPage() {
  const status = await api('/vpn-server/install-status');
  if (!status.installed) {
    $('l2tp-not-installed').classList.remove('hidden');
    $('l2tp-main').classList.add('hidden');
    return;
  }
  $('l2tp-not-installed').classList.add('hidden');
  $('l2tp-main').classList.remove('hidden');

  const [st, users, settings] = await Promise.all([
    api('/l2tp/status'),
    api('/l2tp/users'),
    api('/l2tp/settings'),
  ]);
  updateL2tpUI(st, settings);
  renderL2tpUsers(users);
}

function updateL2tpUI(st, settings) {
  const pill = $('l2tp-pill');
  const pillText = $('l2tp-pill-text');
  const statusText = $('l2tp-status-text');
  const btnStart = $('btn-l2tp-start');
  const btnStop = $('btn-l2tp-stop');

  pillText.textContent = st.status === 'running' ? 'Running' : 'Stopped';
  pill.className = 'pill ' + (st.status === 'running' ? 'green' : '');

  const maxInfo = settings && settings.maxUsers > 0 ? ` (max: ${settings.maxUsers})` : '';
  statusText.textContent = st.status === 'running'
    ? `Running — ${st.connectedClients.length} client(s) connected, ${st.usersCount} user(s)${maxInfo}`
    : 'Stopped';

  btnStart.classList.toggle('hidden', st.status === 'running');
  btnStop.classList.toggle('hidden', st.status !== 'running');

  // PSK
  if (st.psk) $('l2tp-psk').textContent = st.psk;

  // Connected clients
  const clientsEl = $('l2tp-connected-clients');
  if (st.connectedClients && st.connectedClients.length > 0) {
    clientsEl.innerHTML = st.connectedClients.map(c =>
      `<div style="margin-bottom:4px">${escapeHtml(typeof c === 'string' ? c : c.ip)}${c.username ? ' — ' + escapeHtml(c.username) : ''}</div>`
    ).join('');
  } else {
    clientsEl.textContent = 'No clients connected';
  }

  // Max users input
  const maxEl = $('l2tp-max-users');
  if (maxEl && settings) maxEl.value = settings.maxUsers || '';
}

function renderL2tpUsers(users) {
  const container = $('l2tp-users-list');
  if (!users.length) {
    container.innerHTML = '<div class="card" style="text-align:center;padding:16px;opacity:0.6">No users yet</div>';
    return;
  }
  container.innerHTML = users.map(u => `
    <div class="card" style="margin-bottom:8px;${u.expired || u.overBandwidth ? 'opacity:0.6;' : ''}${u.online ? 'border-left:3px solid var(--green);' : ''}">
      <div class="card-row">
        <div style="flex:1;min-width:0">
          <div class="card-title">${escapeHtml(u.username)} ${vpnUserStatusBadge(u)}</div>
          <div class="card-meta">${vpnUserMetaLine(u) || 'No limits set'}</div>
        </div>
        <div class="card-actions" style="flex-shrink:0">
          <button class="btn-icon" onclick="editL2tpUser('${u.id}')" title="Edit">
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
          </button>
          <button class="btn-icon danger" onclick="deleteL2tpUser('${u.id}')" title="Delete">
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/></svg>
          </button>
        </div>
      </div>
    </div>
  `).join('');
}

async function startL2tpServer() {
  const res = await api('/l2tp/start', { method: 'POST' });
  if (res.error) { showToast(res.error, 'error'); return; }
  loadL2tpPage();
}

async function stopL2tpServer() {
  await api('/l2tp/stop', { method: 'POST' });
  loadL2tpPage();
}

function addL2tpUser() { showVpnUserModal('l2tp', null); }

function editL2tpUser(id) {
  api('/l2tp/users').then(users => {
    const u = users.find(x => x.id === id);
    if (u) showVpnUserModal('l2tp', u);
  });
}

async function saveL2tpMaxUsers() {
  const val = $('l2tp-max-users').value;
  await api('/l2tp/settings', { method: 'POST', body: { maxUsers: parseInt(val) || 0 } });
  showToast('Max users updated');
}

async function deleteL2tpUser(id) {
  if (!confirm('Delete this user?')) return;
  await api(`/l2tp/users/${id}`, { method: 'DELETE' });
  showToast('User deleted');
  loadL2tpPage();
}

// ─── Keyboard Shortcuts ──────────────────────────────
document.addEventListener('keydown', (e) => {
  // Don't trigger in inputs
  if (['INPUT', 'TEXTAREA', 'SELECT'].includes(e.target.tagName)) return;
  if (e.ctrlKey || e.metaKey || e.altKey) return;

  switch (e.key) {
    case '1': switchPage('dashboard'); break;
    case '2': switchPage('tunnels'); break;
    case '3': switchPage('vpn'); break;
    case '4': switchPage('connections'); break;
    case '5': switchPage('v2ray'); break;
    case '6': switchPage('xray'); break;
    case '7': switchPage('statistics'); break;
    case '8': switchPage('settings'); break;
    case 'n': case 'N':
      if (currentPage === 'v2ray') openAddLocalAccount();
      else if (currentPage === 'tunnels') $('btn-add-tunnel')?.click();
      break;
    case '/':
      e.preventDefault();
      const search = $('acc-search');
      if (search && currentPage === 'v2ray') search.focus();
      break;
  }
});

// ─── Init ────────────────────────────────────────────
loadTunnels();
connectWs();
initNotifToggle();

// Load accounts early for dashboard
loadLocalAccounts();

// Restore page from URL hash or default to dashboard
const validPages = ['dashboard', 'tunnels', 'vpn', 'connections', 'v2ray', 'xray', 'ovpn-server', 'l2tp', 'statistics', 'settings'];
const hashPage = location.hash.replace('#', '');
switchPage(validPages.includes(hashPage) ? hashPage : 'dashboard');

// Settings init is deferred to when user visits the page
// Update dashboard clock every minute
setInterval(() => { if (currentPage === 'dashboard') updateDashboard(); }, 60000);
