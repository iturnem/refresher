'use strict';

const express  = require('express');
const { WebSocketServer } = require('ws');
const http     = require('http');
const os       = require('os');
const path     = require('path');
const { spawn } = require('child_process');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ─── Network interfaces ───────────────────────────────────────────────────────
function getInterfaces() {
  return Object.entries(os.networkInterfaces())
    .flatMap(([name, addrs]) =>
      (addrs || [])
        .filter(a => a.family === 'IPv4' && !a.internal)
        .map(a => ({ name, address: a.address, netmask: a.netmask }))
    );
}

// ─── Device registry ──────────────────────────────────────────────────────────
const devices = new Map();

function registerDevice(id, name, address, numTx, numRx, real = false) {
  if (devices.has(id) && devices.get(id).real) return; // don't overwrite live with live
  const txChannels = Array.from({ length: numTx }, (_, i) => ({ id: i + 1, name: `${name} Tx ${i + 1}` }));
  const rxChannels = Array.from({ length: numRx }, (_, i) => ({ id: i + 1, name: `${name} Rx ${i + 1}` }));
  devices.set(id, { id, name, address, txChannels, rxChannels, real });
  console.log(`[device] ${real ? 'Live' : 'Demo'}: ${name} @ ${address}`);
  broadcastDeviceList();
}

const DEMO_DEVICES = [
  { id: 'console-1',  name: 'Console 1',   address: '192.168.1.10', tx: 16, rx: 16 },
  { id: 'stagebox-a', name: 'Stage Box A', address: '192.168.1.20', tx: 16, rx: 8  },
  { id: 'ioa-rack',   name: 'I/O Rack',    address: '192.168.1.30', tx: 8,  rx: 8  },
  { id: 'dvs-pc',     name: 'DVS (PC)',     address: '192.168.1.40', tx: 8,  rx: 8  },
];
for (const d of DEMO_DEVICES) registerDevice(d.id, d.name, d.address, d.tx, d.rx, false);

// ─── dns-sd discovery (uses macOS system mDNS — same stack as Dante Controller)
// Step 1: browse for _netaudio-arc._udp  → get instance names
// Step 2: lookup each instance           → get hostname + port
// Step 3: resolve hostname               → get IP address

let browseProc    = null;
const resolveProcs = new Map();  // instanceName → child process

function startDiscovery(ifaceName) {
  stopDiscovery();

  // Build args — optionally bind to a specific interface name (e.g. "en5")
  const browseArgs = ifaceName
    ? ['-i', ifaceName, '-B', '_netaudio-arc._udp', 'local']
    : ['-B', '_netaudio-arc._udp', 'local'];

  console.log(`[dns-sd] Browse: dns-sd ${browseArgs.join(' ')}`);
  browseProc = spawn('dns-sd', browseArgs);

  let buf = '';
  browseProc.stdout.on('data', chunk => {
    buf += chunk.toString();
    const lines = buf.split('\n');
    buf = lines.pop();
    for (const line of lines) {
      console.log('[dns-sd browse]', line);
      // Output format: "HH:MM:SS.mmm  Add   <flags>  <ifindex>  <domain>  <regtype>  <instance>"
      const m = line.match(/^\s*\d+:\d+:\d+\.\d+\s+Add\s+\S+\s+\S+\s+\S+\s+\S+\s+(.+)$/);
      if (m) resolveInstance(m[1].trim());
    }
  });

  browseProc.stderr.on('data', d => console.error('[dns-sd browse stderr]', d.toString().trim()));
  browseProc.on('error', e => console.error('[dns-sd browse error]', e.message));
  browseProc.on('close', code => console.log('[dns-sd browse] exited', code));
}

function resolveInstance(instanceName) {
  if (resolveProcs.has(instanceName)) return;
  console.log(`[dns-sd] Resolving instance: "${instanceName}"`);

  const proc = spawn('dns-sd', ['-L', instanceName, '_netaudio-arc._udp', 'local']);
  resolveProcs.set(instanceName, proc);

  let buf = '';
  proc.stdout.on('data', chunk => {
    buf += chunk.toString();
    console.log('[dns-sd lookup]', chunk.toString().trim());
    // "InstanceName._netaudio-arc._udp.local. can be reached at hostname.local.:4440 (interface N)"
    const m = buf.match(/can be reached at ([^.:\s]+)\.local\.:(\d+)/i);
    if (m) {
      resolveHostname(instanceName, m[1]);
      proc.kill();
    }
  });

  proc.stderr.on('data', d => console.error('[dns-sd lookup stderr]', d.toString().trim()));
  proc.on('error', e => console.error('[dns-sd lookup error]', e.message));
  setTimeout(() => { proc.kill(); resolveProcs.delete(instanceName); }, 8000);
}

function resolveHostname(instanceName, hostname) {
  console.log(`[dns-sd] Resolving hostname: ${hostname}.local`);
  const proc = spawn('dns-sd', ['-G', 'v4', `${hostname}.local`]);

  let buf = '';
  proc.stdout.on('data', chunk => {
    buf += chunk.toString();
    console.log('[dns-sd getaddr]', chunk.toString().trim());
    // Look for a valid IPv4 address in the output
    const m = buf.match(/\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\b/);
    if (m && m[1] !== '0.0.0.0') {
      const address = m[1];
      const name = instanceName.trim();
      const id   = name.toLowerCase().replace(/[^a-z0-9]+/g, '-');
      if (!devices.has(id) || !devices.get(id).real) {
        if (devices.has(id)) devices.delete(id); // remove demo placeholder
        registerDevice(id, name, address, 16, 16, true);
      }
      proc.kill();
    }
  });

  proc.stderr.on('data', d => console.error('[dns-sd getaddr stderr]', d.toString().trim()));
  proc.on('error', e => console.error('[dns-sd getaddr error]', e.message));
  setTimeout(() => proc.kill(), 8000);
}

function stopDiscovery() {
  if (browseProc) { browseProc.kill(); browseProc = null; }
  for (const p of resolveProcs.values()) p.kill();
  resolveProcs.clear();
}

// ─── REST API ─────────────────────────────────────────────────────────────────
app.get('/api/interfaces', (_req, res) => {
  res.json(getInterfaces());
});

app.post('/api/discover', (req, res) => {
  const { interface: ifaceAddr } = req.body;
  if (!ifaceAddr) return res.status(400).json({ error: 'interface address required' });

  // Find the interface name (e.g. "en5") from the IP address
  const ifaces = getInterfaces();
  const iface  = ifaces.find(i => i.address === ifaceAddr);
  const ifaceName = iface ? iface.name : null;

  // Remove stale live devices, keep demo devices
  for (const [id, d] of devices) { if (d.real) devices.delete(id); }
  broadcastDeviceList();

  startDiscovery(ifaceName);
  res.json({ ok: true, interface: ifaceAddr, ifaceName });
});

// ─── WebSocket + simulated levels ────────────────────────────────────────────
const activeCrossPoints = new Map();

class CrossPointState {
  constructor() {
    this.baseLevel = -18 - Math.random() * 20;
    this.variance  = 4   + Math.random() * 8;
    this.phase     = Math.random() * Math.PI * 2;
    this.freq      = 0.3 + Math.random() * 1.2;
    this.rms = this.peak = this.peakHold = this.baseLevel;
    this.peakHoldTimer = this.clipTimer = 0;
    this.clip = false; this.silent = Math.random() < 0.15;
  }
  tick(dt) {
    if (this.silent) { this.rms = this.peak = this.peakHold = -60; this.clip = false; return; }
    this.phase += this.freq * dt * 2 * Math.PI;
    this.rms  = Math.max(-60, Math.min(0,  this.baseLevel + Math.sin(this.phase) * this.variance * 0.5 + (Math.random() - 0.5) * 4));
    this.peak = Math.max(-60, Math.min(3,  this.rms + 3 + Math.random() * 3));
    if (this.peak > this.peakHold) { this.peakHold = this.peak; this.peakHoldTimer = 2.5; }
    else { this.peakHoldTimer -= dt; if (this.peakHoldTimer <= 0) this.peakHold = Math.max(this.peakHold - 8 * dt, this.peak); }
    if (this.peak >= 0) { this.clip = true; this.clipTimer = 3; }
    else if (this.clipTimer > 0) { this.clipTimer -= dt; if (this.clipTimer <= 0) this.clip = false; }
  }
  toJSON() {
    return { rms: +this.rms.toFixed(1), peak: +this.peak.toFixed(1), peakHold: +this.peakHold.toFixed(1), clip: this.clip };
  }
}

function broadcastDeviceList() {
  const payload = JSON.stringify({ type: 'devices', data: [...devices.values()] });
  for (const ws of wss.clients) if (ws.readyState === ws.OPEN) ws.send(payload);
}

wss.on('connection', (ws) => {
  ws.subscribedCrossPoints = new Set();
  ws.send(JSON.stringify({ type: 'devices', data: [...devices.values()] }));
  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    if (msg.type === 'subscribe') {
      const key = `${msg.txDevice}:${msg.txChannel}:${msg.rxDevice}:${msg.rxChannel}`;
      ws.subscribedCrossPoints.add(key);
      if (!activeCrossPoints.has(key)) activeCrossPoints.set(key, new CrossPointState());
    }
    if (msg.type === 'unsubscribe') {
      ws.subscribedCrossPoints.delete(`${msg.txDevice}:${msg.txChannel}:${msg.rxDevice}:${msg.rxChannel}`);
    }
    if (msg.type === 'subscribeAll') {
      const txD = devices.get(msg.txDevice), rxD = devices.get(msg.rxDevice);
      if (!txD || !rxD) return;
      for (const tx of txD.txChannels) for (const rx of rxD.rxChannels) {
        const key = `${msg.txDevice}:${tx.id}:${msg.rxDevice}:${rx.id}`;
        ws.subscribedCrossPoints.add(key);
        if (!activeCrossPoints.has(key)) activeCrossPoints.set(key, new CrossPointState());
      }
    }
  });
  ws.on('close', () => {
    for (const key of ws.subscribedCrossPoints) {
      let has = false;
      for (const c of wss.clients) if (c !== ws && c.subscribedCrossPoints?.has(key)) { has = true; break; }
      if (!has) activeCrossPoints.delete(key);
    }
  });
});

let lastTick = Date.now();
setInterval(() => {
  const now = Date.now(), dt = (now - lastTick) / 1000; lastTick = now;
  if (!activeCrossPoints.size) return;
  for (const s of activeCrossPoints.values()) s.tick(dt);
  for (const ws of wss.clients) {
    if (ws.readyState !== ws.OPEN || !ws.subscribedCrossPoints?.size) continue;
    const levels = {};
    for (const key of ws.subscribedCrossPoints) { const s = activeCrossPoints.get(key); if (s) levels[key] = s.toJSON(); }
    ws.send(JSON.stringify({ type: 'levels', data: levels }));
  }
}, 40);

// ─── Start ───────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  const ifaces = getInterfaces();
  console.log(`\nDante VU Meter  →  http://localhost:${PORT}`);
  console.log('\nAvailable network interfaces:');
  for (const i of ifaces) console.log(`  ${i.name.padEnd(20)} ${i.address}`);
  console.log('\nSelect your Dante interface in the app to start discovery.\n');
});
