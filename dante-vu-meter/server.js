'use strict';

const express   = require('express');
const { WebSocketServer } = require('ws');
const http      = require('http');
const os        = require('os');
const path      = require('path');
const mDNS      = require('multicast-dns');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ─── Network interface helpers ────────────────────────────────────────────────
function getInterfaces() {
  return Object.entries(os.networkInterfaces())
    .flatMap(([name, addrs]) =>
      (addrs || [])
        .filter(a => a.family === 'IPv4' && !a.internal)
        .map(a => ({ name, address: a.address, netmask: a.netmask }))
    );
}

// ─── Device registry ──────────────────────────────────────────────────────────
const devices = new Map();   // id → device

function registerDevice(id, name, address, numTx, numRx, real = false) {
  if (devices.has(id)) return;
  const txChannels = Array.from({ length: numTx }, (_, i) => ({ id: i + 1, name: `${name} Tx ${i + 1}` }));
  const rxChannels = Array.from({ length: numRx }, (_, i) => ({ id: i + 1, name: `${name} Rx ${i + 1}` }));
  devices.set(id, { id, name, address, txChannels, rxChannels, real });
  console.log(`[device] ${real ? 'Live' : 'Demo'}: ${name} @ ${address}`);
  broadcastDeviceList();
}

// Demo devices (always present so the UI isn't empty on first load)
const DEMO_DEVICES = [
  { id: 'console-1',  name: 'Console 1',   address: '192.168.1.10', tx: 16, rx: 16 },
  { id: 'stagebox-a', name: 'Stage Box A', address: '192.168.1.20', tx: 16, rx: 8  },
  { id: 'ioa-rack',   name: 'I/O Rack',    address: '192.168.1.30', tx: 8,  rx: 8  },
  { id: 'dvs-pc',     name: 'DVS (PC)',     address: '192.168.1.40', tx: 8,  rx: 8  },
];
for (const d of DEMO_DEVICES) registerDevice(d.id, d.name, d.address, d.tx, d.rx, false);

// ─── mDNS discovery ───────────────────────────────────────────────────────────
// Dante devices advertise these service types via mDNS (Zeroconf / Bonjour):
//   _netaudio-arc._udp  – Audinate Remote Control
//   _netaudio-dbc._udp  – Dante Browse & Connect
const DANTE_SERVICES = ['_netaudio-arc._udp.local', '_netaudio-dbc._udp.local'];

let mdnsInstance   = null;   // current multicast-dns instance
let queryInterval  = null;   // repeating PTR query timer
let currentIface   = null;   // currently selected interface address

// Pending SRV/A lookups: name → { service, host, port }
const pendingSRV = new Map();
const pendingA   = new Map();  // hostname → device name

function startDiscovery(ifaceAddress) {
  stopDiscovery();
  currentIface = ifaceAddress;
  console.log(`[mDNS] Starting discovery on interface ${ifaceAddress}`);

  mdnsInstance = mDNS({ interface: ifaceAddress, reuseAddr: true });

  mdnsInstance.on('response', (response) => {
    // Collect all records from all sections
    const records = [
      ...(response.answers   || []),
      ...(response.additionals || []),
    ];

    // --- PTR records tell us a service instance name exists ---
    for (const r of records) {
      if (r.type !== 'PTR') continue;
      if (!DANTE_SERVICES.some(s => r.name === s)) continue;
      const instanceName = r.data; // e.g. "My-Dante-Device._netaudio-arc._udp.local"
      if (!pendingSRV.has(instanceName)) {
        pendingSRV.set(instanceName, { name: instanceName });
      }
    }

    // --- SRV records give us hostname + port ---
    for (const r of records) {
      if (r.type !== 'SRV') continue;
      const entry = pendingSRV.get(r.name);
      if (!entry) continue;
      entry.host = r.data.target;
      entry.port = r.data.port;
      // Friendly device name = first label of instance name, de-escaped
      entry.friendlyName = r.name.split('.')[0].replace(/_/g, ' ').replace(/\\032/g, ' ');
    }

    // --- A records give us the IP address ---
    for (const r of records) {
      if (r.type !== 'A') continue;
      // Match against pending SRV host fields
      for (const [, entry] of pendingSRV) {
        if (entry.host && (entry.host === r.name || entry.host.replace(/\.$/, '') === r.name.replace(/\.$/, ''))) {
          if (!entry.address) {
            entry.address = r.data;
          }
        }
      }
      // Also store for later lookup
      pendingA.set(r.name, r.data);
    }

    // --- Flush any complete entries ---
    for (const [key, entry] of pendingSRV) {
      const address = entry.address || pendingA.get(entry.host) || pendingA.get((entry.host || '').replace(/\.$/, '') + '.');
      if (!address) continue;

      const name = entry.friendlyName || entry.name.split('.')[0];
      const id   = name.toLowerCase().replace(/[^a-z0-9]+/g, '-');

      if (!devices.has(id) || !devices.get(id).real) {
        // Remove any demo device with same id
        if (devices.has(id) && !devices.get(id).real) devices.delete(id);

        const numTx = 2 + Math.floor(Math.random() * 14);
        const numRx = 2 + Math.floor(Math.random() * 14);
        registerDevice(id, name, address, numTx, numRx, true);
      }
      pendingSRV.delete(key);
    }
  });

  mdnsInstance.on('error', (err) => {
    console.error('[mDNS] Error:', err.message);
  });

  // Send PTR queries immediately and then every 5 s
  function query() {
    for (const service of DANTE_SERVICES) {
      mdnsInstance.query({ questions: [{ name: service, type: 'PTR' }] });
    }
  }
  query();
  queryInterval = setInterval(query, 5000);
}

function stopDiscovery() {
  if (queryInterval) { clearInterval(queryInterval); queryInterval = null; }
  if (mdnsInstance)  { mdnsInstance.destroy(); mdnsInstance = null; }
  pendingSRV.clear();
  pendingA.clear();
}

// ─── REST API ─────────────────────────────────────────────────────────────────
app.get('/api/interfaces', (_req, res) => {
  res.json(getInterfaces());
});

app.post('/api/discover', (req, res) => {
  const { interface: iface } = req.body;
  if (!iface) return res.status(400).json({ error: 'interface address required' });
  // Remove all live (real) devices so we get a fresh discovery
  for (const [id, d] of devices) { if (d.real) devices.delete(id); }
  broadcastDeviceList();
  startDiscovery(iface);
  res.json({ ok: true, interface: iface });
});

// ─── WebSocket ────────────────────────────────────────────────────────────────
const activeCrossPoints = new Map();

class CrossPointState {
  constructor() {
    this.baseLevel   = -18 - Math.random() * 20;
    this.variance    = 4   + Math.random() * 8;
    this.phase       = Math.random() * Math.PI * 2;
    this.freq        = 0.3 + Math.random() * 1.2;
    this.rms         = this.baseLevel;
    this.peak        = this.baseLevel;
    this.peakHold    = this.baseLevel;
    this.peakHoldTimer = 0;
    this.clip        = false;
    this.clipTimer   = 0;
    this.silent      = Math.random() < 0.15;
  }
  tick(dt) {
    if (this.silent) { this.rms = this.peak = this.peakHold = -60; this.clip = false; return; }
    this.phase += this.freq * dt * 2 * Math.PI;
    const mod   = Math.sin(this.phase) * this.variance * 0.5;
    const noise = (Math.random() - 0.5) * 4;
    this.rms  = Math.max(-60, Math.min(0, this.baseLevel + mod + noise));
    this.peak = Math.max(-60, Math.min(3, this.rms + 3 + Math.random() * 3));
    if (this.peak > this.peakHold) {
      this.peakHold = this.peak; this.peakHoldTimer = 2.5;
    } else {
      this.peakHoldTimer -= dt;
      if (this.peakHoldTimer <= 0) this.peakHold = Math.max(this.peakHold - 8 * dt, this.peak);
    }
    if (this.peak >= 0) { this.clip = true; this.clipTimer = 3; }
    else if (this.clipTimer > 0) { this.clipTimer -= dt; if (this.clipTimer <= 0) this.clip = false; }
  }
  toJSON() {
    return { rms: +this.rms.toFixed(1), peak: +this.peak.toFixed(1), peakHold: +this.peakHold.toFixed(1), clip: this.clip };
  }
}

function broadcastDeviceList() {
  const payload = JSON.stringify({
    type: 'devices',
    data: [...devices.values()],
  });
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
      let hasOther = false;
      for (const c of wss.clients) if (c !== ws && c.subscribedCrossPoints?.has(key)) { hasOther = true; break; }
      if (!hasOther) activeCrossPoints.delete(key);
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
  console.log('\nSelect your Dante interface in the app to start discovery.');
});
