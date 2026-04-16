'use strict';

const express  = require('express');
const { WebSocketServer } = require('ws');
const http     = require('http');
const os       = require('os');
const path     = require('path');
const dgram    = require('dgram');
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
  if (devices.has(id) && devices.get(id).real) return;
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

// ─── Real device levels from Dante multicast ──────────────────────────────────
// Dante devices broadcast metering packets to 224.0.0.233:8708
// Packets contain an "Audinate" magic marker followed by TLV data with int16 levels
const DANTE_MULTICAST = '224.0.0.233';
const DANTE_PORT      = 8708;
const AUDINATE_MAGIC  = Buffer.from('Audinate');

// deviceLevels[sourceIP] = { channels: Float32Array, updatedAt: timestamp }
const deviceLevels = new Map();

let udpSocket   = null;
let udpIfaceAddr = null;

function rawToDbfs(raw) {
  // Raw values are Q15 unsigned linear amplitude (0–32767 ≈ 0 dBFS)
  // Negative raw values are clipped/overload indicators — treat as near 0 dBFS
  const abs = Math.abs(raw);
  if (abs < 1) return -60;
  return Math.max(-60, Math.min(3, 20 * Math.log10(abs / 32768)));
}

function parseAudinatePacket(msg, srcAddr) {
  const idx = msg.indexOf(AUDINATE_MAGIC);
  if (idx < 0) return;

  // Slice payload after "Audinate" magic
  const payload = msg.slice(idx + 8);
  if (payload.length < 4) return;

  // Read all int16 big-endian values from the payload
  const vals = [];
  for (let i = 0; i + 1 < payload.length; i += 2) {
    vals.push(payload.readInt16BE(i));
  }

  // Heuristic: find positive values that could be level readings.
  // From empirical analysis:
  //   Short packets (~30 vals): channel count at index 10, levels start at index 15
  //   Larger packets:           similar structure repeated per channel block
  //
  // We scan for a "channel count" marker at known offsets and extract levels.
  // Fallback: collect all clearly positive values as channel levels.
  let levels = extractLevels(vals);
  if (levels.length === 0) return;

  deviceLevels.set(srcAddr, { channels: levels, updatedAt: Date.now() });
}

function extractLevels(vals) {
  // Dante metering TLV structure (reverse-engineered):
  // After magic: [type(u8), len(u8), ...] repeated blocks
  // Each metering block: header bytes, then numChannels * 2 bytes of int16 level values
  //
  // Observed packet layout (int16 pairs after magic):
  //   [0]  type/flags
  //   [1]  sub-type
  //   [2]  0x1000 (4096) — block marker
  //   [3]  0
  //   [4]  payload length in bytes
  //   [5]  -32768 (0x8000) — flags
  //   [6]  4
  //   [7]  4
  //   [8]  sequence number (incrementing)
  //   [9]  0
  //   [10] numChannels (e.g. 16)
  //   [11] 0
  //   [12] channelBlock (1)
  //   [13] numChannels again
  //   [14] 6 (sub-block type)
  //   [15..15+numCh-1] level values
  //
  // For multi-block packets the pattern repeats.

  const levels = [];

  // Try to find the 0x1000 (4096) block marker and parse from there
  let i = 0;
  while (i < vals.length) {
    if (vals[i] === 4096 && i >= 2) {
      // Potential block start — numChannels is at offset +8 from the 4096
      const numCh = vals[i + 8];
      if (numCh > 0 && numCh <= 64 && i + 8 + numCh < vals.length) {
        // sub-block type 6 contains levels at i+12
        if (vals[i + 11] === 6 || vals[i + 12] === 6) {
          const start = (vals[i + 11] === 6) ? i + 12 : i + 13;
          for (let ch = 0; ch < numCh; ch++) {
            const raw = vals[start + ch];
            levels.push(rawToDbfs(raw));
          }
          i = start + numCh;
          continue;
        }
      }
    }
    i++;
  }

  // Fallback: if structure parse failed, grab all non-trivial positive values
  // that look like amplitude readings (between 100 and 32767)
  if (levels.length === 0) {
    for (const v of vals) {
      if (v > 100 && v < 32768) levels.push(rawToDbfs(v));
    }
  }

  return levels;
}

function startMulticastListener(ifaceAddr) {
  stopMulticastListener();
  udpIfaceAddr = ifaceAddr;

  udpSocket = dgram.createSocket({ type: 'udp4', reuseAddr: true });

  udpSocket.on('error', (err) => {
    console.error('[dante-meter] UDP error:', err.message);
  });

  udpSocket.on('message', (msg, rinfo) => {
    parseAudinatePacket(msg, rinfo.address);
  });

  udpSocket.bind(DANTE_PORT, () => {
    try {
      udpSocket.addMembership(DANTE_MULTICAST, ifaceAddr);
      console.log(`[dante-meter] Joined ${DANTE_MULTICAST}:${DANTE_PORT} on ${ifaceAddr}`);
    } catch (e) {
      // Try without interface binding (fallback)
      try {
        udpSocket.addMembership(DANTE_MULTICAST);
        console.log(`[dante-meter] Joined ${DANTE_MULTICAST}:${DANTE_PORT} (no interface bind)`);
      } catch (e2) {
        console.error('[dante-meter] addMembership failed:', e2.message);
      }
    }
  });
}

function stopMulticastListener() {
  if (udpSocket) {
    try {
      if (udpIfaceAddr) udpSocket.dropMembership(DANTE_MULTICAST, udpIfaceAddr);
      else               udpSocket.dropMembership(DANTE_MULTICAST);
    } catch {}
    udpSocket.close();
    udpSocket = null;
  }
  deviceLevels.clear();
}

// ─── dns-sd discovery ─────────────────────────────────────────────────────────
let browseProc    = null;
const resolveProcs = new Map();

function startDiscovery(ifaceName, ifaceAddr) {
  stopDiscovery();

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
      const m = line.match(/^\s*\d+:\d+:\d+\.\d+\s+Add\s+\S+\s+\S+\s+\S+\s+\S+\s+(.+)$/);
      if (m) resolveInstance(m[1].trim());
    }
  });

  browseProc.stderr.on('data', d => console.error('[dns-sd browse stderr]', d.toString().trim()));
  browseProc.on('error', e => console.error('[dns-sd browse error]', e.message));
  browseProc.on('close', code => console.log('[dns-sd browse] exited', code));

  // Start multicast listener on the same interface
  if (ifaceAddr) startMulticastListener(ifaceAddr);
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
    const m = buf.match(/\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\b/);
    if (m && m[1] !== '0.0.0.0') {
      const address = m[1];
      const name = instanceName.trim();
      const id   = name.toLowerCase().replace(/[^a-z0-9]+/g, '-');
      if (!devices.has(id) || !devices.get(id).real) {
        if (devices.has(id)) devices.delete(id);
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
  stopMulticastListener();
}

// ─── REST API ─────────────────────────────────────────────────────────────────
app.get('/api/interfaces', (_req, res) => {
  res.json(getInterfaces());
});

app.post('/api/discover', (req, res) => {
  const { interface: ifaceAddr } = req.body;
  if (!ifaceAddr) return res.status(400).json({ error: 'interface address required' });

  const ifaces   = getInterfaces();
  const iface    = ifaces.find(i => i.address === ifaceAddr);
  const ifaceName = iface ? iface.name : null;

  for (const [id, d] of devices) { if (d.real) devices.delete(id); }
  broadcastDeviceList();

  startDiscovery(ifaceName, ifaceAddr);
  res.json({ ok: true, interface: ifaceAddr, ifaceName });
});

// ─── WebSocket + level streaming ──────────────────────────────────────────────
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

// Real level state per cross point (when live device data is available)
class RealCrossPointState {
  constructor() {
    this.rms = this.peak = this.peakHold = -60;
    this.peakHoldTimer = 0;
    this.clipTimer = 0;
    this.clip = false;
  }

  // Update from a live dBFS level reading
  update(dbfs, dt) {
    this.rms  = Math.max(-60, Math.min(3, dbfs));
    this.peak = Math.max(-60, Math.min(3, dbfs + 2));  // slight peak above RMS

    if (this.peak > this.peakHold) {
      this.peakHold = this.peak;
      this.peakHoldTimer = 2.5;
    } else {
      this.peakHoldTimer -= dt;
      if (this.peakHoldTimer <= 0) this.peakHold = Math.max(this.peakHold - 8 * dt, this.peak);
    }

    if (this.peak >= 0) { this.clip = true; this.clipTimer = 3; }
    else if (this.clipTimer > 0) { this.clipTimer -= dt; if (this.clipTimer <= 0) this.clip = false; }
  }

  toJSON() {
    return { rms: +this.rms.toFixed(1), peak: +this.peak.toFixed(1), peakHold: +this.peakHold.toFixed(1), clip: this.clip, live: true };
  }
}

// Map: key → RealCrossPointState (used when live data exists)
const realCrossPoints = new Map();

function getLevelForCrossPoint(key) {
  // key format: txDevice:txChannel:rxDevice:rxChannel
  const [txDeviceId, txChannelStr] = key.split(':');
  const txDevice = devices.get(txDeviceId);
  if (!txDevice || !txDevice.real) return null;

  const txAddress = txDevice.address;
  const levels = deviceLevels.get(txAddress);
  if (!levels || Date.now() - levels.updatedAt > 2000) return null;  // stale

  const channelIdx = parseInt(txChannelStr, 10) - 1;
  if (channelIdx < 0 || channelIdx >= levels.channels.length) return null;

  return levels.channels[channelIdx];
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
      if (!has) { activeCrossPoints.delete(key); realCrossPoints.delete(key); }
    }
  });
});

let lastTick = Date.now();
setInterval(() => {
  const now = Date.now(), dt = (now - lastTick) / 1000; lastTick = now;
  if (!activeCrossPoints.size) return;

  for (const [key, state] of activeCrossPoints) {
    const liveLevel = getLevelForCrossPoint(key);
    if (liveLevel !== null) {
      // Use real level data
      if (!realCrossPoints.has(key)) realCrossPoints.set(key, new RealCrossPointState());
      realCrossPoints.get(key).update(liveLevel, dt);
    } else {
      state.tick(dt);
    }
  }

  for (const ws of wss.clients) {
    if (ws.readyState !== ws.OPEN || !ws.subscribedCrossPoints?.size) continue;
    const levels = {};
    for (const key of ws.subscribedCrossPoints) {
      const real = realCrossPoints.get(key);
      if (real) {
        levels[key] = real.toJSON();
      } else {
        const sim = activeCrossPoints.get(key);
        if (sim) levels[key] = sim.toJSON();
      }
    }
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
