'use strict';

const express = require('express');
const { WebSocketServer } = require('ws');
const http = require('http');
const dgram = require('dgram');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static(path.join(__dirname, 'public')));

// ─── Dante device registry ────────────────────────────────────────────────────
// Map of deviceId -> { name, address, txChannels: [], rxChannels: [] }
const devices = new Map();
// Map of `${txDevice}:${txCh}:${rxDevice}:${rxCh}` -> { levels, peak, ... }
const crossPoints = new Map();

// ─── mDNS discovery ───────────────────────────────────────────────────────────
let mdns;
try {
  mdns = require('mdns-js');
  mdns.excludeInterface('0.0.0.0');

  const browser = mdns.createBrowser(
    mdns.tcp('netaudio-arc'),   // Dante ARC service
    mdns.udp('netaudio-arc'),
    mdns.tcp('netaudio-dbc'),   // Dante Browse & Connect
  );

  browser.on('ready', () => browser.discover());

  browser.on('update', (data) => {
    const name = (data.fullname || data.host || '').replace(/\._netaudio.*/, '');
    const address = data.addresses && data.addresses[0];
    if (!name || !address) return;

    const id = name.toLowerCase().replace(/\s+/g, '-');
    if (!devices.has(id)) {
      const numTx = 2 + Math.floor(Math.random() * 14); // 2-16 tx channels
      const numRx = 2 + Math.floor(Math.random() * 14);
      registerDevice(id, name, address, numTx, numRx, true);
      console.log(`[mDNS] Found Dante device: ${name} @ ${address}`);
      broadcastDeviceList();
    }
  });
} catch (e) {
  console.warn('[mDNS] Discovery unavailable:', e.message);
}

function registerDevice(id, name, address, numTx, numRx, real = false) {
  const txChannels = Array.from({ length: numTx }, (_, i) => ({
    id: i + 1,
    name: `${name} Tx ${i + 1}`,
  }));
  const rxChannels = Array.from({ length: numRx }, (_, i) => ({
    id: i + 1,
    name: `${name} Rx ${i + 1}`,
  }));
  devices.set(id, { id, name, address, txChannels, rxChannels, real });
}

// ─── Simulated devices (always present for demo) ──────────────────────────────
const DEMO_DEVICES = [
  { id: 'console-1', name: 'Console 1',        address: '192.168.1.10', tx: 16, rx: 16 },
  { id: 'stagebox-a', name: 'Stage Box A',     address: '192.168.1.20', tx: 16, rx: 8  },
  { id: 'ioa-rack',   name: 'I/O Rack',        address: '192.168.1.30', tx: 8,  rx: 8  },
  { id: 'dvs-pc',     name: 'DVS (PC)',         address: '192.168.1.40', tx: 8,  rx: 8  },
];
for (const d of DEMO_DEVICES) registerDevice(d.id, d.name, d.address, d.tx, d.rx, false);

// ─── Cross-point level simulation ─────────────────────────────────────────────
// Each active cross-point gets a simulated signal with:
//   rms  – current RMS level (dBFS, −60..0)
//   peak – short-term peak hold (dBFS)
//   clip – clip indicator
const activeCrossPoints = new Map(); // key -> CrossPointState

class CrossPointState {
  constructor(key) {
    this.key = key;
    // Randomise signal character per cross-point
    this.baseLevel = -18 - Math.random() * 20; // −18 to −38 dBFS nominal
    this.variance  = 4 + Math.random() * 8;    // dynamic range of signal
    this.phase     = Math.random() * Math.PI * 2;
    this.freq      = 0.3 + Math.random() * 1.2; // Hz of slow modulation
    this.rms       = this.baseLevel;
    this.peak      = this.baseLevel;
    this.peakHold  = this.baseLevel;
    this.peakHoldTimer = 0;
    this.clip      = false;
    this.clipTimer = 0;
    this.silent    = Math.random() < 0.15; // 15% chance of silent channel
  }

  tick(dt) {
    if (this.silent) {
      this.rms  = -60;
      this.peak = -60;
      this.peakHold = -60;
      this.clip = false;
      return;
    }

    this.phase += this.freq * dt * 2 * Math.PI;
    const mod = Math.sin(this.phase) * this.variance * 0.5;
    const noise = (Math.random() - 0.5) * 4;
    this.rms = Math.max(-60, Math.min(0, this.baseLevel + mod + noise));

    // Peak is a few dB above RMS
    const instantPeak = this.rms + 3 + Math.random() * 3;
    this.peak = Math.max(-60, Math.min(3, instantPeak));

    // Peak hold
    if (this.peak > this.peakHold) {
      this.peakHold = this.peak;
      this.peakHoldTimer = 2.5; // hold for 2.5s
    } else {
      this.peakHoldTimer -= dt;
      if (this.peakHoldTimer <= 0) {
        this.peakHold = Math.max(this.peakHold - 8 * dt, this.peak);
      }
    }

    // Clip
    if (this.peak >= 0) {
      this.clip = true;
      this.clipTimer = 3;
    } else if (this.clipTimer > 0) {
      this.clipTimer -= dt;
      if (this.clipTimer <= 0) this.clip = false;
    }
  }

  toJSON() {
    return {
      rms:      parseFloat(this.rms.toFixed(1)),
      peak:     parseFloat(this.peak.toFixed(1)),
      peakHold: parseFloat(this.peakHold.toFixed(1)),
      clip:     this.clip,
    };
  }
}

// ─── WebSocket handling ───────────────────────────────────────────────────────
function broadcastDeviceList() {
  const payload = JSON.stringify({
    type: 'devices',
    data: [...devices.values()].map(d => ({
      id:         d.id,
      name:       d.name,
      address:    d.address,
      real:       d.real,
      txChannels: d.txChannels,
      rxChannels: d.rxChannels,
    })),
  });
  for (const ws of wss.clients) {
    if (ws.readyState === ws.OPEN) ws.send(payload);
  }
}

wss.on('connection', (ws) => {
  // Send current device list immediately
  ws.send(JSON.stringify({
    type: 'devices',
    data: [...devices.values()].map(d => ({
      id:         d.id,
      name:       d.name,
      address:    d.address,
      real:       d.real,
      txChannels: d.txChannels,
      rxChannels: d.rxChannels,
    })),
  }));

  // Send list of active cross-points for this client
  ws.subscribedCrossPoints = new Set();

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'subscribe') {
      // { type:'subscribe', txDevice, txChannel, rxDevice, rxChannel }
      const key = `${msg.txDevice}:${msg.txChannel}:${msg.rxDevice}:${msg.rxChannel}`;
      ws.subscribedCrossPoints.add(key);
      if (!activeCrossPoints.has(key)) {
        activeCrossPoints.set(key, new CrossPointState(key));
      }
    }

    if (msg.type === 'unsubscribe') {
      const key = `${msg.txDevice}:${msg.txChannel}:${msg.rxDevice}:${msg.rxChannel}`;
      ws.subscribedCrossPoints.delete(key);
    }

    if (msg.type === 'subscribeAll') {
      // Subscribe to all tx channels for a given device pair
      const txDev = devices.get(msg.txDevice);
      const rxDev = devices.get(msg.rxDevice);
      if (!txDev || !rxDev) return;
      for (const tx of txDev.txChannels) {
        for (const rx of rxDev.rxChannels) {
          const key = `${msg.txDevice}:${tx.id}:${msg.rxDevice}:${rx.id}`;
          ws.subscribedCrossPoints.add(key);
          if (!activeCrossPoints.has(key)) {
            activeCrossPoints.set(key, new CrossPointState(key));
          }
        }
      }
    }
  });

  ws.on('close', () => {
    // Clean up cross-points that have no more subscribers
    for (const key of ws.subscribedCrossPoints) {
      let hasOther = false;
      for (const other of wss.clients) {
        if (other !== ws && other.subscribedCrossPoints && other.subscribedCrossPoints.has(key)) {
          hasOther = true; break;
        }
      }
      if (!hasOther) activeCrossPoints.delete(key);
    }
  });
});

// ─── Meter broadcast loop (25 fps) ────────────────────────────────────────────
let lastTick = Date.now();
setInterval(() => {
  const now = Date.now();
  const dt  = (now - lastTick) / 1000;
  lastTick  = now;

  if (activeCrossPoints.size === 0) return;

  // Tick all active states
  for (const state of activeCrossPoints.values()) state.tick(dt);

  // Build per-client payloads (only send what they subscribed to)
  for (const ws of wss.clients) {
    if (ws.readyState !== ws.OPEN || !ws.subscribedCrossPoints || ws.subscribedCrossPoints.size === 0) continue;

    const levels = {};
    for (const key of ws.subscribedCrossPoints) {
      const state = activeCrossPoints.get(key);
      if (state) levels[key] = state.toJSON();
    }

    ws.send(JSON.stringify({ type: 'levels', data: levels }));
  }
}, 40); // ~25 fps

// ─── Start ───────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Dante VU Meter server running on http://localhost:${PORT}`);
  console.log('Demo devices loaded. mDNS discovery active (if network available).');
});
