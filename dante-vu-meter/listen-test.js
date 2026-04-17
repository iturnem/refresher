'use strict';
// Dante multicast level sniffer — run while varying audio to find responsive fields
// Usage: node listen-test.js <interface-ip>   e.g.  node listen-test.js 10.77.200.1

const dgram = require('dgram');

const DANTE_MULTICAST = '224.0.0.233';
const DANTE_PORT      = 8708;
const AUDINATE_MAGIC  = Buffer.from('Audinate');

const ifaceAddr = process.argv[2] || '0.0.0.0';
console.log(`Listening on ${DANTE_MULTICAST}:${DANTE_PORT} (iface ${ifaceAddr})`);
console.log('Watch for lines marked [CHANGED] — those indices respond to audio.');
console.log('---');

// Per-source: last int16 array for change detection
const lastVals = new Map();
// Per-source: packet count
const pktCount = new Map();

const sock = dgram.createSocket({ type: 'udp4', reuseAddr: true });
sock.bind(DANTE_PORT, () => {
  try {
    sock.addMembership(DANTE_MULTICAST, ifaceAddr);
  } catch {
    sock.addMembership(DANTE_MULTICAST);
  }
});

sock.on('message', (msg, rinfo) => {
  const src = rinfo.address;
  const idx = msg.indexOf(AUDINATE_MAGIC);
  if (idx < 0) return;

  const payload = msg.slice(idx + 8);
  if (payload.length < 4) return;

  const vals = [];
  for (let i = 0; i + 1 < payload.length; i += 2) {
    vals.push(payload.readInt16BE(i));
  }

  const prev  = lastVals.get(src);
  const count = (pktCount.get(src) || 0) + 1;
  pktCount.set(src, count);
  lastVals.set(src, vals);

  // Find changed indices relative to previous packet
  const changed = [];
  if (prev && prev.length === vals.length) {
    for (let i = 0; i < vals.length; i++) {
      if (vals[i] !== prev[i]) changed.push(i);
    }
  }

  // Print header
  const changedNote = changed.length ? ` [CHANGED: ${changed.join(',')}]` : '';
  console.log(`\n${src} pkt#${count} (${vals.length} vals)${changedNote}`);

  // Print values in groups of 8 with indices
  for (let i = 0; i < vals.length; i += 8) {
    const slice = vals.slice(i, i + 8);
    const parts = slice.map((v, off) => {
      const isChanged = changed.includes(i + off);
      const formatted = String(v).padStart(7);
      return isChanged ? `*${formatted}` : ` ${formatted}`;
    });
    const lineIdx = String(i).padStart(3);
    console.log(`  [${lineIdx}] ${parts.join(' ')}`);
  }

  // If large packet, also show potential level blocks around 4096 markers
  const markers = [];
  for (let i = 0; i < vals.length; i++) {
    if (vals[i] === 4096) markers.push(i);
  }
  if (markers.length) {
    console.log(`  4096 markers at indices: ${markers.join(', ')}`);
  }

  // Show signed dBFS interpretation for all changed values
  if (changed.length && prev) {
    const dbfsLines = changed
      .map(i => {
        const v = vals[i];
        const p = prev[i];
        let interp = '';
        if (v !== 0) {
          const dbfs = v / 256;
          if (dbfs >= -80 && dbfs <= 6) interp = ` = ${dbfs.toFixed(1)} dBFS(×256)`;
          else if (v > 0 && v <= 32767) {
            const amp = 20 * Math.log10(v / 32768);
            if (amp >= -80) interp = ` = ${amp.toFixed(1)} dBFS(amp)`;
          }
        }
        return `    idx[${i}]: ${p} → ${v}${interp}`;
      });
    if (dbfsLines.length) {
      console.log('  Changes:');
      dbfsLines.forEach(l => console.log(l));
    }
  }
});

sock.on('error', e => console.error('UDP error:', e.message));
