'use strict';

// ---------------------------------------------------------------------------
// 433-mqtt-bridge dashboard
//
// Initial state comes from the REST API; live deltas arrive over the WebSocket.
// Sensor/receiver cards are rendered once and then updated in place so the
// uPlot sparklines (and DOM identity) are preserved across updates.
// ---------------------------------------------------------------------------

const FRESH_S = 5 * 60;       // green: seen within 5 min
const STALE_S = 60 * 60;      // amber: not seen for 1 h
const HISTORY_S = 24 * 3600;  // sparkline window
const MAX_POINTS = 600;       // cap per sparkline
const MAX_FEED = 200;         // cap raw-feed lines

const sensorCards = new Map();   // key -> { el, refs, chart, xs, ys, primaryTopic, lastSeen }
const receiverCards = new Map(); // name -> { el, refs }

// --- helpers ---------------------------------------------------------------

function el(tag, cls, text) {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text !== undefined) node.textContent = text;
  return node;
}

function fmtAge(seconds) {
  if (seconds == null || !isFinite(seconds)) return 'never';
  seconds = Math.max(0, Math.floor(seconds));
  if (seconds < 60) return seconds + 's ago';
  if (seconds < 3600) return Math.floor(seconds / 60) + 'm ago';
  if (seconds < 86400) return Math.floor(seconds / 3600) + 'h ago';
  return Math.floor(seconds / 86400) + 'd ago';
}

function fmtValue(v) {
  if (typeof v === 'number') return Number.isInteger(v) ? String(v) : v.toFixed(2);
  return String(v);
}

function shortTopic(topic, key) {
  return topic.startsWith(key + '/') ? topic.slice(key.length + 1) : topic;
}

async function getJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(url + ' -> ' + r.status);
  return r.json();
}

// --- sensors ---------------------------------------------------------------

function pickPrimaryTopic(rows) {
  // Choose the numeric topic with the most points, preferring temperature.
  const counts = new Map();
  for (const row of rows) {
    if (row.value_num == null) continue;
    counts.set(row.topic, (counts.get(row.topic) || 0) + 1);
  }
  let best = null, bestScore = -1;
  for (const [topic, count] of counts) {
    const score = count + (/temperature/i.test(topic) ? 100000 : 0);
    if (score > bestScore) { best = topic; bestScore = score; }
  }
  return best;
}

function makeChart(container) {
  const opts = {
    width: container.clientWidth || 260,
    height: 70,
    cursor: { show: true, points: { show: true } },
    legend: { show: false },
    scales: { x: { time: true } },
    axes: [
      { show: false },
      { size: 34, grid: { show: false }, ticks: { show: false },
        font: '10px system-ui', stroke: '#6b7480' },
    ],
    series: [
      {},
      { stroke: '#2f6fed', width: 1.5, points: { show: false } },
    ],
  };
  return new uPlot(opts, [[], []], container);
}

function createSensorCard(sensor) {
  const card = el('div', 'card');

  const head = el('div', 'card-head');
  const titleWrap = el('div', 'card-title');
  const dot = el('span', 'dot');
  titleWrap.appendChild(dot);
  titleWrap.appendChild(document.createTextNode(' ' + sensor.key));
  head.appendChild(titleWrap);
  head.appendChild(el('span', 'card-type', sensor.type));
  card.appendChild(head);

  const ident = Object.entries(sensor.identifier).map(([k, v]) => `${k}=${v}`).join(', ');
  card.appendChild(el('div', 'card-ident', ident));

  const readings = el('div', 'readings');
  card.appendChild(readings);

  const meta = el('div', 'meta');
  const age = el('span'); const rate = el('span'); const battery = el('span'); const signal = el('span');
  meta.append(age, rate, battery, signal);
  card.appendChild(meta);

  const spark = el('div', 'spark');
  card.appendChild(spark);

  document.getElementById('sensors').appendChild(card);

  const entry = {
    el: card,
    refs: { dot, readings, age, rate, battery, signal, spark },
    chart: null, xs: [], ys: [], primaryTopic: null, lastSeen: null,
  };
  sensorCards.set(sensor.key, entry);

  if (sensor.stats) updateSensorStats(sensor.key, sensor.stats);
  loadSensorHistory(sensor.key);
  return entry;
}

async function loadSensorHistory(key) {
  const entry = sensorCards.get(key);
  if (!entry) return;
  let rows;
  try {
    const since = Date.now() / 1000 - HISTORY_S;
    rows = await getJSON('/api/sensors/history?key=' + encodeURIComponent(key) + '&since=' + since);
  } catch (e) { return; }

  const topic = pickPrimaryTopic(rows);
  if (!topic) return; // nothing numeric to plot (e.g. door/button)

  entry.primaryTopic = topic;
  entry.xs = rows.filter(r => r.topic === topic && r.value_num != null).map(r => r.ts);
  entry.ys = rows.filter(r => r.topic === topic && r.value_num != null).map(r => r.value_num);
  if (!entry.chart) entry.chart = makeChart(entry.refs.spark);
  entry.chart.setData([entry.xs, entry.ys]);
}

function updateSensorStats(key, s) {
  const entry = sensorCards.get(key);
  if (!entry || !s) return;
  const r = entry.refs;

  r.readings.innerHTML = '';
  for (const [topic, value] of Object.entries(s.last_readings || {})) {
    const item = el('span', 'reading');
    item.appendChild(el('span', 'rk', shortTopic(topic, key) + ' '));
    item.appendChild(el('b', null, fmtValue(value)));
    r.readings.appendChild(item);
  }

  entry.lastSeen = s.last_seen ? Date.parse(s.last_seen) : null;
  r.rate.textContent = (s.rate_per_min || 0) + '/min';
  r.battery.textContent = s.battery_ok == null ? '' : (s.battery_ok ? '🔋 ok' : '🪫 low');
  r.signal.textContent = s.rssi == null ? '' : ('📶 ' + s.rssi.toFixed(0) + ' dBm');
  refreshAge(key);
}

function appendSensorPoint(key, readings, tsIso) {
  const entry = sensorCards.get(key);
  if (!entry || !entry.primaryTopic) return;
  if (!(entry.primaryTopic in readings)) return;
  const value = readings[entry.primaryTopic];
  if (typeof value !== 'number') return;

  entry.xs.push(Date.parse(tsIso) / 1000);
  entry.ys.push(value);
  if (entry.xs.length > MAX_POINTS) { entry.xs.shift(); entry.ys.shift(); }
  if (!entry.chart) entry.chart = makeChart(entry.refs.spark);
  entry.chart.setData([entry.xs, entry.ys]);
}

function refreshAge(key) {
  const entry = sensorCards.get(key);
  if (!entry) return;
  const r = entry.refs;
  if (entry.lastSeen == null) {
    r.age.textContent = 'not seen';
    r.age.className = 'age-offline';
    entry.el.classList.add('offline'); entry.el.classList.remove('stale');
    r.dot.className = 'dot dot-bad';
    return;
  }
  const seconds = (Date.now() - entry.lastSeen) / 1000;
  r.age.textContent = fmtAge(seconds);
  entry.el.classList.remove('stale', 'offline');
  if (seconds < FRESH_S) { r.dot.className = 'dot dot-good'; r.age.className = ''; }
  else if (seconds < STALE_S) { r.dot.className = 'dot dot-warn'; r.age.className = 'age-stale'; entry.el.classList.add('stale'); }
  else { r.dot.className = 'dot dot-bad'; r.age.className = 'age-offline'; entry.el.classList.add('offline'); }
}

// --- receivers -------------------------------------------------------------

function createReceiverCard(rec) {
  const card = el('div', 'card');

  const head = el('div', 'card-head');
  const titleWrap = el('div', 'card-title');
  const dot = el('span', 'dot');
  titleWrap.appendChild(dot);
  titleWrap.appendChild(document.createTextNode(' ' + rec.name));
  head.appendChild(titleWrap);

  const restart = el('button', null, 'Restart');
  restart.addEventListener('click', () => restartReceiver(rec.name, restart));
  head.appendChild(restart);
  card.appendChild(head);

  card.appendChild(el('div', 'rec-args', rec.arguments));

  const meta = el('div', 'meta');
  const status = el('span'); const count = el('span'); const rate = el('span'); const age = el('span'); const signal = el('span');
  meta.append(status, count, rate, age, signal);
  card.appendChild(meta);

  document.getElementById('receivers').appendChild(card);

  const entry = { el: card, refs: { dot, status, count, rate, age, signal } };
  receiverCards.set(rec.name, entry);
  updateReceiverStats(rec.name, rec);
  return entry;
}

function updateReceiverStats(name, s) {
  const entry = receiverCards.get(name);
  if (!entry) return;
  const r = entry.refs;
  const running = !!s.running;
  r.dot.className = 'dot ' + (running ? 'dot-good' : 'dot-bad');
  r.status.textContent = running ? 'running' : 'stopped';
  r.count.textContent = (s.packet_count || 0) + ' pkts';
  r.rate.textContent = (s.rate_per_min || 0) + '/min';
  if (s.restart_count) r.status.textContent += ` (${s.restart_count} restarts)`;
  r.age.textContent = s.last_seen ? fmtAge((Date.now() - Date.parse(s.last_seen)) / 1000) : 'no packets';
  r.signal.textContent = s.avg_rssi == null ? '' : ('📶 ' + s.avg_rssi.toFixed(0) + ' dBm');
}

async function restartReceiver(name, button) {
  button.disabled = true;
  button.textContent = 'Restarting…';
  try {
    await fetch('/api/receivers/' + encodeURIComponent(name) + '/restart', { method: 'POST' });
  } catch (e) { /* ignore */ }
  setTimeout(() => { button.disabled = false; button.textContent = 'Restart'; }, 1500);
}

// --- raw feed --------------------------------------------------------------

const feedEl = document.getElementById('feed');
const feedFilter = document.getElementById('feed-filter');
const feedPause = document.getElementById('feed-pause');
document.getElementById('feed-clear').addEventListener('click', () => { feedEl.innerHTML = ''; });

function addFeedLine(ev) {
  if (feedPause.checked) return;
  const json = JSON.stringify(ev.data);
  const tag = ev.ignored ? 'ignored' : (ev.sensor ? 'known' : 'unknown');
  const label = ev.ignored ? 'IGNORED' : (ev.sensor ? ev.sensor : 'UNKNOWN');

  const filter = feedFilter.value.trim().toLowerCase();
  if (filter && !(json + ' ' + ev.receiver + ' ' + label).toLowerCase().includes(filter)) return;

  const line = el('div', 'line');
  line.appendChild(el('span', 'ts', new Date(ev.time).toLocaleTimeString() + ' '));
  line.appendChild(el('span', 'rcv', ev.receiver + ' '));
  line.appendChild(el('span', 'tag tag-' + tag, label));
  line.appendChild(document.createTextNode(' ' + json));

  const atBottom = feedEl.scrollHeight - feedEl.scrollTop - feedEl.clientHeight < 40;
  feedEl.appendChild(line);
  while (feedEl.childElementCount > MAX_FEED) feedEl.removeChild(feedEl.firstChild);
  if (atBottom) feedEl.scrollTop = feedEl.scrollHeight;
}

// --- status pills ----------------------------------------------------------

function setWsStatus(ok) {
  const pill = document.getElementById('ws-status');
  pill.textContent = 'WebSocket: ' + (ok ? 'connected' : 'disconnected');
  pill.className = 'pill ' + (ok ? 'pill-good' : 'pill-bad');
}

function setMqttStatus(s) {
  const pill = document.getElementById('mqtt-status');
  pill.textContent = `MQTT: ${s.connected ? 'connected' : 'down'} · ${s.broker} · ${s.published} published`;
  pill.className = 'pill ' + (s.connected ? 'pill-good' : 'pill-bad');
}

// --- websocket -------------------------------------------------------------

function connectWs() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const socket = new WebSocket(`${proto}://${location.host}/ws`);

  socket.onopen = () => setWsStatus(true);
  socket.onclose = () => { setWsStatus(false); setTimeout(connectWs, 2000); };
  socket.onerror = () => socket.close();
  socket.onmessage = (msg) => handleEvent(JSON.parse(msg.data));
}

function handleEvent(ev) {
  switch (ev.type) {
    case 'packet':
      addFeedLine(ev);
      break;
    case 'reading':
      if (ev.snapshot) updateSensorStats(ev.sensor, ev.snapshot);
      appendSensorPoint(ev.sensor, ev.readings || {}, ev.time);
      break;
    case 'receiver_status':
      if (ev.name) updateReceiverStats(ev.name, ev);
      break;
    case 'unknown':
      // Surfaced in the raw feed already; dedicated unknown UI lands in a later phase.
      break;
  }
}

// --- init ------------------------------------------------------------------

async function refreshStatus() {
  try { setMqttStatus((await getJSON('/api/status')).mqtt); } catch (e) { /* ignore */ }
}

async function refreshStats() {
  // Rates and last-seen decay over time; periodically re-pull so idle cards don't
  // show a stale rate even when no events are arriving.
  try {
    for (const s of await getJSON('/api/sensors')) if (s.stats) updateSensorStats(s.key, s.stats);
    for (const r of await getJSON('/api/receivers')) updateReceiverStats(r.name, r);
  } catch (e) { /* ignore */ }
}

async function init() {
  try {
    const [sensors, receivers] = await Promise.all([getJSON('/api/sensors'), getJSON('/api/receivers')]);
    if (!receivers.length) document.getElementById('receivers').appendChild(el('div', 'empty', 'No receivers configured.'));
    receivers.forEach(createReceiverCard);
    if (!sensors.length) document.getElementById('sensors').appendChild(el('div', 'empty', 'No sensors configured.'));
    sensors.forEach(createSensorCard);
  } catch (e) {
    console.error('init failed', e);
  }

  await refreshStatus();
  connectWs();

  setInterval(() => { for (const key of sensorCards.keys()) refreshAge(key); }, 1000);
  setInterval(refreshStatus, 3000);
  setInterval(refreshStats, 5000);

  window.addEventListener('resize', () => {
    for (const entry of sensorCards.values()) {
      if (entry.chart) entry.chart.setSize({ width: entry.refs.spark.clientWidth || 260, height: 70 });
    }
  });
}

init();
