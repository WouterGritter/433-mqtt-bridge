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

function fmtDuration(seconds) {
  // A bare duration (no "ago"), used for the average interval between messages.
  if (seconds == null || !isFinite(seconds)) return null;
  seconds = Math.max(0, Math.round(seconds));
  if (seconds < 60) return seconds + 's';
  if (seconds < 3600) return Math.round(seconds / 60) + 'm';
  if (seconds < 86400) return Math.round(seconds / 3600) + 'h';
  return Math.round(seconds / 86400) + 'd';
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
    // Note: do not set cursor.points.show to a boolean — uPlot expects it to be a
    // function returning the point DOM element, so the default must be left in place.
    cursor: { show: true },
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

  const sources = el('div', 'sources-line muted');
  card.appendChild(sources);

  const spark = el('div', 'spark');
  card.appendChild(spark);

  const actions = el('div', 'card-actions');
  const editBtn = el('button', 'small', 'Edit');
  const delBtn = el('button', 'small danger', 'Delete');
  editBtn.addEventListener('click', () => openSensorModal(sensor));
  delBtn.addEventListener('click', () => deleteSensor(sensor));
  actions.append(editBtn, delBtn);
  card.appendChild(actions);

  document.getElementById('sensors').appendChild(card);

  const entry = {
    el: card,
    refs: { dot, readings, age, rate, battery, signal, sources, spark },
    chart: null, xs: [], ys: [], primaryTopic: null, lastSeen: null, avgInterval: null, sourcesData: [],
  };
  sensorCards.set(sensor.key, entry);

  if (sensor.stats) updateSensorStats(sensor.key, sensor.stats);
  loadSensorHistory(sensor.key);
  return entry;
}

function renderSensors(sensors) {
  for (const entry of sensorCards.values()) {
    if (entry.chart) entry.chart.destroy();
  }
  sensorCards.clear();
  const grid = document.getElementById('sensors');
  grid.innerHTML = '';
  if (!sensors.length) grid.appendChild(el('div', 'empty', 'No sensors configured.'));
  sensors.forEach(createSensorCard);
}

async function reloadSensors() {
  try { renderSensors(await getJSON('/api/sensors')); } catch (e) { console.error(e); }
}

async function deleteSensor(sensor) {
  if (!confirm(`Delete sensor "${sensor.key}"? This rewrites sensors.yml.`)) return;
  await fetch('/api/sensors/' + sensor.index, { method: 'DELETE' });
  reloadSensors();
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
  entry.avgInterval = s.avg_interval_seconds;
  r.rate.textContent = (s.rate_per_min || 0) + '/min';
  r.battery.textContent = s.battery_ok == null ? '' : (s.battery_ok ? '🔋 ok' : '🪫 low');
  r.signal.textContent = s.rssi == null ? '' : ('📶 ' + s.rssi.toFixed(0) + ' dBm');
  entry.sourcesData = s.sources || [];
  refreshAge(key);
}

function renderSources(entry) {
  const list = entry.sourcesData || [];
  const node = entry.refs.sources;
  node.className = 'sources-line' + (list.length > 1 ? ' multi' : ' muted');
  if (!list.length) { node.textContent = ''; return; }
  node.innerHTML = '';
  node.appendChild(document.createTextNode('📡 '));
  list.forEach((src, i) => {
    if (i) node.appendChild(document.createTextNode(' · '));
    const secs = src.last_seen ? (Date.now() - Date.parse(src.last_seen)) / 1000 : null;
    node.appendChild(el('span', 'src', `${src.receiver} (${fmtAge(secs)})`));
  });
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
  renderSources(entry);
  const r = entry.refs;

  // The card shows the average interval between messages; the dot/staleness colouring is
  // still driven by how long it's actually been since the last message.
  const interval = fmtDuration(entry.avgInterval);

  if (entry.lastSeen == null) {
    r.age.textContent = interval ? 'every ~' + interval : 'not seen';
    r.age.title = 'Not seen since restart';
    r.age.className = 'age-offline';
    entry.el.classList.add('offline'); entry.el.classList.remove('stale');
    r.dot.className = 'dot dot-bad';
    return;
  }
  const seconds = (Date.now() - entry.lastSeen) / 1000;
  // Fall back to "ago" until we have enough messages to know the interval.
  r.age.textContent = interval ? 'every ~' + interval : fmtAge(seconds);
  r.age.title = 'Last message ' + fmtAge(seconds);
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

  const line = el('div', 'line' + (ev.duplicate ? ' dup' : ''));
  line.appendChild(el('span', 'ts', new Date(ev.time).toLocaleTimeString() + ' '));
  line.appendChild(el('span', 'rcv', ev.receiver + ' '));
  line.appendChild(el('span', 'tag tag-' + tag, label));
  if (ev.duplicate) line.appendChild(el('span', 'tag tag-dup', 'dup'));
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
    case 'sensor_source':
      // A duplicate reading from another receiver: only the "seen by" view changes.
      if (ev.snapshot) updateSensorStats(ev.sensor, ev.snapshot);
      break;
    case 'receiver_status':
      if (ev.name) updateReceiverStats(ev.name, ev);
      break;
    case 'test':
      onTestEvent(ev);
      break;
    case 'unknown':
      scheduleUnknownsReload();
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
    renderSensors(sensors);
  } catch (e) {
    console.error('init failed', e);
  }

  reloadIgnored();
  reloadUnknowns();
  reloadDecoders();
  document.getElementById('add-sensor').addEventListener('click', () => openSensorModal(null));
  document.getElementById('add-ignored').addEventListener('click', addIgnored);
  document.getElementById('add-decoder').addEventListener('click', addDecoder);

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

// ---------------------------------------------------------------------------
// Recent unknown devices (claim / create from a live packet)
// ---------------------------------------------------------------------------

const DEVICE_FIELDS = ['model', 'subtype', 'id', 'channel', 'type'];

let unknownsDebounce = null;
function scheduleUnknownsReload() {
  clearTimeout(unknownsDebounce);
  unknownsDebounce = setTimeout(reloadUnknowns, 500);
}

async function reloadUnknowns() {
  let list;
  try { list = await getJSON('/api/unknowns'); } catch (e) { return; }
  const grid = document.getElementById('unknowns');
  grid.innerHTML = '';
  if (!list.length) { grid.appendChild(el('div', 'empty', 'No unknown devices seen yet.')); return; }
  list.slice(0, 10).forEach(createUnknownCard);
}

function prefillFromPacket(data) {
  const identifier = {};
  for (const f of DEVICE_FIELDS) if (f in data) identifier[f] = data[f];
  return { index: null, config: { type: 'temperature', topic_prefix: '', identifier } };
}

function createUnknownCard(u) {
  const card = el('div', 'card unknown-card');
  const head = el('div', 'uk-head');
  head.append(el('span', null, u.receiver), el('span', null, new Date(u.time).toLocaleTimeString()));
  card.appendChild(head);
  card.appendChild(el('pre', null, JSON.stringify(u.data, null, 2)));

  const actions = el('div', 'card-actions');
  const newBtn = el('button', 'small primary', 'New sensor');
  newBtn.addEventListener('click', () => openSensorModal(prefillFromPacket(u.data)));
  actions.appendChild(newBtn);
  if ('id' in u.data && u.encoded) {
    const claim = el('button', 'small', 'Claim existing');
    claim.addEventListener('click', () => { location.href = '/claim?packet=' + encodeURIComponent(u.encoded); });
    actions.appendChild(claim);
  }
  card.appendChild(actions);
  document.getElementById('unknowns').appendChild(card);
}

// ---------------------------------------------------------------------------
// Custom decoders
// ---------------------------------------------------------------------------

async function reloadDecoders() {
  let list;
  try { list = await getJSON('/api/decoders'); } catch (e) { return; }
  const grid = document.getElementById('decoders');
  grid.innerHTML = '';
  if (!list.length) { grid.appendChild(el('div', 'empty', 'No custom decoders.')); return; }
  list.forEach((spec, index) => {
    const card = el('div', 'card decoder-card');
    card.appendChild(el('span', 'spec', spec));
    const del = el('button', 'small danger', 'Remove');
    del.addEventListener('click', async () => {
      if (!confirm('Remove this decoder? Applies after a receiver restart.')) return;
      await fetch('/api/decoders/' + index, { method: 'DELETE' });
      reloadDecoders();
    });
    card.appendChild(del);
    grid.appendChild(card);
  });
}

async function addDecoder() {
  const spec = prompt('rtl_433 -X decoder spec, e.g.\nn=mybutton,m=OOK_PWM,s=364,l=1072,r=1084,g=0,t=283,y=0,bits=25');
  if (!spec) return;
  const r = await fetch('/api/decoders', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ decoder: spec }) });
  if (!r.ok) { const d = await r.json().catch(() => ({})); alert(d.error || 'Failed to add decoder'); return; }
  reloadDecoders();
}

// ---------------------------------------------------------------------------
// Ignored devices
// ---------------------------------------------------------------------------

async function reloadIgnored() {
  let list;
  try { list = await getJSON('/api/ignored'); } catch (e) { return; }
  const grid = document.getElementById('ignored');
  grid.innerHTML = '';
  if (!list.length) { grid.appendChild(el('div', 'empty', 'No ignored devices.')); return; }
  list.forEach((ident, index) => {
    const card = el('div', 'card ignored-card');
    card.appendChild(el('span', 'ident', Object.entries(ident).map(([k, v]) => `${k}=${v}`).join(', ')));
    const del = el('button', 'small danger', 'Remove');
    del.addEventListener('click', async () => { await fetch('/api/ignored/' + index, { method: 'DELETE' }); reloadIgnored(); });
    card.appendChild(del);
    grid.appendChild(card);
  });
}

async function addIgnored() {
  const raw = prompt('Ignore devices matching (comma-separated key=value), e.g.\nmodel=Nexa-Security');
  if (!raw) return;
  const ident = {};
  raw.split(',').forEach(pair => {
    const eq = pair.indexOf('=');
    if (eq > 0) ident[pair.slice(0, eq).trim()] = coerceValue(pair.slice(eq + 1));
  });
  if (!Object.keys(ident).length) return;
  await fetch('/api/ignored', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(ident) });
  reloadIgnored();
}

// ---------------------------------------------------------------------------
// Sensor add/edit modal + live test
// ---------------------------------------------------------------------------

const modal = document.getElementById('modal');
const fType = document.getElementById('f-type');
const fTopic = document.getElementById('f-topic');
const fTopicLabel = document.getElementById('f-topic-label');
const fIdentifier = document.getElementById('f-identifier');
const fExtra = document.getElementById('f-extra');
const fError = document.getElementById('f-error');
const testToggle = document.getElementById('test-toggle');
const testStatus = document.getElementById('test-status');
const testOutput = document.getElementById('test-output');
const testRaw = document.getElementById('test-raw');
const testParsed = document.getElementById('test-parsed');
const testType = document.getElementById('test-type');
const testCountEl = document.getElementById('test-count');

let editingIndex = null;
let testId = null;
let testRenewTimer = null;
let testDebounce = null;
let matchCount = 0;

function coerceValue(s) {
  s = String(s).trim();
  if (s === '') return s;
  if (/^-?\d+$/.test(s)) return parseInt(s, 10);
  if (/^-?\d*\.\d+$/.test(s)) return parseFloat(s);
  return s;
}

function topicKey(type) { return (type === 'door' || type === 'lightning') ? 'topic' : 'topic_prefix'; }

function addKvRow(container, key, val) {
  const row = el('div', 'kv-row');
  const k = el('input', 'kv-k'); k.placeholder = 'key'; k.value = key || '';
  const v = el('input', 'kv-v'); v.placeholder = 'value'; v.value = val === undefined ? '' : val;
  const rm = el('button', 'small', '✕');
  rm.type = 'button';
  rm.addEventListener('click', () => { row.remove(); restartTestSoon(); });
  row.append(k, v, rm);
  container.appendChild(row);
}

function readKv(container, coerce) {
  const out = {};
  for (const row of container.querySelectorAll('.kv-row')) {
    const k = row.querySelector('.kv-k').value.trim();
    if (!k) continue;
    const v = row.querySelector('.kv-v').value;
    out[k] = coerce ? coerceValue(v) : v.trim();
  }
  return out;
}

function updateTopicLabel() {
  fTopicLabel.textContent = topicKey(fType.value) === 'topic' ? 'Topic' : 'Topic prefix';
}

function buildExtra(type, config) {
  fExtra.innerHTML = '';
  if (type === 'button') {
    const fs = el('fieldset');
    fs.appendChild(el('legend', null, 'Buttons (raw code → name)'));
    const kv = el('div', 'kv'); kv.id = 'f-buttons';
    fs.appendChild(kv);
    const add = el('button', 'small', '+ button'); add.type = 'button';
    add.addEventListener('click', () => addKvRow(kv, '', ''));
    fs.appendChild(add);
    fExtra.appendChild(fs);
    const buttons = (config && config.buttons) || {};
    const entries = Object.entries(buttons);
    if (entries.length) entries.forEach(([k, v]) => addKvRow(kv, k, v));
    else addKvRow(kv, '', '');
  } else if (type === 'door') {
    const mk = (id, label, value) => {
      const wrap = el('label', 'field', label);
      const input = el('input'); input.id = id; input.type = 'text'; input.value = value || '';
      wrap.appendChild(input);
      return wrap;
    };
    fExtra.appendChild(mk('f-door-open', 'Door open code', config && config.door_open_code));
    fExtra.appendChild(mk('f-door-closed', 'Door closed code', config && config.door_closed_code));
    const rep = el('label', 'field');
    const cb = el('input'); cb.id = 'f-ignore-repeats'; cb.type = 'checkbox';
    cb.checked = config ? !!config.ignore_repeats : true;
    rep.append(cb, document.createTextNode(' ignore repeats'));
    fExtra.appendChild(rep);
  }
}

function assembleConfig() {
  const type = fType.value;
  const cfg = { type };
  cfg[topicKey(type)] = fTopic.value.trim();
  cfg.identifier = readKv(fIdentifier, true);
  if (type === 'button') cfg.buttons = readKv(document.getElementById('f-buttons'), false);
  if (type === 'door') {
    cfg.door_open_code = document.getElementById('f-door-open').value.trim();
    cfg.door_closed_code = document.getElementById('f-door-closed').value.trim();
    cfg.ignore_repeats = document.getElementById('f-ignore-repeats').checked;
  }
  return cfg;
}

function buildForm(config) {
  const type = (config && config.type) || 'temperature';
  fType.value = type;
  updateTopicLabel();
  fTopic.value = config ? (config.topic_prefix ?? config.topic ?? '') : '';
  fIdentifier.innerHTML = '';
  const entries = Object.entries((config && config.identifier) || {});
  if (entries.length) entries.forEach(([k, v]) => addKvRow(fIdentifier, k, v));
  else addKvRow(fIdentifier, 'model', '');
  buildExtra(type, config);
}

function openSensorModal(sensor) {
  // sensor may be: null (blank add), an existing sensor (edit, has index), or a prefill
  // {index: null, config} built from an unknown packet (add, prefilled).
  editingIndex = sensor && sensor.index != null ? sensor.index : null;
  document.getElementById('modal-title').textContent = editingIndex == null ? 'Add sensor' : 'Edit sensor';
  fError.textContent = '';
  stopTest();
  testOutput.classList.add('hidden');
  buildForm(sensor ? sensor.config : null);
  modal.classList.remove('hidden');
}

function closeModal() {
  stopTest();
  modal.classList.add('hidden');
}

async function saveSensor() {
  const cfg = assembleConfig();
  const url = editingIndex == null ? '/api/sensors' : '/api/sensors/' + editingIndex;
  const method = editingIndex == null ? 'POST' : 'PUT';
  const r = await fetch(url, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cfg) });
  const d = await r.json();
  if (!r.ok) { fError.textContent = d.error || 'Save failed'; return; }
  closeModal();
  reloadSensors();
}

// --- live test ---

function toggleTest() { testId ? stopTest() : startTest(); }

async function startTest() {
  const cfg = assembleConfig();
  const r = await fetch('/api/test-sensors', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cfg) });
  const d = await r.json();
  if (!r.ok) { fError.textContent = d.error || 'Invalid config'; return; }
  fError.textContent = '';
  testId = d.id;
  matchCount = 0;
  testToggle.textContent = '■ Stop test';
  testStatus.textContent = 'listening…';
  testType.textContent = cfg.type;
  testRaw.textContent = '—';
  testParsed.textContent = '—';
  testCountEl.textContent = '';
  testOutput.classList.remove('hidden');
  clearInterval(testRenewTimer);
  testRenewTimer = setInterval(() => { if (testId) fetch('/api/test-sensors/' + testId + '/renew', { method: 'POST' }); }, 120000);
}

function stopTest() {
  if (testId) { fetch('/api/test-sensors/' + testId, { method: 'DELETE' }); testId = null; }
  clearInterval(testRenewTimer); testRenewTimer = null;
  clearTimeout(testDebounce); testDebounce = null;
  testToggle.textContent = '▶ Start live test';
  testStatus.textContent = '';
}

function restartTestSoon() {
  if (!testId) return;
  testStatus.textContent = 'updating…';
  clearTimeout(testDebounce);
  testDebounce = setTimeout(() => {
    const old = testId;
    testId = null;
    fetch('/api/test-sensors/' + old, { method: 'DELETE' });
    startTest();
  }, 700);
}

function onTestEvent(ev) {
  if (ev.id !== testId) return;
  matchCount++;
  testRaw.textContent = JSON.stringify(ev.raw, null, 2);
  const hasParsed = ev.readings && Object.keys(ev.readings).length;
  testParsed.textContent = hasParsed ? JSON.stringify(ev.readings, null, 2) : '(matched — no parsed output for this packet)';
  testStatus.textContent = matchCount + ' matched';
}

document.getElementById('modal-close').addEventListener('click', closeModal);
document.getElementById('modal-cancel').addEventListener('click', closeModal);
document.getElementById('modal-save').addEventListener('click', saveSensor);
document.getElementById('f-id-add').addEventListener('click', () => addKvRow(fIdentifier, '', ''));
testToggle.addEventListener('click', toggleTest);
fType.addEventListener('change', () => { updateTopicLabel(); buildExtra(fType.value, null); restartTestSoon(); });
modal.addEventListener('input', restartTestSoon);
modal.addEventListener('click', (e) => { if (e.target === modal) closeModal(); });

init();
