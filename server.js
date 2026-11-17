const express = require('express');
const path = require('path');
const { GoogleAuth } = require('google-auth-library');

const PORT = process.env.PORT || 8080;
const REGION = process.env.REGION || 'us-central1';
const SERVICE = process.env.SERVICE || 'portfolio';
const PASSWORD = process.env.PASSWORD || ''; // set this! protects the dashboard
const FIXED_TARGET = process.env.TARGET_URL || ''; // optional; else read from Cloud Run service
const MAX_CONCURRENCY = 300;
const MAX_DURATION_SEC = 600;

const auth = new GoogleAuth({ scopes: ['https://www.googleapis.com/auth/cloud-platform'] });
let projectId = process.env.PROJECT_ID || '';

const app = express();
app.use(express.json());

// ---- simple basic-auth gate (the target URL is fixed, so this can't be aimed elsewhere) ----
app.use((req, res, next) => {
  if (!PASSWORD) return next();
  const h = req.headers.authorization || '';
  const [scheme, b64] = h.split(' ');
  if (scheme === 'Basic' && b64) {
    const pass = Buffer.from(b64, 'base64').toString().split(':').slice(1).join(':');
    if (pass === PASSWORD) return next();
  }
  res.set('WWW-Authenticate', 'Basic realm="scaling-tester"').status(401).send('Auth required');
});
app.use(express.static(path.join(__dirname, 'public')));

// ---- Google API helpers ----
async function gget(url) {
  const client = await auth.getClient();
  const r = await client.request({ url });
  return r.data;
}
async function getProject() {
  if (!projectId) projectId = await auth.getProjectId();
  return projectId;
}
let cachedUri = '';
async function getTarget() {
  if (FIXED_TARGET) return FIXED_TARGET;
  if (cachedUri) return cachedUri;
  const p = await getProject();
  const svc = await gget(`https://run.googleapis.com/v2/projects/${p}/locations/${REGION}/services/${SERVICE}`);
  cachedUri = svc.uri;
  return cachedUri;
}

// ---- load generator ----
let load = null;
let ticker = null;

function startLoad(url, concurrency, durationSec) {
  const run = {
    running: true, url, concurrency, durationSec,
    startedAt: Date.now(), endsAt: Date.now() + durationSec * 1000,
    sent: 0, ok: 0, errors: 0, codes: {}, latSum: 0, latMax: 0,
    series: [], _lastSent: 0, _lastErr: 0, active: 0,
  };
  load = run;
  clearInterval(ticker);
  ticker = setInterval(() => {
    run.series.push({ t: Date.now(), rps: run.sent - run._lastSent, err: run.errors - run._lastErr });
    run._lastSent = run.sent; run._lastErr = run.errors;
    if (run.series.length > 180) run.series.shift();
    if (!run.running && run.active === 0) clearInterval(ticker);
  }, 1000);

  const worker = async () => {
    run.active++;
    while (run.running && Date.now() < run.endsAt) {
      const t0 = Date.now();
      const ctrl = new AbortController();
      const to = setTimeout(() => ctrl.abort(), 15000);
      try {
        const sep = url.includes('?') ? '&' : '?';
        const r = await fetch(`${url}${sep}cb=${t0}${Math.random().toString(36).slice(2)}`, {
          signal: ctrl.signal, headers: { 'cache-control': 'no-cache' },
        });
        await r.arrayBuffer();
        run.codes[r.status] = (run.codes[r.status] || 0) + 1;
        if (r.status < 400) run.ok++; else run.errors++;
      } catch (e) {
        run.errors++;
        run.codes['err'] = (run.codes['err'] || 0) + 1;
      } finally {
        clearTimeout(to);
        run.sent++;
        const lat = Date.now() - t0;
        run.latSum += lat; if (lat > run.latMax) run.latMax = lat;
      }
    }
    run.active--;
    if (run.active === 0) run.running = false;
  };
  for (let i = 0; i < concurrency; i++) worker();
}

app.post('/api/load/start', async (req, res) => {
  try {
    if (load && load.running) return res.status(409).json({ error: 'Already running' });
    const concurrency = Math.min(Math.max(parseInt(req.body.concurrency) || 20, 1), MAX_CONCURRENCY);
    const durationSec = Math.min(Math.max(parseInt(req.body.durationSec) || 60, 5), MAX_DURATION_SEC);
    const url = await getTarget();
    startLoad(url, concurrency, durationSec);
    res.json({ ok: true, url, concurrency, durationSec });
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
});

app.post('/api/load/stop', (req, res) => {
  if (load) load.running = false;
  res.json({ ok: true });
});

app.get('/api/load', (req, res) => {
  if (!load) return res.json({ idle: true });
  const { _lastSent, _lastErr, ...rest } = load;
  res.json({
    ...rest,
    elapsedSec: Math.round((Math.min(Date.now(), load.endsAt) - load.startedAt) / 1000),
    avgLatencyMs: load.sent ? Math.round(load.latSum / load.sent) : 0,
  });
});

// ---- cloud state: scaling config, instance count, scheduler jobs ----
app.get('/api/cloud', async (req, res) => {
  const out = { service: null, instances: null, scheduler: null, errors: {} };
  let p;
  try { p = await getProject(); } catch (e) { return res.status(500).json({ error: 'No project: ' + e.message }); }

  try {
    const s = await gget(`https://run.googleapis.com/v2/projects/${p}/locations/${REGION}/services/${SERVICE}`);
    const sc = (s.template && s.template.scaling) || {};
    out.service = {
      uri: s.uri,
      minInstances: sc.minInstanceCount || 0,
      maxInstances: sc.maxInstanceCount || null,
      concurrency: s.template && s.template.maxInstanceRequestConcurrency,
      latestRevision: s.latestReadyRevision && s.latestReadyRevision.split('/').pop(),
      updateTime: s.updateTime,
    };
  } catch (e) { out.errors.service = String(e.message || e); }

  try {
    const end = new Date(), start = new Date(end.getTime() - 30 * 60 * 1000);
    const q = new URLSearchParams({
      filter: `metric.type="run.googleapis.com/container/instance_count" AND resource.labels.service_name="${SERVICE}"`,
      'interval.startTime': start.toISOString(),
      'interval.endTime': end.toISOString(),
      'aggregation.alignmentPeriod': '60s',
      'aggregation.perSeriesAligner': 'ALIGN_MAX',
      'aggregation.crossSeriesReducer': 'REDUCE_SUM',
      'aggregation.groupByFields': 'metric.label.state',
    });
    const d = await gget(`https://monitoring.googleapis.com/v3/projects/${p}/timeSeries?${q}`);
    const byTime = {};
    for (const ts of d.timeSeries || []) {
      const state = (ts.metric.labels && ts.metric.labels.state) || 'total';
      for (const pt of ts.points || []) {
        const t = pt.interval.endTime;
        byTime[t] = byTime[t] || { t, active: 0, idle: 0 };
        byTime[t][state] = Number(pt.value.int64Value || pt.value.doubleValue || 0);
      }
    }
    const rows = Object.values(byTime).sort((a, b) => a.t.localeCompare(b.t));
    rows.forEach(r => (r.total = (r.active || 0) + (r.idle || 0)));
    out.instances = rows;
  } catch (e) { out.errors.instances = String(e.message || e); }

  try {
    const d = await gget(`https://cloudscheduler.googleapis.com/v1/projects/${p}/locations/${REGION}/jobs`);
    out.scheduler = (d.jobs || []).map(j => ({
      name: j.name.split('/').pop(), schedule: j.schedule, timeZone: j.timeZone, state: j.state,
      lastAttemptTime: j.lastAttemptTime, nextRun: j.scheduleTime,
      lastStatus: j.status && j.status.code ? `error ${j.status.code}` : (j.lastAttemptTime ? 'OK' : '-'),
    }));
  } catch (e) { out.errors.scheduler = String(e.message || e); }

  res.json(out);
});

app.listen(PORT, () => console.log(`scaling-tester listening on ${PORT}`));
