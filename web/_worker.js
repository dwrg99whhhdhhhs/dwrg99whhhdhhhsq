function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' },
  });
}
function err(status, msg, extra) {
  return new Response(JSON.stringify({ error: msg, ...(extra || {}) }), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8' },
  });
}
function decodeBase64(b64) {
  const bytes = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
  return new TextDecoder().decode(bytes);
}

const API = 'https://api.github.com';
function ghHeaders(token) {
  return {
    Authorization: `Bearer ${token}`,
    Accept: 'application/vnd.github+json',
    'User-Agent': 'videovault-pages',
    'X-GitHub-Api-Version': '2022-11-28',
  };
}
function pad(n) { return String(n).padStart(5, '0'); }

async function handleConfig(env) {
  return json({
    baseUrl: env.PUBLIC_BASE_URL || '',
    repo: env.GH_REPO || '',
    mode: 'read-only',
  });
}

async function handleList(env) {
  const repo = env.GH_REPO;
  const token = env.GH_TOKEN;
  if (!repo || !token) return err(500, 'server missing GH_REPO/GH_TOKEN');
  try {
    const listRes = await fetch(`${API}/repos/${repo}/contents/storage/videos`, { headers: ghHeaders(token) });
    if (listRes.status === 404) return json([]);
    if (!listRes.ok) return err(listRes.status, 'github list failed');
    const entries = await listRes.json();
    const dirs = Array.isArray(entries) ? entries.filter(e => e.type === 'dir') : [];
    const items = await Promise.all(dirs.map(async d => {
      try {
        const mRes = await fetch(`${API}/repos/${repo}/contents/storage/videos/${d.name}/meta.json`, { headers: ghHeaders(token) });
        if (!mRes.ok) return null;
        const meta = JSON.parse(decodeBase64((await mRes.json()).content));
        return {
          id: meta.id || d.name,
          title: meta.title || d.name,
          chunkCount: meta.chunkCount || 0,
          size: (meta.chunkSizes || []).reduce((a, b) => a + b, 0),
          downloadedAt: meta.downloadedAt || 0,
        };
      } catch { return null; }
    }));
    const clean = items.filter(Boolean).sort((a, b) => (b.downloadedAt || 0) - (a.downloadedAt || 0));
    return new Response(JSON.stringify(clean), {
      headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'public, max-age=60' },
    });
  } catch (e) {
    return err(500, 'list error: ' + e.message);
  }
}

async function handleVideo(env, id) {
  const repo = env.GH_REPO;
  const token = env.GH_TOKEN;
  if (!repo || !token) return err(500, 'server missing GH_REPO/GH_TOKEN');
  try {
    const mRes = await fetch(`${API}/repos/${repo}/contents/storage/videos/${id}/meta.json`, { headers: ghHeaders(token) });
    if (mRes.status === 404) return err(404, 'not found');
    if (!mRes.ok) return err(mRes.status, 'meta fetch failed');
    const meta = JSON.parse(decodeBase64((await mRes.json()).content));
    const jsd = `https://cdn.jsdelivr.net/gh/${repo}@main/storage/videos/${id}`;
    return new Response(JSON.stringify({
      id: meta.id || id,
      title: meta.title || id,
      chunkCount: meta.chunkCount || 0,
      chunkSizes: meta.chunkSizes || [],
      size: (meta.chunkSizes || []).reduce((a, b) => a + b, 0),
      streamUrl: `/api/stream/${id}`,
      chunkBase: `${jsd}/chunk.`,
    }), {
      headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'public, max-age=300' },
    });
  } catch (e) {
    return err(500, 'video error: ' + e.message);
  }
}

async function handleStream(env, id, request) {
  const repo = env.GH_REPO;
  const token = env.GH_TOKEN;
  if (!repo || !token) return err(500, 'server missing GH_REPO/GH_TOKEN');
  try {
    const mRes = await fetch(`${API}/repos/${repo}/contents/storage/videos/${id}/meta.json`, { headers: ghHeaders(token) });
    if (!mRes.ok) return err(mRes.status, 'meta fetch failed');
    const meta = JSON.parse(decodeBase64((await mRes.json()).content));
    const chunkSizes = meta.chunkSizes || [];
    const total = chunkSizes.reduce((a, b) => a + b, 0);
    if (total === 0) return err(500, 'empty video');

    const range = request.headers.get('Range');
    let start = 0, end = total - 1;
    if (range) {
      const m = /^bytes=([0-9]*)-([0-9]*)$/.exec(range);
      if (m) {
        if (m[1]) start = parseInt(m[1], 10);
        if (m[2]) end = parseInt(m[2], 10);
      }
    }
    if (start < 0 || start > end || end >= total) {
      return new Response('', { status: 416, headers: { 'Content-Range': `bytes */${total}` } });
    }

    const jsdBase = `https://cdn.jsdelivr.net/gh/${repo}@main/storage/videos/${id}/chunk.`;
    const needed = [];
    let acc = 0;
    for (let i = 0; i < chunkSizes.length; i++) {
      const cStart = acc, cEnd = acc + chunkSizes[i] - 1;
      if (cEnd >= start && cStart <= end) {
        needed.push({ i, localStart: Math.max(0, start - cStart), localEnd: Math.min(chunkSizes[i] - 1, end - cStart) });
      }
      acc = cEnd + 1;
    }

    const body = new ReadableStream({
      async start(controller) {
        try {
          for (const n of needed) {
            const r = await fetch(`${jsdBase}${pad(n.i)}`);
            if (!r.ok) { controller.error(new Error('chunk ' + n.i + ' status ' + r.status)); return; }
            const buf = new Uint8Array(await r.arrayBuffer());
            controller.enqueue(buf.subarray(n.localStart, n.localEnd + 1));
          }
          controller.close();
        } catch (e) { controller.error(e); }
      },
    });

    const headersOut = {
      'Content-Type': 'video/mp4',
      'Accept-Ranges': 'bytes',
      'Content-Length': String(end - start + 1),
      'Content-Range': `bytes ${start}-${end}/${total}`,
      'Cache-Control': 'public, max-age=3600',
    };
    return new Response(body, { status: range ? 206 : 200, headers: headersOut });
  } catch (e) {
    return err(500, 'stream error: ' + e.message);
  }
}

function handleVRedirect(id) {
  return new Response(null, {
    status: 302,
    headers: { 'Location': `/#v=${id}` },
  });
}

const STYLE_CSS = `:root {
  --bg: #0e1116;
  --panel: #161b22;
  --panel-2: #1c2230;
  --text: #e6edf3;
  --muted: #8b949e;
  --accent: #2f81f7;
  --border: #2a313c;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
  background: var(--bg);
  color: var(--text);
}
.topbar {
  display: flex; align-items: center; justify-content: space-between;
  padding: 16px 24px; border-bottom: 1px solid var(--border); background: var(--panel);
}
.brand { display: flex; align-items: center; gap: 12px; }
.logo { font-size: 28px; color: var(--accent); }
.topbar h1 { margin: 0; font-size: 18px; }
.sub { margin: 2px 0 0; font-size: 12px; color: var(--muted); }
.badge { font-size: 12px; color: var(--muted); border: 1px solid var(--border); padding: 6px 10px; border-radius: 999px; }
.notice {
  margin: 16px 24px; padding: 12px 16px; font-size: 13px; line-height: 1.6;
  background: rgba(47,129,247,.08); border: 1px solid rgba(47,129,247,.3); border-radius: 10px; color: #c9d6e3;
}
.notice strong { color: #fff; }
.status { margin: 8px 24px; font-size: 13px; color: var(--muted); }
.status.ok { color: #3fb950; }
.status.err { color: #f85149; }
.grid {
  display: grid; gap: 16px; padding: 8px 24px 40px;
  grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
}
.card {
  background: var(--panel); border: 1px solid var(--border); border-radius: 12px;
  overflow: hidden; cursor: pointer; transition: transform .12s ease, border-color .12s ease;
}
.card:hover { transform: translateY(-3px); border-color: var(--accent); }
.thumb {
  height: 120px; display: flex; align-items: center; justify-content: center;
  background: linear-gradient(135deg, #1c2230, #11161f); font-size: 34px; color: var(--accent);
}
.meta { padding: 10px 12px 14px; }
.title { font-size: 14px; font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.info { margin-top: 4px; font-size: 12px; color: var(--muted); }

.overlay {
  position: fixed; inset: 0; background: rgba(0,0,0,.7);
  display: flex; align-items: center; justify-content: center; padding: 24px; z-index: 50;
}
.overlay.hidden { display: none; }
.player-box {
  position: relative; width: min(900px, 100%); background: var(--panel);
  border: 1px solid var(--border); border-radius: 14px; padding: 18px;
}
.player-box h2 { margin: 0 0 12px; font-size: 16px; }
.close {
  position: absolute; top: 12px; right: 12px; background: var(--panel-2); color: var(--text);
  border: 1px solid var(--border); border-radius: 8px; width: 32px; height: 32px; cursor: pointer; font-size: 14px;
}
.video-frame { width: 100%; aspect-ratio: 16/9; background: #000; border-radius: 10px; overflow: hidden; }
video { width: 100%; display: block; border-radius: 10px; background: #000; }
.row { margin-top: 12px; display: flex; align-items: center; gap: 12px; }
.btn {
  background: var(--accent); color: #fff; border: none; border-radius: 8px;
  padding: 8px 14px; font-size: 13px; cursor: pointer;
}
.btn:hover { filter: brightness(1.1); }
.msg { font-size: 12px; color: var(--muted); word-break: break-all; }`;

const APP_JS = `const $ = (s) => document.querySelector(s);
const grid = $('#grid');
const statusEl = $('#status');
const player = $('#player');
const video = $('#video');

let BASE = '';

function fmtSize(n) {
  if (!n) return '\\u2014';
  const u = ['B', 'KB', 'MB', 'GB'];
  let i = 0, v = n;
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return v.toFixed(1) + ' ' + u[i];
}
function fmtDate(ms) {
  if (!ms) return '';
  return new Date(ms).toLocaleString('zh-CN', { hour12: false });
}
function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

async function load() {
  try {
    const cfg = await (await fetch('/api/config')).json();
    BASE = cfg.baseUrl || location.origin;
    const res = await fetch('/api/list');
    if (!res.ok) {
      const e = await res.json().catch(() => ({}));
      throw new Error(e.error || ('HTTP ' + res.status));
    }
    const items = await res.json();
    render(items);
  } catch (e) {
    statusEl.className = 'status err';
    statusEl.textContent = '\\u52a0\\u8f7d\\u5931\\u8d25\\uff1a' + e.message;
  }
}

function render(items) {
  if (!items.length) {
    statusEl.className = 'status';
    statusEl.textContent = '\\u6682\\u65e0\\u89c6\\u9891\\u3002\\u5728\\u672c\\u673a VideoVault \\u4e0a\\u4f20\\u540e\\u8fd9\\u91cc\\u4f1a\\u81ea\\u52a8\\u51fa\\u73b0\\u3002';
    grid.innerHTML = '';
    return;
  }
  statusEl.className = 'status ok';
  statusEl.textContent = '\\u5171 ' + items.length + ' \\u4e2a\\u89c6\\u9891';
  grid.innerHTML = '';
  for (const it of items) {
    const card = document.createElement('div');
    card.className = 'card';
    card.innerHTML =
      '<div class="thumb">\\u25b6</div>' +
      '<div class="meta"><div class="title">' + escapeHtml(it.title) + '</div>' +
      '<div class="info">' + fmtSize(it.size) + ' \\u00b7 ' + it.chunkCount + ' \\u7247' +
      (it.downloadedAt ? ' \\u00b7 ' + fmtDate(it.downloadedAt) : '') + '</div></div>';
    card.onclick = () => openPlayer(it.id, it.title);
    grid.appendChild(card);
  }
}

function openPlayer(id, title) {
  $('#ptitle').textContent = title || id;
  video.src = '/api/stream/' + encodeURIComponent(id);
  video.play().catch(() => {});
  player.classList.remove('hidden');
  player.dataset.id = id;
  history.replaceState(null, '', '#v=' + encodeURIComponent(id));
}
function closePlayer() {
  video.pause();
  video.removeAttribute('src');
  video.load();
  player.classList.add('hidden');
  history.replaceState(null, '', location.pathname);
}

$('#close').onclick = closePlayer;
player.onclick = (e) => { if (e.target === player) closePlayer(); };
$('#share').onclick = async () => {
  const id = player.dataset.id;
  const link = location.origin + '/#v=' + encodeURIComponent(id);
  try {
    await navigator.clipboard.writeText(link);
    $('#shareMsg').textContent = '\\u5df2\\u590d\\u5236\\uff1a' + link;
  } catch {
    $('#shareMsg').textContent = '\\u94fe\\u63a5\\uff1a' + link;
  }
};

const hm = /#v=(.+)/.exec(location.hash);
if (hm) {
  const id = decodeURIComponent(hm[1]);
  fetch('/api/video/' + encodeURIComponent(id))
    .then(r => r.ok ? r.json() : null)
    .then(m => { if (m && m.title) openPlayer(m.id, m.title); })
    .catch(() => {});
}

load();`;

const INDEX_HTML = `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>VideoVault \u00b7 \u53ea\u8bfb\u955c\u50cf</title>
<style>
${STYLE_CSS}
</style>
</head>
<body>
<header class="topbar">
  <div class="brand">
    <span class="logo">\u2596</span>
    <div>
      <h1>VideoVault</h1>
      <p class="sub">\u53ea\u8bfb\u955c\u50cf\u5de5\u4f5c\u53f0 \u00b7 \u6570\u636e\u5b9e\u65f6\u6765\u81ea GitHub \u4ed3\u5e93</p>
    </div>
  </div>
  <span class="badge">\U0001f512 \u53ea\u8bfb \u00b7 \u672c\u673a\u5173\u95ed\u4e5f\u4e0d\u5f71\u54cd\u89c2\u770b</span>
</header>

<div class="notice">
  \u26a0\ufe0f \u8fd9\u662f<strong>\u53ea\u8bfb\u955c\u50cf</strong>\uff1a\u6d4f\u89c8 / \u64ad\u653e / \u5206\u4eab\u90fd\u53ef\u7528\u3002\u8981<strong>\u4e0a\u4f20\u6216\u8f6c\u7801\u65b0\u89c6\u9891</strong>\uff0c\u8bf7\u5728\u672c\u673a\u8fd0\u884c VideoVault \u540e\u64cd\u4f5c\uff0c\u4f20\u5b8c\u5373\u53ef\u5173\u673a\uff0c\u8fd9\u91cc\u4f1a\u81ea\u52a8\u51fa\u73b0\u65b0\u7247\u3002
</div>

<main id="app">
  <div id="status" class="status">\u52a0\u8f7d\u4e2d\u2026</div>
  <section id="grid" class="grid"></section>
</main>

<div id="player" class="overlay hidden">
  <div class="player-box">
    <button id="close" class="close" aria-label="\u5173\u95ed">\u2715</button>
    <h2 id="ptitle"></h2>
    <video id="video" controls></video>
    <div class="row">
      <button id="share" class="btn">\u590d\u5236\u5206\u4eab\u94fe\u63a5</button>
      <span id="shareMsg" class="msg"></span>
    </div>
  </div>
</div>

<script>
${APP_JS}
</script>
</body>
</html>`;

function serveStatic(path) {
  if (path === '/' || path === '/index.html') {
    return new Response(INDEX_HTML, {
      headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'public, max-age=300' },
    });
  }
  if (path === '/style.css') {
    return new Response(STYLE_CSS, {
      headers: { 'content-type': 'text/css; charset=utf-8', 'cache-control': 'public, max-age=3600' },
    });
  }
  if (path === '/app.js') {
    return new Response(APP_JS, {
      headers: { 'content-type': 'application/javascript; charset=utf-8', 'cache-control': 'public, max-age=3600' },
    });
  }
  return new Response('Not Found', { status: 404, headers: { 'content-type': 'text/plain' } });
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (path === '/api/config') return handleConfig(env);
    if (path === '/api/list') return handleList(env);

    const videoMatch = path.match(/^\/api\/video\/(.+)$/);
    if (videoMatch) return handleVideo(env, decodeURIComponent(videoMatch[1]));

    const streamMatch = path.match(/^\/api\/stream\/(.+)$/);
    if (streamMatch) return handleStream(env, decodeURIComponent(streamMatch[1]), request);

    const vMatch = path.match(/^\/v\/(.+)$/);
    if (vMatch) return handleVRedirect(vMatch[1]);

    return serveStatic(path);
  }
}
