// The online lobby server: who's in Online mode, the regattas (race rooms) they're hosting, matchmaking
// messages between browsers, the lobby chat, and the online rankings. The races themselves run browser to browser
// (WebRTC, set up through the matchmaking messages), so this only ever handles small, slow traffic.
//
// Everything but the rankings is soft state: each browser re-sends its presence (name, the regatta it hosts) every
// poll, and anything not refreshed for a little while drops out. Storage is Redis (Upstash REST) when the
// env vars are set, otherwise memory (fine for one machine; on Vercel, add Upstash Redis for reliability).

const STALE_MS = 30000;          // a browser that hasn't polled for this long has gone
const LOBBY_MAX = 400;           // browsers in Online mode at once
const PRES_MAX = 2048;           // one browser's presence, as JSON
const MSG_MAX = 4096;            // one matchmaking message, as JSON
const CHAT_MAX = 200;            // characters in a chat line
const CHAT_KEEP = 80;            // lines of lobby chat kept
const CHAT_GAP_MS = 700;         // one chat line per browser per this long
const INBOX_TTL = 120;           // s a matchmaking message waits to be picked up
const ID_RE = /^c[a-z0-9]{11}$/;
const KEY_RE = /^[a-z0-9]{24}$/;
const CODE_RE = /^[a-z0-9]{3,8}$/;
const MSG_TYPES = new Set(["invite", "reply", "full", "cancel"]);
const PID_RE = /^p[a-z0-9]{11}$/;   // a player's lasting id (the browser keeps it, with its key, for the rankings)
const ELO_START = 1000, ELO_K = 32;
const TOP_N = 50;

// ---------------------------------------------------------------------------
// storage: a Redis pipeline, over Upstash's REST API or in memory
// ---------------------------------------------------------------------------
function upstashStore(url, token) {
  return {
    kind: "redis",
    async pipe(cmds) {
      const r = await fetch(url.replace(/\/$/, "") + "/pipeline", {
        method: "POST",
        headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
        body: JSON.stringify(cmds),
      });
      if (!r.ok) throw new Error(`redis ${r.status}`);
      const out = await r.json();
      return out.map((x) => (x && x.error ? null : x ? x.result : null));
    },
  };
}

function memoryStore() {
  const data = new Map();      // key → { v, exp }
  const live = (k) => { const e = data.get(k); if (!e) return null; if (e.exp && e.exp <= Date.now()) { data.delete(k); return null; } return e; };
  const hash = (k) => { let e = live(k); if (!e) { e = { v: new Map(), exp: 0 }; data.set(k, e); } return e.v; };
  const list = (k) => { let e = live(k); if (!e) { e = { v: [], exp: 0 }; data.set(k, e); } return e.v; };
  const idx = (n, len) => (n < 0 ? Math.max(0, len + n) : Math.min(n, len));
  const run = ([cmd, k, ...a]) => {
    switch (cmd) {
      case "HGETALL": { const e = live(k); if (!e) return []; const out = []; for (const [f, v] of e.v) out.push(f, v); return out; }
      case "HGET": { const e = live(k); return e && e.v.has(a[0]) ? e.v.get(a[0]) : null; }
      case "HMGET": { const e = live(k); return a.map((f) => (e && e.v.has(f) ? e.v.get(f) : null)); }
      case "HSET": { const h = hash(k); let n = 0; for (let i = 0; i + 1 < a.length; i += 2) { if (!h.has(a[i])) n++; h.set(a[i], a[i + 1]); } return n; }
      case "HDEL": { const e = live(k); if (!e) return 0; let n = 0; for (const f of a) if (e.v.delete(f)) n++; return n; }
      case "RPUSH": { const l = list(k); l.push(...a); return l.length; }
      case "LPOP": { const e = live(k); if (!e) return null; const n = a.length ? Number(a[0]) : 1; const out = e.v.splice(0, n); if (!e.v.length) data.delete(k); return a.length ? (out.length ? out : null) : out[0] ?? null; }
      case "LRANGE": { const e = live(k); if (!e) return []; const len = e.v.length; const s = idx(Number(a[0]), len), t = Number(a[1]) < 0 ? len + Number(a[1]) : Math.min(Number(a[1]), len - 1); return e.v.slice(s, t + 1); }
      case "LTRIM": { const e = live(k); if (!e) return "OK"; const len = e.v.length; const s = idx(Number(a[0]), len), t = Number(a[1]) < 0 ? len + Number(a[1]) : Math.min(Number(a[1]), len - 1); e.v = e.v.slice(s, t + 1); return "OK"; }
      case "EXPIRE": { const e = live(k); if (!e) return 0; e.exp = Date.now() + Number(a[0]) * 1000; return 1; }
      case "SET": {
        const nx = a.includes("NX"), px = a.indexOf("PX");
        if (nx && live(k)) return null;
        data.set(k, { v: a[0], exp: px >= 0 ? Date.now() + Number(a[px + 1]) : 0 });
        return "OK";
      }
      case "DEL": return data.delete(k) ? 1 : 0;
      case "ZADD": { const h = hash(k); let n = 0; for (let i = 0; i + 1 < a.length; i += 2) { if (!h.has(a[i + 1])) n++; h.set(a[i + 1], Number(a[i])); } return n; }
      case "ZSCORE": { const e = live(k); return e && e.v.has(a[0]) ? String(e.v.get(a[0])) : null; }
      case "ZREVRANGE": {
        const e = live(k); if (!e) return [];
        const all = [...e.v].sort((x, y) => y[1] - x[1] || (x[0] < y[0] ? -1 : 1));
        const s = Number(a[0]), t = Number(a[1]) < 0 ? all.length + Number(a[1]) : Number(a[1]);
        const out = []; for (const [m, sc] of all.slice(s, t + 1)) { out.push(m); if (a[2] === "WITHSCORES") out.push(String(sc)); }
        return out;
      }
      case "ZREVRANK": {
        const e = live(k); if (!e || !e.v.has(a[0])) return null;
        return [...e.v].sort((x, y) => y[1] - x[1] || (x[0] < y[0] ? -1 : 1)).findIndex(([m]) => m === a[0]);
      }
      default: throw new Error("unsupported " + cmd);
    }
  };
  return { kind: "memory", async pipe(cmds) { return cmds.map(run); } };
}

let store = null;
export function getStore(env = process.env) {
  if (store) return store;
  const url = env.KV_REST_API_URL || env.UPSTASH_REDIS_REST_URL, token = env.KV_REST_API_TOKEN || env.UPSTASH_REDIS_REST_TOKEN;
  store = url && token ? upstashStore(url, token) : memoryStore();
  return store;
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------
const P = "hotr:";
const K_LOBBY = P + "lobby", K_CHAT = P + "chat:lobby";
const kInbox = (id) => P + "in:" + id, kRate = (id) => P + "rl:" + id;
const K_PKEY = P + "pkey", K_RANK = P + "rank", K_ELO = P + "elo", kRes = (rid) => P + "res:" + rid;

class HttpError extends Error { constructor(status, code) { super(code); this.status = status; this.code = code; } }
const bad = (code) => new HttpError(400, code);

function cleanText(s, max) {
  if (typeof s !== "string") return "";
  // no control or invisible characters, one line
  return Array.from(s.replace(/[\u0000-\u001f\u007f-\u009f­​-‏‪-‮⁠-⁯﻿]/g, "").replace(/\s+/g, " ").trim()).slice(0, max).join("");
}
function parseEntry(raw) { try { const e = JSON.parse(raw); return e && typeof e === "object" ? e : null; } catch (e) { return null; } }
function pairs(arr) { const out = []; for (let i = 0; i + 1 < (arr || []).length; i += 2) out.push([arr[i], arr[i + 1]]); return out; }

// everyone in the lobby who's still polling: id → entry { k, at, p }
async function readLobby(st, now) {
  const [all] = await st.pipe([["HGETALL", K_LOBBY]]);
  const live = new Map(), stale = [];
  for (const [id, raw] of pairs(all)) {
    const e = parseEntry(raw);
    if (!e || now - e.at > STALE_MS) stale.push(id); else live.set(id, e);
  }
  return { live, stale };
}
function authed(lobby, id, key) {
  if (!ID_RE.test(id || "") || !KEY_RE.test(key || "")) throw bad("bad_id");
  const e = lobby.live.get(id);
  if (e && e.k !== key) throw new HttpError(403, "not_yours");
  return e || null;
}
// what the others see of a browser: no key, and a private server's code stays private
function publicPeer(id, e) {
  const p = { ...(e.p || {}) };
  if (p.h && typeof p.h === "object" && p.h.pv) delete p.h;
  return { peer: id, presence: p };
}

// a player's lasting id: the first browser to use it sets its key; after that only that key can use it
async function sha(s) { const { createHash } = await import("node:crypto"); return createHash("sha256").update(s).digest("hex"); }
async function checkPid(st, b, prev) {
  if (!PID_RE.test(b.pid || "") || !KEY_RE.test(b.pkey || "")) return null;
  if (prev && prev.pid === b.pid) return b.pid;
  const h = await sha(b.pkey);
  const [have] = await st.pipe([["HGET", K_PKEY, b.pid]]);
  if (have && have !== h) return false;
  if (!have) await st.pipe([["HSET", K_PKEY, b.pid, h]]);
  return b.pid;
}

// ---------------------------------------------------------------------------
// the ops
// ---------------------------------------------------------------------------
async function opPoll(st, b, now) {
  const lobby = await readLobby(st, now);
  const prev = authed(lobby, b.id, b.key);
  const leaving = b.pres === null;
  let pres = null;
  if (!leaving) {
    pres = b.pres && typeof b.pres === "object" && !Array.isArray(b.pres) ? b.pres : {};
    if (JSON.stringify(pres).length > PRES_MAX) throw bad("too_big");
    if (!prev && lobby.live.size >= LOBBY_MAX) throw new HttpError(503, "full");
    // two servers can't share a code: the one that had it first keeps it
    const c = pres.h && typeof pres.h === "object" ? pres.h.c : null;
    if (c != null) {
      if (typeof c !== "string" || !CODE_RE.test(c)) throw bad("bad_code");
      for (const [id, e] of lobby.live) if (id !== b.id && e.p && e.p.h && e.p.h.c === c) { pres = { ...pres, h: null }; break; }
    }
  }
  const pid = leaving ? null : await checkPid(st, b, prev);
  const cmds = [];
  if (lobby.stale.length) cmds.push(["HDEL", K_LOBBY, ...lobby.stale]);
  if (leaving) cmds.push(["HDEL", K_LOBBY, b.id]);
  else cmds.push(["HSET", K_LOBBY, b.id, JSON.stringify({ k: b.key, at: now, p: pres, pid: pid || null })], ["EXPIRE", K_LOBBY, 600]);
  cmds.push(["LPOP", kInbox(b.id), "50"], ["LRANGE", K_CHAT, String(-CHAT_KEEP), "-1"]);
  const res = await st.pipe(cmds);
  const inbox = (res[res.length - 2] || []).map(parseEntry).filter(Boolean);
  const since = Number.isFinite(b.since) ? b.since : 0;
  const chat = (res[res.length - 1] || []).map(parseEntry).filter((m) => m && m.at > since);
  const peers = [];
  if (!leaving) {
    lobby.live.set(b.id, { k: b.key, at: now, p: pres });
    for (const [id, e] of lobby.live) peers.push(id === b.id ? { peer: id, presence: e.p || {} } : publicPeer(id, e));
  }
  const taken = !!(b.pres && b.pres.h && b.pres.h.c && !(pres && pres.h));
  return { ok: true, now, peers, inbox, chat, taken, pidBad: pid === false, store: st.kind };
}

// a matchmaking message to another browser in the lobby
async function opSend(st, b, now) {
  const lobby = await readLobby(st, now);
  if (!authed(lobby, b.id, b.key)) throw new HttpError(403, "not_in_lobby");
  if (!ID_RE.test(b.to || "") || !lobby.live.has(b.to)) throw new HttpError(404, "gone");
  const m = b.msg;
  if (!m || typeof m !== "object" || !MSG_TYPES.has(m.t)) throw bad("bad_msg");
  const msg = JSON.stringify({ ...m, from: b.id, at: now });
  if (msg.length > MSG_MAX) throw bad("too_big");
  await st.pipe([["RPUSH", kInbox(b.to), msg], ["EXPIRE", kInbox(b.to), INBOX_TTL]]);
  return { ok: true };
}

// asking to join a server by its code: the host hears about it, and you hear who the host is
async function opKnock(st, b, now) {
  const lobby = await readLobby(st, now);
  const me = authed(lobby, b.id, b.key);
  if (!me) throw new HttpError(403, "not_in_lobby");
  const code = String(b.code || "").toLowerCase();
  if (!CODE_RE.test(code)) throw bad("bad_code");
  let host = null;
  for (const [id, e] of lobby.live) if (id !== b.id && e.p && e.p.h && e.p.h.c === code) { host = [id, e]; break; }
  if (!host) throw new HttpError(404, "no_server");
  const nm = cleanText(me.p && me.p.nm, 16) || "Rower";
  await st.pipe([["RPUSH", kInbox(host[0]), JSON.stringify({ t: "knock", from: b.id, nm, at: now })], ["EXPIRE", kInbox(host[0]), INBOX_TTL]]);
  const h = host[1].p.h;
  return { ok: true, host: host[0], nm: cleanText(host[1].p.nm, 16) || "The host", sn: cleanText(h.sn, 24) };
}

// a line in the lobby chat (signed with the name you're using: the one you send, or your presence's)
async function opChat(st, b, now) {
  const lobby = await readLobby(st, now);
  const me = authed(lobby, b.id, b.key);
  if (!me) throw new HttpError(403, "not_in_lobby");
  const tx = cleanText(b.tx, CHAT_MAX);
  if (!tx) throw bad("empty");
  const [ok] = await st.pipe([["SET", kRate(b.id), "1", "PX", String(CHAT_GAP_MS), "NX"]]);
  if (!ok) throw new HttpError(429, "slow_down");
  const line = { id: Math.random().toString(36).slice(2, 10), at: now, from: b.id, nm: cleanText(b.nm, 16) || cleanText(me.p && me.p.nm, 16) || "Rower", tx };
  await st.pipe([["RPUSH", K_CHAT, JSON.stringify(line)], ["LTRIM", K_CHAT, String(-CHAT_KEEP), "-1"], ["EXPIRE", K_CHAT, 86400]]);
  return { ok: true, line };
}

// Rankings. A regatta's host reports each race between two or more people in the lobby: [pid, name, place].
// Ratings move Elo-style, every pair of crews in the race counting as a head-to-head (K shared between them).
// Every player in the report has to be in Online mode right now, and each race only counts once.
async function opResult(st, b, now) {
  const lobby = await readLobby(st, now);
  if (!authed(lobby, b.id, b.key)) throw new HttpError(403, "not_in_lobby");
  if (typeof b.rid !== "string" || !/^[a-z0-9]{6,16}$/.test(b.rid)) throw bad("bad_rid");
  if (!Array.isArray(b.rows) || b.rows.length < 2 || b.rows.length > 8) throw bad("bad_rows");
  const online = new Set([...lobby.live.values()].map((e) => e.pid).filter(Boolean));
  const rows = [], seen = new Set();
  for (const r of b.rows) {
    if (!Array.isArray(r) || !PID_RE.test(r[0] || "") || seen.has(r[0]) || !Number.isInteger(r[2]) || r[2] < 1 || r[2] > 8) throw bad("bad_rows");
    if (!online.has(r[0])) throw new HttpError(403, "not_online");
    seen.add(r[0]);
    rows.push({ pid: r[0], nm: cleanText(r[1], 16) || "Rower", place: r[2] });
  }
  const [fresh] = await st.pipe([["SET", kRes(b.rid), "1", "EX", "86400", "NX"]]);
  if (!fresh) throw new HttpError(409, "counted");
  const recs = await st.pipe([["HMGET", K_RANK, ...rows.map((r) => r.pid)]]);
  const cur = recs[0].map((raw) => parseEntry(raw) || { r: ELO_START, n: 0, w: 0, best: ELO_START });
  const n = rows.length, k = ELO_K / (n - 1), delta = new Array(n).fill(0);
  for (let i = 0; i < n; i++) for (let j = 0; j < n; j++) {
    if (i === j) continue;
    const exp = 1 / (1 + 10 ** ((cur[j].r - cur[i].r) / 400));
    const score = rows[i].place < rows[j].place ? 1 : rows[i].place === rows[j].place ? 0.5 : 0;
    delta[i] += k * (score - exp);
  }
  const best = Math.min(...rows.map((r) => r.place));
  const out = [], cmds = [];
  rows.forEach((r, i) => {
    const e = cur[i], nr = Math.round((e.r + delta[i]) * 10) / 10;
    const rec = { nm: r.nm, r: nr, n: e.n + 1, w: e.w + (r.place === best ? 1 : 0), best: Math.max(e.best || ELO_START, nr), at: now };
    cmds.push(["HSET", K_RANK, r.pid, JSON.stringify(rec)], ["ZADD", K_ELO, String(nr), r.pid]);
    out.push([r.pid, Math.round(nr - e.r), Math.round(nr)]);
  });
  await st.pipe(cmds);
  return { ok: true, rk: out };
}

// the table: the top fifty, and any players you ask about (with their place in it)
async function opRanks(st, b) {
  const want = Array.isArray(b.pids) ? [...new Set(b.pids.filter((p) => PID_RE.test(p || "")))].slice(0, 16) : [];
  const [top] = await st.pipe([["ZREVRANGE", K_ELO, "0", String(TOP_N - 1), "WITHSCORES"]]);
  const ids = []; for (let i = 0; i + 1 < (top || []).length; i += 2) ids.push(top[i]);
  const all = [...new Set([...ids, ...want])];
  const cmds = [];
  if (all.length) cmds.push(["HMGET", K_RANK, ...all]);
  for (const p of want) cmds.push(["ZREVRANK", K_ELO, p]);
  const res = cmds.length ? await st.pipe(cmds) : [];
  const recs = new Map();
  if (all.length) all.forEach((p, i) => { const e = parseEntry(res[0][i]); if (e) recs.set(p, e); });
  const row = (p, pos) => { const e = recs.get(p); return e ? { pid: p, pos, nm: cleanText(e.nm, 16) || "Rower", r: Math.round(e.r), n: e.n | 0, w: e.w | 0, best: Math.round(e.best || e.r) } : null; };
  const out = ids.map((p, i) => row(p, i + 1)).filter(Boolean);
  const me = {};
  want.forEach((p, i) => { const pos = res[(all.length ? 1 : 0) + i]; const r = row(p, Number.isInteger(pos) ? pos + 1 : null); if (r) me[p] = r; });
  return { ok: true, top: out, players: me };
}

// Relay servers (TURN) for connections that can't go direct: phone hotspots, mobile data, strict Wi-Fi.
// Cloudflare's TURN (CF_TURN_KEY_ID + CF_TURN_API_TOKEN: short-lived credentials made here), or any TURN
// service's fixed ones (TURN_URLS, comma-separated, with TURN_USERNAME and TURN_CREDENTIAL).
let iceCache = null;
async function opIce(st, b, now, env = process.env) {
  if (iceCache && iceCache.until > now) return { ok: true, iceServers: iceCache.list };
  const list = [];
  if (env.CF_TURN_KEY_ID && env.CF_TURN_API_TOKEN) {
    try {
      const r = await fetch(`https://rtc.live.cloudflare.com/v1/turn/keys/${env.CF_TURN_KEY_ID}/credentials/generate-ice-servers`, {
        method: "POST", headers: { Authorization: `Bearer ${env.CF_TURN_API_TOKEN}`, "Content-Type": "application/json" }, body: JSON.stringify({ ttl: 86400 }),
      });
      const j = r.ok ? await r.json() : null;
      const got = j && j.iceServers;
      for (const x of Array.isArray(got) ? got : got ? [got] : []) if (x && x.urls) list.push({ urls: x.urls, username: x.username, credential: x.credential });
    } catch (e) { console.error("turn", e); }
  }
  if (env.TURN_URLS && env.TURN_USERNAME && env.TURN_CREDENTIAL) list.push({ urls: env.TURN_URLS.split(",").map((u) => u.trim()).filter(Boolean), username: env.TURN_USERNAME, credential: env.TURN_CREDENTIAL });
  iceCache = { list, until: now + (list.length ? 3600000 : 60000) };
  return { ok: true, iceServers: list };
}

const OPS = { ice: opIce, poll: opPoll, send: opSend, knock: opKnock, chat: opChat, result: opResult, ranks: opRanks };

// one request: { op, ...args } → [status, body]
export async function handle(body, env) {
  const st = getStore(env);
  const now = Date.now();
  if (!body || typeof body !== "object") return [400, { ok: false, error: "bad_request" }];
  if (body.op === "hello") return [200, { ok: true, v: 1, store: st.kind, now }];
  const op = OPS[body.op];
  if (!op) return [400, { ok: false, error: "bad_op" }];
  try { return [200, await op(st, body, now, env || process.env)]; }
  catch (e) {
    if (e instanceof HttpError) return [e.status, { ok: false, error: e.code }];
    console.error(e);
    return [500, { ok: false, error: "server" }];
  }
}
