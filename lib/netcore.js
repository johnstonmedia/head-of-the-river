// The online lobby server: who's in Online mode, the servers (race rooms) and regattas they're hosting,
// matchmaking messages between browsers, and the lobby chat. The races themselves run browser to browser
// (WebRTC, set up through the matchmaking messages), so this only ever handles small, slow traffic.
//
// Everything here is soft state: each browser re-sends its presence (name, and the server it hosts) every
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
  const cmds = [];
  if (lobby.stale.length) cmds.push(["HDEL", K_LOBBY, ...lobby.stale]);
  if (leaving) cmds.push(["HDEL", K_LOBBY, b.id]);
  else cmds.push(["HSET", K_LOBBY, b.id, JSON.stringify({ k: b.key, at: now, p: pres })], ["EXPIRE", K_LOBBY, 600]);
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
  return { ok: true, now, peers, inbox, chat, taken, store: st.kind };
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

const OPS = { poll: opPoll, send: opSend, knock: opKnock, chat: opChat };

// one request: { op, ...args } → [status, body]
export async function handle(body, env) {
  const st = getStore(env);
  const now = Date.now();
  if (!body || typeof body !== "object") return [400, { ok: false, error: "bad_request" }];
  if (body.op === "hello") return [200, { ok: true, v: 1, store: st.kind, now }];
  const op = OPS[body.op];
  if (!op) return [400, { ok: false, error: "bad_op" }];
  try { return [200, await op(st, body, now)]; }
  catch (e) {
    if (e instanceof HttpError) return [e.status, { ok: false, error: e.code }];
    console.error(e);
    return [500, { ok: false, error: "server" }];
  }
}
