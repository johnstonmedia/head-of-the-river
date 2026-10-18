// Vercel Function: the online lobby (see lib/netcore.js)
import { handle } from "../lib/netcore.js";

export default async function handler(req, res) {
  res.setHeader("Cache-Control", "no-store");
  if (req.method === "GET") { const [s, b] = await handle({ op: "hello" }); res.status(s).json(b); return; }
  if (req.method !== "POST") { res.status(405).json({ ok: false, error: "method" }); return; }
  let body = req.body;
  if (typeof body === "string") { try { body = JSON.parse(body); } catch (e) { body = null; } }
  const [status, out] = await handle(body);
  res.status(status).json(out);
}
