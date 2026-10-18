// Run the game and its online lobby on one machine: node server/dev.js (then open http://localhost:3000)
import http from "node:http";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { handle } from "../lib/netcore.js";

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const port = Number(process.env.PORT) || 3000;

http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://x");
  if (url.pathname === "/api/net") {
    let body = null;
    if (req.method === "POST") {
      let raw = "";
      for await (const chunk of req) { raw += chunk; if (raw.length > 65536) break; }
      try { body = JSON.parse(raw); } catch (e) { body = null; }
    } else body = { op: "hello" };
    const [status, out] = await handle(body);
    res.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" });
    res.end(JSON.stringify(out));
    return;
  }
  if (url.pathname === "/" || url.pathname === "/index.html") {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(await readFile(path.join(root, "index.html")));
    return;
  }
  res.writeHead(404); res.end("Not found");
}).listen(port, () => console.log(`Head of the River on http://localhost:${port}`));
