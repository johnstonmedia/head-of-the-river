# SIRC Head of the River

A 3D rowing race game set at the Sydney International Regatta Centre. The game is a single self-contained `index.html` (three.js loaded from jsDelivr), deployed on Vercel.

## Online

Online mode has a lobby served by `api/net.js` (a Vercel Function; the logic is in `lib/netcore.js`):

- **Regattas**: create your own (name, course, boat class, conditions, entry cap, listed or private with a 4-letter code) or join one from the list.
- **Points series**: a regatta can run 3, 5 or 7 races scored 10, 8, 6, 5, 4, 3, 2, 1 by place, with a live table.
- **Chat**: lobby chat, chat inside each regatta, and in-race chat (press T).

The lobby only carries presence, matchmaking and chat. The races themselves run browser to browser over WebRTC through the host's browser, so the host keeps the page open. Direct connections can fail on some locked-down Wi-Fi; mobile data usually works. Invite codes (swapped by text) still work without the lobby.

### Storage

The lobby keeps its state in Redis when `KV_REST_API_URL`/`KV_REST_API_TOKEN` (or `UPSTASH_REDIS_REST_URL`/`UPSTASH_REDIS_REST_TOKEN`) are set: add **Upstash Redis** from the Vercel Marketplace to the project. Without them it falls back to memory, which works on one machine but on Vercel can split players across function instances.

### Running locally

```
npm run dev     # http://localhost:3000, game + lobby (memory storage)
```
