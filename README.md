# SIRC Head of the River

A 3D rowing race game set at the Sydney International Regatta Centre. The game is a single self-contained `index.html` (three.js loaded from jsDelivr), deployed on Vercel.

## Online

Online mode has a lobby served by `api/net.js` (a Vercel Function; the logic is in `lib/netcore.js`):

- **Regattas**: create your own (name, course, boat class, conditions, entry cap, listed or private with a 4-letter code) or join one from the list.
- **Points series**: a regatta can run 3, 5 or 7 races scored 10, 8, 6, 5, 4, 3, 2, 1 by place, with a live table.
- **Heats & finals**: the host makes a draw; one heat for lanes (up to 8 crews) or two heats, then Finals A and B seeded by heat time.
- **Online rankings**: an Elo rating per player (kept in Redis), from every regatta race between two or more people.
- **Chat**: lobby chat, chat inside each regatta, and in-race chat (press T).

The lobby only carries presence, matchmaking and chat. The races themselves run browser to browser over WebRTC through the host's browser, so the host keeps the page open. Direct connections can fail on some locked-down Wi-Fi; mobile data usually works. Invite codes (swapped by text) still work without the lobby.

### Relays (hotspots and mobile data)

Races connect browser to browser. On phone hotspots and some networks that can't go direct, so set up a TURN relay:
either Cloudflare TURN (`CF_TURN_KEY_ID` and `CF_TURN_API_TOKEN`) or any TURN service's fixed credentials
(`TURN_URLS` comma-separated, `TURN_USERNAME`, `TURN_CREDENTIAL`) as Vercel environment variables.

### Storage

The lobby keeps its state in Redis when `KV_REST_API_URL`/`KV_REST_API_TOKEN` (or `UPSTASH_REDIS_REST_URL`/`UPSTASH_REDIS_REST_TOKEN`) are set: add **Upstash Redis** from the Vercel Marketplace to the project. Without them it falls back to memory, which works on one machine but on Vercel can split players across function instances.

### Running locally

```
npm run dev     # http://localhost:3000, game + lobby (memory storage)
```
