# hantavirus-worker

Cloudflare Worker backend for the [Hantavirus Monitor](https://bookhockeys.com/hantavirus/) dashboard. Ingests hantavirus surveillance items from public RSS feeds, classifies them with Claude Haiku, and writes normalised event records to Firestore for the SPA to read.

## What it does

On a `5 */3 * * *` cron (every 3 hours, 5 minutes past), the Worker:

1. Fetches RSS from configured public-health sources (ProMED-mail, Google News, WHO Disease Outbreak News).
2. Applies a hantavirus keyword filter where the source isn't already topic-specific.
3. Deduplicates by canonical URL against events already in Firestore.
4. Sends each new item to [Claude Haiku](https://www.anthropic.com/) for structured extraction — severity tier (S1 / S2 / S3), strain, lat/lng, location, categories.
5. Writes the normalised event to `/events/{id}` in the `hantavirus-monitor` Firebase project.

Caps per run: 30 items total, 12 per source (override via `MAX_ITEMS_PER_RUN` / `MAX_ITEMS_PER_SOURCE` in `wrangler.toml`).

## HTTP endpoints

| Method | Path | What it does |
| --- | --- | --- |
| `GET` | `/healthz` | service health |
| `GET` | `/preview?source=promed` | fetch + classify one source, **does not write** — useful for tuning |
| `POST` | `/ingest` | run the full ingest pipeline now |
| `GET` | `/events?limit=200` | latest N events from Firestore (CORS-friendly fallback for the SPA) |

CORS: allowed origins are configured in `wrangler.toml` under `ALLOWED_ORIGINS`.

## Local dev

```sh
npm install
wrangler login                                       # if not already
wrangler secret put FIREBASE_SERVICE_ACCOUNT_JSON    # paste the full JSON from Firebase → Service Accounts
wrangler secret put ANTHROPIC_API_KEY                # paste your sk-ant-... key
wrangler dev                                         # local dev server
```

`wrangler dev` exposes the worker at `http://localhost:8787`. Hit `/preview?source=promed` to confirm RSS parsing + classification before letting the cron loose.

## Deploy

```sh
wrangler deploy
```

The deployed URL is `https://hantavirus-worker.<your-cf-subdomain>.workers.dev`.

## Firestore schema

Collection: `events`. Doc ID is a deterministic hash of the canonical source URL, so re-running ingest never creates duplicates.

```json
{
  "id": "e_3ab12c4d",
  "date": "2026-05-08",
  "title": "Third HPS death confirmed in Mono County",
  "summary": "Mono County Public Health confirmed a third HPS death in the Eastern Sierra...",
  "sourceUrl": "https://...",
  "sourceName": "ProMED-mail",
  "sourceId": "promed",
  "location": "Mammoth Lakes, CA, USA",
  "region": "americas",
  "lat": 37.6485,
  "lng": -118.9721,
  "sev": 3,
  "cats": ["CLUSTER", "DEATH", "HPS"],
  "strain": "Sin Nombre",
  "classifiedAt": "2026-05-08T12:00:00Z",
  "classifierVersion": "0.1.0",
  "discardReason": null,
  "manualOverride": false
}
```

If the classifier flags an item as off-topic (vaccine research, opinion piece, etc.) it sets `discardReason` and the SPA can filter those out by default.

## Secrets

| Secret | Where it comes from | What it's for |
| --- | --- | --- |
| `FIREBASE_SERVICE_ACCOUNT_JSON` | Firebase → Project Settings → Service Accounts → Generate new private key | Firestore admin via JWT bearer flow |
| `ANTHROPIC_API_KEY` | [console.anthropic.com](https://console.anthropic.com) | Haiku classifier |

Set both via `wrangler secret put`. Never paste them into chat or commit them.

## Permissions note

When you generate the Firebase service account, the auto-granted IAM role (Firebase Authentication Admin) **is not enough** to write Firestore. After creating the key, go to [console.cloud.google.com/iam-admin/iam](https://console.cloud.google.com/iam-admin/iam) and add the **Cloud Datastore User** role to the `firebase-adminsdk-...` principal. (Same trip-up as `usage-worker` got bit by in May 2026 — see that project's IAM note.)

## Changelog

### v0.1.0 &mdash; 2026-05-08
- First scaffold. ProMED + Google News (hantavirus / HFRS / Andes) + WHO Disease Outbreak News as sources. Claude Haiku classifier. Cron every 3 hours. `/preview`, `/ingest`, `/events`, `/healthz` HTTP endpoints. Deterministic-ID dedup.
