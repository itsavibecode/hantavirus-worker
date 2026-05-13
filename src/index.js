/* Hantavirus Monitor — ingest worker
 *
 * Cron-driven Cloudflare Worker that:
 *   1. Pulls fresh items from public-health RSS sources (ProMED, Google News,
 *      WHO, state DOH press feeds) every 3 hours.
 *   2. Filters for hantavirus mentions.
 *   3. Deduplicates against events already in Firestore.
 *   4. Passes each new item to Claude Haiku to extract structured fields
 *      (severity, lat/lng, location, strain, categories).
 *   5. Writes normalised event records to Firestore at /events/{eventId}.
 *
 * The Hantavirus Monitor SPA at bookhockeys.com/hantavirus reads from the
 * same Firestore collection at page-load time (public read), replacing the
 * v0.2.x hardcoded EVENTS[] array.
 *
 * HTTP endpoints:
 *   GET  /healthz                          — service health
 *   GET  /preview?source=<id>              — fetch + classify one source, do NOT write
 *   POST /ingest                           — run the full ingest pipeline NOW
 *   GET  /events?limit=N                   — read the most recent N events (CORS-friendly fallback for the SPA)
 *
 * Secrets (configure via `wrangler secret put`):
 *   FIREBASE_SERVICE_ACCOUNT_JSON   full Firebase service-account JSON
 *   ANTHROPIC_API_KEY               sk-ant-... key for the Haiku classifier
 */

const FIRESTORE_BASE = 'https://firestore.googleapis.com/v1';
const ANTHROPIC_BASE = 'https://api.anthropic.com/v1';
const ANTHROPIC_VERSION = '2023-06-01';

// Hantavirus keyword filter. ProMED items are already on-topic, but Google News
// and WHO feeds return everything — we keep only items whose title or summary
// mention any of these terms (case-insensitive).
const HANTAVIRUS_TERMS = [
  'hantavirus', 'hanta virus', 'hps', 'hcps', 'hfrs',
  'sin nombre', 'andes virus', 'hantaan', 'puumala', 'seoul virus',
  'choclo virus', 'pulmonary syndrome', 'hemorrhagic fever with renal',
];

// RSS sources to ingest from. Each entry is normalised by the same parser.
const SOURCES = [
  {
    id: 'promed',
    name: 'ProMED-mail',
    url: 'https://promedmail.org/promed-rss/',
    weight: 1.0,             // priority for dedup tie-breaking
    keywordFilter: false,    // ProMED's own feed only returns relevant items
  },
  {
    id: 'gnews-hantavirus',
    name: 'Google News — hantavirus',
    url: 'https://news.google.com/rss/search?q=hantavirus&hl=en-US&gl=US&ceid=US:en',
    weight: 0.7,
    keywordFilter: true,
  },
  {
    id: 'gnews-hfrs',
    name: 'Google News — HFRS',
    url: 'https://news.google.com/rss/search?q=%22HFRS%22+hemorrhagic+fever+renal&hl=en-US&gl=US&ceid=US:en',
    weight: 0.7,
    keywordFilter: true,
  },
  {
    id: 'gnews-andes',
    name: 'Google News — Andes virus',
    url: 'https://news.google.com/rss/search?q=%22Andes+virus%22+OR+%22virus+andes%22&hl=en-US&gl=US&ceid=US:en',
    weight: 0.7,
    keywordFilter: true,
  },
  {
    id: 'who-don',
    name: 'WHO Disease Outbreak News',
    url: 'https://www.who.int/rss-feeds/news-english.xml',
    weight: 0.9,
    keywordFilter: true,
  },
];

// ─────────────────────────────────────────────────────────────────────
// Worker entrypoints
// ─────────────────────────────────────────────────────────────────────

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const origin = request.headers.get('Origin') || '';

    // Preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders(origin, env) });
    }

    try {
      let body;
      switch (url.pathname) {
        case '/':
        case '/healthz':
          body = { ok: true, name: 'hantavirus-worker', version: '0.1.0' };
          break;

        case '/preview': {
          const sourceId = url.searchParams.get('source') || 'promed';
          const source = SOURCES.find((s) => s.id === sourceId);
          if (!source) {
            return json({ error: 'unknown source', valid: SOURCES.map((s) => s.id) }, 400, origin, env);
          }
          body = await previewSource(source, env);
          break;
        }

        case '/ingest': {
          if (request.method !== 'POST') {
            return json({ error: 'POST required' }, 405, origin, env);
          }
          body = await runIngest(env, ctx);
          break;
        }

        case '/events': {
          const limit = Math.min(parseInt(url.searchParams.get('limit') || '200', 10) || 200, 500);
          body = await readEvents(env, limit);
          break;
        }

        default:
          return json({ error: 'not found' }, 404, origin, env);
      }
      return json(body, 200, origin, env);
    } catch (err) {
      console.error('fetch error:', err);
      return json({ error: String(err.message || err) }, 500, origin, env);
    }
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(
      runIngest(env, ctx).catch((err) => {
        console.error('Cron ingest failed:', err);
      }),
    );
  },
};

// ─────────────────────────────────────────────────────────────────────
// Ingest pipeline
// ─────────────────────────────────────────────────────────────────────

async function runIngest(env, ctx) {
  const started = Date.now();
  const maxTotal = parseInt(env.MAX_ITEMS_PER_RUN || '30', 10);
  const maxPerSource = parseInt(env.MAX_ITEMS_PER_SOURCE || '12', 10);

  // 1. Fetch every source in parallel.
  const fetched = await Promise.all(
    SOURCES.map(async (source) => {
      try {
        const items = await fetchSourceItems(source, maxPerSource);
        return { source, items, error: null };
      } catch (err) {
        console.warn('source ' + source.id + ' failed: ' + err.message);
        return { source, items: [], error: String(err.message || err) };
      }
    }),
  );

  // 2. Flatten + apply per-source keyword filter where required.
  const candidates = [];
  for (const { source, items } of fetched) {
    for (const it of items) {
      if (source.keywordFilter && !matchesHantavirusTerms(it.title + ' ' + it.summary)) continue;
      candidates.push({ ...it, sourceId: source.id, sourceName: source.name, sourceWeight: source.weight });
    }
  }

  // 3. Dedup by canonical URL hash; if the same story appears across sources, keep the highest-weight one.
  const deduped = dedupByUrl(candidates);

  // 4. Drop anything already in Firestore (by deterministic event ID).
  const accessToken = await getServiceAccountToken(env);
  const existingIds = await listExistingEventIds(env, accessToken);
  const newOnes = deduped.filter((c) => !existingIds.has(deterministicEventId(c)));

  // 5. Cap to MAX_ITEMS_PER_RUN — fresh-first.
  newOnes.sort((a, b) => (b.pubDate || '').localeCompare(a.pubDate || ''));
  const toIngest = newOnes.slice(0, maxTotal);

  // 6. Classify + write.
  const written = [];
  const errors = [];
  for (const item of toIngest) {
    try {
      const classified = (env.CLASSIFIER_ENABLED === 'true')
        ? await classifyItem(item, env)
        : heuristicClassify(item);
      const eventDoc = buildEventDoc(item, classified);
      await firestoreWriteEvent(env, accessToken, eventDoc);
      written.push(eventDoc.id);
    } catch (err) {
      console.error('ingest item failed:', err);
      errors.push({ url: item.url, error: String(err.message || err) });
    }
  }

  return {
    ok: true,
    durationMs: Date.now() - started,
    sourcesFetched: fetched.map((f) => ({ id: f.source.id, count: f.items.length, error: f.error })),
    candidates: candidates.length,
    deduped: deduped.length,
    alreadyKnown: deduped.length - newOnes.length,
    classified: written.length,
    errors,
    writtenIds: written,
  };
}

async function previewSource(source, env) {
  const items = await fetchSourceItems(source, 6);
  const out = [];
  for (const it of items) {
    if (source.keywordFilter && !matchesHantavirusTerms(it.title + ' ' + it.summary)) {
      out.push({ ...it, classified: null, skipped: 'no keyword match' });
      continue;
    }
    try {
      const classified = (env.CLASSIFIER_ENABLED === 'true')
        ? await classifyItem({ ...it, sourceId: source.id, sourceName: source.name }, env)
        : heuristicClassify({ ...it, sourceId: source.id });
      out.push({ ...it, classified });
    } catch (err) {
      out.push({ ...it, classified: null, error: String(err.message || err) });
    }
  }
  return { source: source.id, items: out };
}

// ─────────────────────────────────────────────────────────────────────
// RSS fetch + parse
// ─────────────────────────────────────────────────────────────────────

async function fetchSourceItems(source, maxItems) {
  const resp = await fetch(source.url, {
    headers: {
      'User-Agent': 'hantavirus-worker/0.1 (+https://bookhockeys.com/hantavirus/)',
      'Accept': 'application/rss+xml, application/xml, text/xml, */*',
    },
    cf: { cacheTtl: 600, cacheEverything: true },
  });
  if (!resp.ok) throw new Error(source.id + ' HTTP ' + resp.status);
  const xml = await resp.text();
  return parseRssItems(xml).slice(0, maxItems);
}

/* Very small RSS / Atom item extractor. We do not need full XML compliance —
 * RSS feeds are predictable. We extract <item> blocks (or <entry> for Atom),
 * then pull title/link/description/pubDate/guid via regex. CDATA is unwrapped;
 * common HTML entities are decoded; HTML tags inside description are stripped. */
function parseRssItems(xml) {
  const out = [];
  const isAtom = /<feed[\s>]/i.test(xml) && !/<rss[\s>]/i.test(xml);
  const itemRe = isAtom ? /<entry\b[\s\S]*?<\/entry>/gi : /<item\b[\s\S]*?<\/item>/gi;
  const blocks = xml.match(itemRe) || [];
  for (const block of blocks) {
    const title = stripHtml(decodeEntities(unwrapCdata(matchFirst(block, /<title\b[^>]*>([\s\S]*?)<\/title>/i)))).trim();
    let link = matchFirst(block, /<link\b[^>]*>([\s\S]*?)<\/link>/i).trim();
    if (!link || link.startsWith('<')) {
      // Atom-style: <link href="..." />
      link = (block.match(/<link[^>]*href="([^"]+)"[^>]*\/?>/i) || [])[1] || '';
    }
    const desc = stripHtml(decodeEntities(unwrapCdata(matchFirst(block, /<description\b[^>]*>([\s\S]*?)<\/description>/i) || matchFirst(block, /<summary\b[^>]*>([\s\S]*?)<\/summary>/i)))).trim();
    const pubDate = (matchFirst(block, /<pubDate\b[^>]*>([\s\S]*?)<\/pubDate>/i) || matchFirst(block, /<updated\b[^>]*>([\s\S]*?)<\/updated>/i) || matchFirst(block, /<published\b[^>]*>([\s\S]*?)<\/published>/i)).trim();
    const guid = matchFirst(block, /<guid\b[^>]*>([\s\S]*?)<\/guid>/i).trim() || (block.match(/<id\b[^>]*>([\s\S]*?)<\/id>/i) || [])[1] || '';
    if (!title || !link) continue;
    out.push({
      title,
      url: canonicalUrl(link),
      summary: desc.slice(0, 1500),
      pubDate: normaliseDate(pubDate),
      guid: (guid || link).trim(),
    });
  }
  return out;
}

function matchFirst(s, re) { const m = s.match(re); return m ? m[1] : ''; }
function unwrapCdata(s) { return (s || '').replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1'); }
function decodeEntities(s) {
  return (s || '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n, 10)))
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCharCode(parseInt(h, 16)));
}
function stripHtml(s) { return (s || '').replace(/<[^>]+>/g, ''); }

function canonicalUrl(u) {
  // Google News wraps each item URL in a redirect; strip the wrapper to get the
  // publisher URL when present.
  if (/news\.google\.com/.test(u)) {
    const m = u.match(/[?&]url=([^&]+)/);
    if (m) return decodeURIComponent(m[1]);
  }
  // Drop common analytics params.
  try {
    const parsed = new URL(u);
    ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content', 'CMP', 'cmp', 'mbid']
      .forEach((p) => parsed.searchParams.delete(p));
    return parsed.toString();
  } catch (_) {
    return u;
  }
}

function normaliseDate(raw) {
  if (!raw) return new Date().toISOString().slice(0, 10);
  const d = new Date(raw);
  if (isNaN(d)) return new Date().toISOString().slice(0, 10);
  return d.toISOString().slice(0, 10);
}

function matchesHantavirusTerms(text) {
  const t = (text || '').toLowerCase();
  return HANTAVIRUS_TERMS.some((kw) => t.indexOf(kw) !== -1);
}

function dedupByUrl(items) {
  const byKey = new Map();
  for (const it of items) {
    const key = it.url || it.guid;
    const existing = byKey.get(key);
    if (!existing || (it.sourceWeight || 0) > (existing.sourceWeight || 0)) {
      byKey.set(key, it);
    }
  }
  return Array.from(byKey.values());
}

// Deterministic event ID from canonical URL — so re-running ingest never
// creates duplicates. SHA-1 hex truncated to 16 chars is plenty unique.
async function sha1Hex(s) {
  const buf = await crypto.subtle.digest('SHA-1', new TextEncoder().encode(s));
  const bytes = new Uint8Array(buf);
  let hex = '';
  for (let i = 0; i < bytes.length; i++) hex += bytes[i].toString(16).padStart(2, '0');
  return hex;
}
function deterministicEventId(item) { return item.__id || (item.__id = quickHash(item.url || item.guid || item.title)); }
function quickHash(s) {
  // Tiny non-cryptographic hash for the dedup key. Stable across runs.
  let h = 0;
  for (let i = 0; i < s.length; i++) { h = (h << 5) - h + s.charCodeAt(i); h |= 0; }
  return 'e_' + (h >>> 0).toString(16);
}

// ─────────────────────────────────────────────────────────────────────
// Classifier (Claude Haiku)
// ─────────────────────────────────────────────────────────────────────

const CLASSIFIER_SCHEMA = `Return ONLY a JSON object with these fields, no markdown:
{
  "title": string                          // concise headline, <= 110 chars
  "summary": string                        // 1-2 sentences, factual, no editorialising
  "lat": number                            // approximate latitude of the event location
  "lng": number                            // approximate longitude
  "location": string                       // human-readable location, e.g. "Mono County, CA, USA"
  "region": "americas" | "eurasia"         // which hemispheric region
  "strain": string                         // best-guess strain, e.g. "Sin Nombre", "Hantaan", "Andes", "Puumala", "Seoul", "Choclo", or "unknown"
  "severity": 1 | 2 | 3                    // 1=confirmed recovered, 2=hospitalized/active, 3=death OR cluster >=3
  "cats": string[]                         // any of: CASE, DEATH, CLUSTER, ADVISORY, ENV, HPS, HFRS, ANDES, HANTAAN, PUUMALA, SEOUL, CHOCLO
  "discardReason": string | null           // set to a short reason if this item is NOT a real hantavirus event (e.g. opinion piece, off-topic, vaccine research only); else null
}`;

async function classifyItem(item, env) {
  const sys = 'You are a public-health surveillance classifier. Convert a raw news / press-release item into a structured hantavirus event record. Be conservative: if the item is opinion, vaccine research with no human case, or off-topic, set discardReason. Output JSON only, no prose.';
  const user = [
    'SOURCE: ' + (item.sourceName || item.sourceId || 'unknown'),
    'PUBLISHED: ' + (item.pubDate || 'unknown'),
    'URL: ' + (item.url || ''),
    'TITLE: ' + (item.title || ''),
    'SUMMARY:',
    (item.summary || '').slice(0, 1200),
    '',
    'SCHEMA:',
    CLASSIFIER_SCHEMA,
  ].join('\n');

  const resp = await fetch(ANTHROPIC_BASE + '/messages', {
    method: 'POST',
    headers: {
      'x-api-key': env.ANTHROPIC_API_KEY,
      'anthropic-version': ANTHROPIC_VERSION,
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      model: env.CLASSIFIER_MODEL || 'claude-haiku-4-5',
      max_tokens: 600,
      system: sys,
      messages: [{ role: 'user', content: user }],
    }),
  });
  if (!resp.ok) {
    const t = await resp.text();
    throw new Error('classifier HTTP ' + resp.status + ': ' + t.slice(0, 300));
  }
  const data = await resp.json();
  const text = (data.content || []).map((c) => c.text || '').join('').trim();
  // Tolerate occasional ```json fences.
  const jsonText = text.replace(/^```(?:json)?\s*/, '').replace(/```\s*$/, '');
  let parsed;
  try { parsed = JSON.parse(jsonText); }
  catch (e) { throw new Error('classifier returned non-JSON: ' + text.slice(0, 200)); }
  return parsed;
}

// Fallback used when CLASSIFIER_ENABLED='false'. Captures the bare minimum
// so manual admin work has something to fix; severity defaults to 2.
function heuristicClassify(item) {
  const text = (item.title + ' ' + item.summary).toLowerCase();
  const cats = [];
  if (/death|died|fatal|fatalit|killed by/.test(text)) cats.push('DEATH');
  if (/cluster|outbreak|multiple cases/.test(text)) cats.push('CLUSTER');
  if (/case|patient|infected|confirmed/.test(text)) cats.push('CASE');
  if (/advisory|warning|alert|guidance/.test(text)) cats.push('ADVISORY');
  let strain = 'unknown';
  if (/sin nombre/.test(text)) strain = 'Sin Nombre';
  else if (/andes/.test(text)) strain = 'Andes';
  else if (/hantaan/.test(text)) strain = 'Hantaan';
  else if (/puumala/.test(text)) strain = 'Puumala';
  else if (/seoul/.test(text)) strain = 'Seoul';
  else if (/choclo/.test(text)) strain = 'Choclo';
  return {
    title: item.title.slice(0, 110),
    summary: (item.summary || '').slice(0, 280),
    lat: 0, lng: 0, location: 'unknown',
    region: /hfrs|hantaan|puumala|seoul/.test(text) ? 'eurasia' : 'americas',
    strain,
    severity: cats.includes('DEATH') ? 3 : (cats.includes('CASE') ? 2 : 1),
    cats: cats.length ? cats : ['CASE'],
    discardReason: null,
  };
}

// ─────────────────────────────────────────────────────────────────────
// Firestore: build event doc + write + list-existing
// ─────────────────────────────────────────────────────────────────────

function buildEventDoc(item, classified) {
  const id = deterministicEventId(item);
  return {
    id,
    fields: {
      id: { stringValue: id },
      date: { stringValue: item.pubDate || new Date().toISOString().slice(0, 10) },
      title: { stringValue: classified.title || item.title },
      summary: { stringValue: classified.summary || item.summary || '' },
      sourceUrl: { stringValue: item.url || '' },
      sourceName: { stringValue: item.sourceName || item.sourceId || '' },
      sourceId: { stringValue: item.sourceId || '' },
      location: { stringValue: classified.location || 'unknown' },
      region: { stringValue: classified.region || 'americas' },
      lat: { doubleValue: numOrZero(classified.lat) },
      lng: { doubleValue: numOrZero(classified.lng) },
      sev: { integerValue: String(intInRange(classified.severity, 1, 3, 2)) },
      cats: { arrayValue: { values: (classified.cats || []).map((c) => ({ stringValue: String(c) })) } },
      strain: { stringValue: classified.strain || 'unknown' },
      classifiedAt: { timestampValue: new Date().toISOString() },
      classifierVersion: { stringValue: '0.1.0' },
      discardReason: classified.discardReason ? { stringValue: String(classified.discardReason) } : { nullValue: null },
      manualOverride: { booleanValue: false },
    },
  };
}

function numOrZero(n) { return Number.isFinite(Number(n)) ? Number(n) : 0; }
function intInRange(n, lo, hi, fallback) {
  const v = parseInt(n, 10);
  if (!Number.isFinite(v)) return fallback;
  return Math.max(lo, Math.min(hi, v));
}

async function firestoreWriteEvent(env, accessToken, doc) {
  const url = FIRESTORE_BASE + '/projects/' + env.FIREBASE_PROJECT_ID +
    '/databases/(default)/documents/events?documentId=' + encodeURIComponent(doc.id);
  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': 'Bearer ' + accessToken,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ fields: doc.fields }),
  });
  if (!resp.ok && resp.status !== 409) {
    const t = await resp.text();
    throw new Error('firestore write HTTP ' + resp.status + ': ' + t.slice(0, 300));
  }
  return true;
}

async function listExistingEventIds(env, accessToken) {
  // List only document NAMES (not the body) — cheaper. Paginate until done.
  const ids = new Set();
  let pageToken = '';
  for (let page = 0; page < 8; page++) {
    const url = FIRESTORE_BASE + '/projects/' + env.FIREBASE_PROJECT_ID +
      '/databases/(default)/documents/events?pageSize=300&mask.fieldPaths=id' +
      (pageToken ? '&pageToken=' + encodeURIComponent(pageToken) : '');
    const resp = await fetch(url, {
      headers: { 'Authorization': 'Bearer ' + accessToken },
    });
    if (!resp.ok) {
      if (resp.status === 404) return ids; // collection doesn't exist yet
      const t = await resp.text();
      throw new Error('firestore list HTTP ' + resp.status + ': ' + t.slice(0, 300));
    }
    const data = await resp.json();
    for (const d of (data.documents || [])) {
      const name = d.name || '';
      const id = name.split('/').pop();
      if (id) ids.add(id);
    }
    if (!data.nextPageToken) break;
    pageToken = data.nextPageToken;
  }
  return ids;
}

async function readEvents(env, limit) {
  const accessToken = await getServiceAccountToken(env);
  const url = FIRESTORE_BASE + '/projects/' + env.FIREBASE_PROJECT_ID +
    '/databases/(default)/documents:runQuery';
  const body = {
    structuredQuery: {
      from: [{ collectionId: 'events' }],
      orderBy: [{ field: { fieldPath: 'date' }, direction: 'DESCENDING' }],
      limit,
    },
  };
  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': 'Bearer ' + accessToken,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });
  if (!resp.ok) {
    const t = await resp.text();
    throw new Error('firestore query HTTP ' + resp.status + ': ' + t.slice(0, 300));
  }
  const data = await resp.json();
  const out = [];
  for (const row of (data || [])) {
    if (!row.document) continue;
    out.push(flattenFirestoreDoc(row.document));
  }
  return { count: out.length, events: out };
}

function flattenFirestoreDoc(doc) {
  const f = doc.fields || {};
  const v = (key) => {
    const x = f[key];
    if (!x) return null;
    if ('stringValue' in x) return x.stringValue;
    if ('integerValue' in x) return parseInt(x.integerValue, 10);
    if ('doubleValue' in x) return Number(x.doubleValue);
    if ('booleanValue' in x) return x.booleanValue;
    if ('timestampValue' in x) return x.timestampValue;
    if ('nullValue' in x) return null;
    if ('arrayValue' in x) return (x.arrayValue.values || []).map((vv) => vv.stringValue);
    return null;
  };
  return {
    id: v('id'), date: v('date'), title: v('title'), summary: v('summary'),
    sourceUrl: v('sourceUrl'), sourceName: v('sourceName'), sourceId: v('sourceId'),
    location: v('location'), region: v('region'), lat: v('lat'), lng: v('lng'),
    sev: v('sev'), cats: v('cats'), strain: v('strain'),
    classifiedAt: v('classifiedAt'), classifierVersion: v('classifierVersion'),
    discardReason: v('discardReason'), manualOverride: v('manualOverride'),
  };
}

// ─────────────────────────────────────────────────────────────────────
// Service-account JWT → Google OAuth2 access token (Firestore scope)
// Mirrors the stocks-worker pattern.
// ─────────────────────────────────────────────────────────────────────

let _accessTokenCache = null;

async function getServiceAccountToken(env) {
  if (_accessTokenCache && _accessTokenCache.expiresAt > Date.now() + 60_000) {
    return _accessTokenCache.token;
  }
  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT_JSON);
  const now = Math.floor(Date.now() / 1000);
  const claim = {
    iss: sa.client_email,
    scope: 'https://www.googleapis.com/auth/datastore',
    aud: 'https://oauth2.googleapis.com/token',
    exp: now + 3600,
    iat: now,
  };
  const enc = new TextEncoder();
  const headerB64 = b64url(btoa(JSON.stringify({ alg: 'RS256', typ: 'JWT' })));
  const claimB64 = b64url(btoa(JSON.stringify(claim)));
  const unsigned = headerB64 + '.' + claimB64;

  const privateKey = await pemToCryptoKey(sa.private_key);
  const sigBuf = await crypto.subtle.sign('RSASSA-PKCS1-v1_5', privateKey, enc.encode(unsigned));
  const sigB64 = b64url(arrayBufferToBase64(sigBuf));
  const jwt = unsigned + '.' + sigB64;

  const resp = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: 'grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=' + jwt,
  });
  if (!resp.ok) {
    const errTxt = await resp.text();
    throw new Error('Service-account token exchange failed: ' + resp.status + ' ' + errTxt);
  }
  const data = await resp.json();
  _accessTokenCache = {
    token: data.access_token,
    expiresAt: Date.now() + (data.expires_in - 120) * 1000,
  };
  return data.access_token;
}

function b64url(b64) { return b64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, ''); }
function arrayBufferToBase64(buf) {
  const bytes = new Uint8Array(buf);
  let bin = '';
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
  return btoa(bin);
}
async function pemToCryptoKey(pem) {
  const stripped = pem
    .replace('-----BEGIN PRIVATE KEY-----', '')
    .replace('-----END PRIVATE KEY-----', '')
    .replace(/\s/g, '');
  const der = Uint8Array.from(atob(stripped), (c) => c.charCodeAt(0));
  return crypto.subtle.importKey(
    'pkcs8',
    der,
    { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
    false,
    ['sign'],
  );
}

// ─────────────────────────────────────────────────────────────────────
// HTTP helpers
// ─────────────────────────────────────────────────────────────────────

function corsHeaders(origin, env) {
  const allowed = (env.ALLOWED_ORIGINS || '').split(',').map((s) => s.trim()).filter(Boolean);
  const ok = allowed.includes(origin) ? origin : (allowed[0] || '*');
  return {
    'Access-Control-Allow-Origin': ok,
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Max-Age': '600',
    'Vary': 'Origin',
  };
}

function json(body, status, origin, env) {
  return new Response(JSON.stringify(body, null, 2), {
    status,
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      ...corsHeaders(origin, env),
    },
  });
}
