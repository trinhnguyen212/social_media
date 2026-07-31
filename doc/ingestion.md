# Data Ingestion — Facebook Graph API

## Overview

The ingestion layer pulls posts and comments from Facebook public pages (fire & emergency services) via the Facebook Graph API and lands raw JSON into Azure Blob Storage.

---

## Authentication

- **Token type:** Long-lived Page Access Token stored in `FACEBOOK_ACCESS_TOKEN`
- Tokens are invalidated if the generating user loses admin access or revokes app access — no push notification exists for this. Detect it via HTTP 401 or error code 190 and halt immediately; do not silently skip pages.
- A token validation call (`GET /{page_id}?fields=id`) runs at pipeline start before any data is written. If it fails, the run aborts and the checkpoint is not advanced.

---

## Facebook Graph API Endpoints

### 1. Page Posts — `/posts`

```
GET /{page_id}/posts
    ?fields=id,message,story,created_time,shares,attachments,
            comments.summary(total_count),
            reactions.type(LIKE).limit(0).summary(total_count).as(reaction_like),
            reactions.type(LOVE).limit(0).summary(total_count).as(reaction_love),
            reactions.type(HAHA).limit(0).summary(total_count).as(reaction_haha),
            reactions.type(WOW).limit(0).summary(total_count).as(reaction_wow),
            reactions.type(SAD).limit(0).summary(total_count).as(reaction_sad),
            reactions.type(ANGRY).limit(0).summary(total_count).as(reaction_angry)
    &since={unix_timestamp}
    &limit=100
    &access_token={token}
```

- `since` must be a **Unix integer timestamp** — filters by `created_time`, returns only posts created after that time
- `.limit(0).summary(total_count)` on reactions: suppresses individual reaction records, returns only the aggregate count. `summary(total_count)` is the documented form — `summary(true)` also works but is not in the official spec
- LIKE count includes both "Like" and "Care" reactions combined — Facebook platform constraint, not configurable

### 2. Single Post — `/{post_id}` (Track 2 refresh)

```
GET /{post_id}
    ?fields=id,message,story,created_time,shares,attachments,
            comments.summary(total_count),
            reactions.type(LIKE).limit(0).summary(total_count).as(reaction_like),
            reactions.type(LOVE).limit(0).summary(total_count).as(reaction_love),
            reactions.type(HAHA).limit(0).summary(total_count).as(reaction_haha),
            reactions.type(WOW).limit(0).summary(total_count).as(reaction_wow),
            reactions.type(SAD).limit(0).summary(total_count).as(reaction_sad),
            reactions.type(ANGRY).limit(0).summary(total_count).as(reaction_angry)
    &access_token={token}
```

- Same fields as `/posts` — used in Track 2 to check whether reaction counts or `comment_count` have changed since the last run
- A new file is written only if at least one count differs from the stored checkpoint value

### 3. Post Comments — `/comments`

```
GET /{post_id}/comments
    ?fields=id,message,created_time,from,parent,comment_count,
            reactions.type(LIKE).limit(0).summary(total_count).as(reaction_like),
            reactions.type(LOVE).limit(0).summary(total_count).as(reaction_love),
            reactions.type(HAHA).limit(0).summary(total_count).as(reaction_haha),
            reactions.type(WOW).limit(0).summary(total_count).as(reaction_wow),
            reactions.type(SAD).limit(0).summary(total_count).as(reaction_sad),
            reactions.type(ANGRY).limit(0).summary(total_count).as(reaction_angry)
    &filter=stream
    &since={unix_timestamp}
    &limit=100
    &access_token={token}
```

- `filter=stream` returns all comment levels (top-level + replies) in a single flat chronological list. Without it, only top-level comments are returned.
- `parent` field on replies identifies their parent comment — use it to reconstruct hierarchy. Top-level comments have no `parent` field.
- `since` filters by comment `created_time`
- Same LIKE + Care note applies

### 3. Page Feed — `/feed` (not used)

```
GET /{page_id}/feed
    ?since={unix_timestamp}
    &limit=100
    &access_token={token}
```

The feed endpoint behaves differently depending on whether `since` is passed:

- **Without `since`:** Returns cursor-based pagination of recent posts, ordered by creation time. Always returns the same posts on repeat calls until new posts are created.
- **With `since`:** Switches to time-based pagination and filters by the **post's own `created_time`**. A post created before `since` will NOT appear even if it received new comments or reactions after that time.

This makes the feed endpoint unsuitable for detecting new activity on existing posts — which is the main reason Track 2 is needed. Track 2 uses the checkpoint-based approach instead (see below).

---

## Pagination

Facebook uses **cursor-based pagination** by default. Adding `since` or `until` switches to **time-based pagination** — these modes are incompatible.

- Cursor-based: `paging.cursors.after` / `paging.cursors.before`
- Time-based: `paging.next` / `paging.previous`

Always follow the `next` URL the API returns. Never construct your own paginated URLs.

**Stop condition:** Stop when `paging.next` is absent — not when `data` is empty. Facebook returns partial or empty pages (due to privacy filtering) while still providing a `next` link.

---

## Rate Limiting

| Header | Scope |
|---|---|
| `X-Business-Use-Case-Usage` | **Primary limit** for page-token requests — `call_count`, `total_cputime`, `total_time` as % of hourly budget |
| `X-App-Usage` | App-level — secondary signal |

The legacy "200 calls/hour" figure is the app-level formula. The applicable limit for this pipeline is the **BUC limit** in `X-Business-Use-Case-Usage`.

- Back off proactively when any BUC dimension exceeds 75%
- HTTP 429 or `Application request limit` body → exponential backoff (base 2s, up to 5 retries)
- 0.5s sleep between paginated requests
- `estimated_time_to_regain_access` in the BUC header gives the recommended wait in seconds

---

## Incremental Loading

Facebook provides no changelog API. You cannot ask "what changed since timestamp X across all posts." New posts are discovered via `/posts?since=`, but new comments on existing posts require re-checking each post individually.

### Two-Track Strategy

**Track 1 — New posts**

```
GET /{page_id}/posts?since={last_run_ts}
```

Returns only posts created after the checkpoint. For each new post: fetch **all** its comments with no `since` filter (the post is new, all its comments are new). Add each new post ID and its full state (`created_time`, `comment_count`, individual reaction counts) to `known_posts` in the checkpoint.

**Track 2 — Known posts: reaction refresh + new comments**

For each post in `known_posts` (checkpoint from previous run):

```
1. GET /{post_id}?fields={POST_FIELDS}
   Compare reaction_like, reaction_love, reaction_haha, reaction_wow,
   reaction_sad, reaction_angry, comment_count against stored checkpoint values.
   → if any count changed: write new post file, update checkpoint state
   → if unchanged: skip write

2. GET /{post_id}/comments?since={last_run_ts}
   → write new comment file if any new comments returned
```

`known_posts` comes from the checkpoint written at the end of the **previous run** — it does not contain posts discovered by Track 1 in the current run. No exclusion needed.

If `ACTIVE_WINDOW_DAYS` is set, posts outside the window are skipped **before any API call** — no point refreshing posts that will be pruned from the checkpoint anyway.

Track 2 is skipped on the first run (no `last_loaded_at` exists yet).

### Full run flow

```
For each page:

  Track 1 — new posts
  ├── GET /{page_id}/posts?since={last_run_ts}
  ├── Write post records
  ├── Add new post IDs + full state (created_time, comment_count,
  │   reaction_like/love/haha/wow/sad/angry) to updated_known_posts
  └── For each new post:
        GET /{post_id}/comments      (no since — fetch ALL)
        Write comment records

  Track 2 — reaction refresh + new comments  [skipped on first run]
  │   known_posts = checkpoint from previous run
  │   does NOT contain Track 1 new posts → no exclusion needed
  │   if ACTIVE_WINDOW_DAYS set → skip posts outside window before any API call
  └── For each post_id in known_posts (within window if ACTIVE_WINDOW_DAYS set):
        GET /{post_id}?fields={POST_FIELDS}
        Compare each reaction count + comment_count vs stored checkpoint values
        → if any changed: write new post file, update checkpoint state
        → if unchanged: skip
        GET /{post_id}/comments?since={last_run_ts}
        Write comment records if any returned

  After all pages complete:
  └── write checkpoint:
        last_loaded_at = run_ts
        known_posts = previous known_posts (updated counts) + new posts from Track 1
        (pruned by ACTIVE_WINDOW_DAYS if set)
```

---

## Reaction Count Tracking

### Post-level reactions *(tracked)*

Track 2 re-fetches every known post on each run and compares the following counts against the stored checkpoint values:

| Field | Tracks |
|---|---|
| `reaction_like` | LIKE + CARE combined (Facebook constraint) |
| `reaction_love` | LOVE |
| `reaction_haha` | HAHA |
| `reaction_wow` | WOW |
| `reaction_sad` | SAD |
| `reaction_angry` | ANGRY |
| `comment_count` | Total comment count on the post |

Each field is compared individually. If **any single field** changes, a new post file is written and the checkpoint state is updated with the latest counts. If nothing changed, no file is written.

Checkpoint stores the last-known value for each field per post:
```json
"111222333444_987654321": {
  "created_time": "2026-03-01T08:00:00+0000",
  "comment_count": 10,
  "reaction_like": 5,
  "reaction_love": 2,
  "reaction_haha": 0,
  "reaction_wow": 1,
  "reaction_sad": 3,
  "reaction_angry": 0
}
```

**Migration note:** Checkpoints written before this feature was added stored only `created_time` as a plain string. On first run after migration, all counts default to 0, which causes every known post to be re-fetched and re-written once as a one-time catch-up.

---

### Comment-level limitations *(snapshot only)*

Comments are captured when first seen (new comment on a known post). Two types of changes on existing comments are not detected:

**1. Edited comment text**

Facebook does not provide a changelog for comment edits. `created_time` does not change when a comment is edited. The `since` filter will never surface an edited comment.

The only way to detect edits is to re-fetch all comments (no `since` filter) and compare message text — or a hash of it — against a stored value. This requires persisting text/hashes per comment in the checkpoint, which grows large quickly for pages with many comments.

**Current behaviour:** Comment text is captured once at first fetch and never updated. If a commenter edits their message, the stored text remains the original.

**2. Comment reaction counts**

Adding a reaction to an existing comment does not change its `created_time`. The `since` filter misses reaction count changes entirely.

The only way to detect reaction changes is to re-fetch all comments (no `since` filter) and compare individual reaction counts (`reaction_like`, `reaction_love`, etc.) against stored values per comment. This carries the same API and checkpoint cost as detecting edits — one full comment fetch per known post per run regardless of whether anything changed.

**Current behaviour:** Comment reaction counts are captured once at first fetch and never updated. For sentiment analysis where comment reactions are a secondary signal, stale counts are unlikely to affect classification.

---

### Option 2 — Full snapshot within active window on every run

Rather than trying to detect individual changes, re-fetch everything within `ACTIVE_WINDOW_DAYS` on every run with no `since` filter — all posts and all their comments. Write all records unconditionally and let Snowflake MERGE handle idempotency (MERGE on primary key absorbs duplicates and updates any changed fields automatically).

```
For each post in known_posts within ACTIVE_WINDOW_DAYS:
  GET /{post_id}?fields={POST_FIELDS}   → write post record (always)
  GET /{post_id}/comments               → write all comments (no since)
```

**What this captures that Option 1 misses:**
- Edited comment text
- Reaction count changes on existing comments
- Any other field change on a post or comment within the window

**Trade-offs:**

| | Option 1 (current) | Option 2 |
|---|---|---|
| **API calls** | 1 post + N new comments per known post | 1 post + all comments per known post |
| **Blob writes** | Only on change | Every run for every post in window |
| **Missed changes** | Comment edits, comment reactions | Nothing within the window |
| **Complexity** | Checkpoint stores counts for comparison | No comparison logic needed — Snowflake handles it |
| **Dependency** | Works without `ACTIVE_WINDOW_DAYS` | Heavily relies on `ACTIVE_WINDOW_DAYS` being set — without a window, every post ever seen is re-fetched every run |

**Not currently implemented.** Implement if comment-level completeness (edited text, reaction counts) becomes a requirement.

---

**Summary of what is and is not captured:**

| Entity | What is captured | What is not captured |
|---|---|---|
| Post | New posts, all reaction count changes, comment count changes | Message edits (out of scope by design) |
| Comment | New comments on known posts | Edited comment text, reaction count changes on existing comments |

---

## Checkpoint State

Stored in Azure Blob Storage alongside the raw data:

```
checkpoints/facebook/state.json
```

```json
{
  "last_loaded_at": "2026-04-03T10:00:00Z",
  "known_posts": {
    "111222333444": {
      "111222333444_987654321": {
        "created_time": "2026-03-01T08:00:00+0000",
        "comment_count": 10,
        "reaction_like": 5,
        "reaction_love": 2,
        "reaction_haha": 0,
        "reaction_wow": 1,
        "reaction_sad": 3,
        "reaction_angry": 0
      }
    }
  }
}
```

- `last_loaded_at` — passed as `since=` to Track 1 and Track 2 comment fetches on the next run
- `known_posts` — keyed by `page_id` → `{ post_id: state }` where state holds `created_time` and the last-known reaction counts and `comment_count`. Used by Track 2 for change detection and window filtering.
- If `ACTIVE_WINDOW_DAYS` is set, posts with `created_time` older than the window are pruned at write time. If not set, all posts are retained indefinitely.
- Written **only after all blob writes succeed**. If the run fails mid-way, the checkpoint stays at the previous value and the next run re-processes from the last successful point.

**Why blob storage and not Snowflake:** Ingestion depends only on Azure Blob. If Snowflake is down, ingestion still runs and data still lands. Snowflake loads independently via COPY INTO.

---

## Raw Data Layout

### File paths
```
raw/facebook/{page_id}/posts/{year}/{month}/{day}/
  posts_{page_id}_{timestamp}.json

raw/facebook/{page_id}/comments/{year}/{month}/{day}/
  comments_{page_id}_{post_id}_{timestamp}.json
```

### Metadata envelope
Every file is wrapped in an envelope:
```json
{
  "meta": {
    "source": "facebook_graph_api",
    "source_type": "posts",
    "page_id": "111222333444",
    "since": "2026-04-02T10:00:00Z",
    "run_timestamp": "2026-04-03T10:00:00Z",
    "record_count": 12
  },
  "data": [ ...raw Facebook objects... ]
}
```

### Post fields
| Field | Type | Notes |
|---|---|---|
| `id` | string | `{page_id}_{post_id}` |
| `message` | string | Post body text |
| `story` | string | Auto-generated text if no message |
| `created_time` | ISO 8601 | |
| `reaction_like` … `reaction_angry` | object | `total_count` in summary |
| `shares` | object | Share count |
| `comments` | object | `total_count` summary only |
| `attachments` | object | Media/link attachments |

### Comment fields
| Field | Type | Notes |
|---|---|---|
| `id` | string | |
| `message` | string | **Primary input for sentiment analysis** |
| `created_time` | ISO 8601 | |
| `from` | object | Author name and ID |
| `reaction_like` … `reaction_angry` | object | `total_count` in summary |
| `comment_count` | int | Number of replies |
| `parent` | object | Present on replies only — parent comment ID |

---

## Error Handling

| Scenario | Handling |
|---|---|
| Token expired / invalid | Halt pipeline — checkpoint not advanced |
| Rate limit (429 or body message) | Exponential backoff, up to 5 retries |
| Network timeout | Retry with backoff, 3 attempts, then fail batch |
| Empty API response | Log, skip — not an error |
| Blob write failure | Retry once; dead-letter locally if still failing |

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `FACEBOOK_ACCESS_TOKEN` | Yes | Long-lived Page Access Token |
| `FACEBOOK_API_VERSION` | No | Graph API version, default `v19.0` |
| `FACEBOOK_PAGE_IDS` | Yes | Comma-separated page IDs to monitor |
| `ACTIVE_WINDOW_DAYS` | No | If set, prune posts older than N days from `known_posts` at checkpoint write time |
| `AZURE_STORAGE_ACCOUNT_NAME` | Yes | Azure storage account |
| `AZURE_BLOB_CONTAINER_NAME` | Yes | Target container |
| `AZURE_BLOB_SAS_TOKEN` | Yes | SAS token with read + write scope |
| `OUTPUT_DIR` | No | Local output path (dev only), default `data/raw` |
| `CHECKPOINT_DIR` | No | Local checkpoint path (dev only), default `checkpoints` |

---

## Known API Quirks

### Comments pagination leaks past the `since` boundary

Subsequent pages of `GET /{post_id}/comments?since={ts}` can return records older than `ts` due to cursor drift. The client filters each batch by `created_time >= since` as a safety net. Only affects posts with more than 100 comments.

### `filter=stream` flattens replies

Returns all levels in a single chronological list. Use `parent.id` to reconstruct hierarchy — do not rely on list order alone.

### LIKE reaction includes "Care"

Facebook combines LIKE and CARE into one count — no way to separate them via the Graph API.

### ~600 posts per year cap

Facebook imposes approximately 600 ranked posts per year on the `/posts` edge. Unlikely to affect low-volume fire and emergency pages.

---

## Things to Handle Outside the Code

### Page Public Content Access

Reading posts from public pages you do not administer requires the **"Page Public Content Access"** feature approved by Meta via App Review. Without it, API calls return empty results or permission errors with no clear message. Test with pages your own account admins during development.

Required permissions: `pages_read_engagement`, `pages_read_user_content`

### Checkpoint overlap buffer

Comments posted right at the `last_loaded_at` boundary may be missed due to clock skew between the API and pipeline. Recommended: subtract a small buffer (5–10 minutes) when passing `since=`. Duplicates are absorbed by dbt MERGE in Snowflake.

### Webhooks as a future alternative to Track 2 polling

Meta Webhooks (`feed` subscription) push real-time comment notifications at zero API quota cost. Track 2 currently polls one API call per known post per run. Webhooks would eliminate this entirely but require a persistent HTTPS listener. Keep polling for now; add webhooks when a service endpoint is available.
