# Data Transformation

## Overview

Transformation converts raw **Facebook** JSON (landed in Azure Blob Storage) into clean, typed models ready for Snowflake loading. The current scope is Facebook posts and comments only. Where transformation runs — before loading, after loading, or split across both — depends on the complexity of the model.

> **Future platforms:** A `platform` column is included on all models so YouTube, Twitter/X, etc. can be added to the same tables later without schema changes.

---

## Transformation Strategy — Decided by Model Complexity

Rather than committing to a single pattern upfront, each model is evaluated individually:

| Complexity level | Where to transform | Rationale |
|---|---|---|
| **Simple** — direct field mapping, type casting, nullability | After loading in Snowflake (ELT) | SQL handles this well; no Python overhead |
| **Medium** — conditional logic, string normalization, fallback fields | Light Python transform before load, finish in Snowflake | Python handles edge cases; Snowflake handles aggregation |
| **Complex** — multi-source joins, business rules, ML scoring | Before load in Python (ETL) | Easier to test, version, and debug in Python |

### Applied to this project

| Model | Expected complexity | Strategy |
|---|---|---|
| `FacebookPage` | Simple | ELT — load raw, flatten in Snowflake via dbt |
| `Post` | Medium — reaction counts, attachment parsing, message/story fallback | Light Python transform (parse + normalize) → Snowflake finishes joins/aggregations |
| `Comment` | Medium — reaction counts, reply threading, author mapping | Light Python transform → Snowflake |
| `SentimentResult` | Complex — ML model invocation, language detection, enrichment | Full ETL — Python scores and writes structured rows |

### "Light transform before load" pattern

In the hybrid (medium) case, Python does the minimum needed to make the data safe to land:
- Parse and validate types (timestamps, nulls, integers)
- Extract nested reaction counts into flat fields
- Normalize text fields (strip whitespace, handle encoding)
- Attach metadata (`raw_blob_path`, `ingested_at`)

Heavy business logic, joins, and aggregations are deferred to Snowflake SQL.

This decision is **revisited per model** as complexity becomes clearer during implementation.

---

## Data Models

### 1. `FacebookPage`

Represents a monitored Facebook public page (e.g. a fire service or emergency department page). This is a metadata/reference table — one row per page being monitored.

| Column | Type | Description |
|---|---|---|
| `source_id` | VARCHAR | Surrogate key: `facebook_{page_id}` (PK) |
| `platform` | VARCHAR | Always `facebook` for now; reserved for future platforms |
| `page_id` | VARCHAR | Facebook's native page ID |
| `page_name` | VARCHAR | Display name of the Facebook page |
| `handle` | VARCHAR | Facebook page username / vanity URL (e.g. `@NSWRuralFire`) |
| `category` | VARCHAR | Facebook page category (e.g. `Fire Station`, `Government`) |
| `country` | VARCHAR | Country code (e.g. `AU`, `US`) |
| `language` | VARCHAR | Primary language of the page (e.g. `en`) |
| `fan_count` | INT | Number of page followers/likes at time of ingestion |
| `is_active` | BOOLEAN | Whether this page is currently being monitored by the pipeline |
| `first_ingested_at` | TIMESTAMP_TZ | When this page was first added to the pipeline |
| `last_ingested_at` | TIMESTAMP_TZ | When this page was last successfully ingested |
| `_updated_at` | TIMESTAMP_TZ | Last time this row was updated |

---

### 2. `Post`

One record per Facebook post. Currently Facebook only; `platform` column included for future extensibility.

| Column | Type | Description |
|---|---|---|
| `post_id` | VARCHAR | Surrogate key: `facebook_{native_post_id}` (PK) |
| `platform` | VARCHAR | Always `facebook` currently |
| `native_post_id` | VARCHAR | Facebook's own post ID |
| `source_id` | VARCHAR | FK → FacebookPage (`facebook_{page_id}`) |
| `message` | TEXT | Post body text |
| `story` | TEXT | Auto-generated story text (fallback when no message) |
| `created_time` | TIMESTAMP_TZ | When the post was published |
| `share_count` | INT | Number of shares |
| `comment_count` | INT | Total comment count (from summary) |
| `reaction_total` | INT | Total reaction count across all types |
| `reaction_like` | INT | Count of LIKE reactions 👍 |
| `reaction_love` | INT | Count of LOVE reactions ❤️ |
| `reaction_haha` | INT | Count of HAHA reactions 😆 |
| `reaction_wow` | INT | Count of WOW reactions 😮 |
| `reaction_sad` | INT | Count of SAD reactions 😢 |
| `reaction_angry` | INT | Count of ANGRY reactions 😡 |
| `has_attachment` | BOOLEAN | Whether the post has media/link attachments |
| `attachment_type` | VARCHAR | Type of attachment (photo, video, link, etc.) |
| `has_text` | BOOLEAN | True if usable text exists after cleaning |
| `is_share` | BOOLEAN | True if the post is a reshare of another post |
| `text_cleaned` | TEXT | Cleaned text used for sentiment analysis |
| `quality_flag` | VARCHAR | Cleaning quality flags, NULL if clean |
| `cleaning_version` | VARCHAR | Version of cleaning rules applied |
| `raw_blob_path` | VARCHAR | Path to source file in Azure Blob Storage |
| `ingested_at` | TIMESTAMP_TZ | Pipeline ingestion timestamp |

> **API note (Facebook):** Request per-type reaction counts explicitly:
> `reactions.type(LIKE).limit(0).summary(true).as(reaction_like)` — repeat for each type.
> For platforms without typed reactions (e.g. YouTube), only `reaction_like` is populated; all others default to 0.

---

### 3. `Comment`

One record per Facebook comment or reply on a post. Reactions are captured at the comment level as well as the post level. Currently Facebook only; `platform` column included for future extensibility.

| Column | Type | Description |
|---|---|---|
| `comment_id` | VARCHAR | Surrogate key: `facebook_{native_comment_id}` (PK) |
| `platform` | VARCHAR | Always `facebook` currently |
| `native_comment_id` | VARCHAR | Facebook's own comment ID |
| `post_id` | VARCHAR | FK → Post |
| `parent_comment_id` | VARCHAR | FK → Comment (null if top-level comment; set if reply) |
| `source_id` | VARCHAR | FK → FacebookPage (denormalized for query convenience) |
| `message` | TEXT | Comment text — **primary sentiment input** |
| `created_time` | TIMESTAMP_TZ | When the comment was posted |
| `author_id` | VARCHAR | Platform user ID of the commenter |
| `author_name` | VARCHAR | Display name of the commenter |
| `reaction_total` | INT | Total reaction count across all types |
| `reaction_like` | INT | Count of LIKE reactions 👍 |
| `reaction_love` | INT | Count of LOVE reactions ❤️ |
| `reaction_haha` | INT | Count of HAHA reactions 😆 |
| `reaction_wow` | INT | Count of WOW reactions 😮 |
| `reaction_sad` | INT | Count of SAD reactions 😢 |
| `reaction_angry` | INT | Count of ANGRY reactions 😡 |
| `reply_count` | INT | Number of replies to this comment |
| `is_reply` | BOOLEAN | True if this is a reply to another comment |
| `has_text` | BOOLEAN | True if usable text exists after cleaning |
| `text_cleaned` | TEXT | Cleaned text used for sentiment analysis |
| `quality_flag` | VARCHAR | Cleaning quality flags, NULL if clean |
| `cleaning_version` | VARCHAR | Version of cleaning rules applied |
| `raw_blob_path` | VARCHAR | Path to source file in Azure Blob Storage |
| `ingested_at` | TIMESTAMP_TZ | Pipeline ingestion timestamp |

> **Note:** `like_count` from the raw API is the legacy field; use the typed `reaction_like` count instead. Both are captured but `reaction_like` is canonical.
> For platforms without typed reactions (e.g. YouTube), only `reaction_like` is populated; all others default to 0.

---

### 4. `Reaction` (optional granular table)

If per-user reaction detail is needed (requires additional API permissions). Platform-neutral — reaction types that don't exist on a platform are simply not present.

| Column | Type | Description |
|---|---|---|
| `reaction_id` | VARCHAR | Surrogate key |
| `platform` | VARCHAR | Platform identifier |
| `content_type` | VARCHAR | `post` or `comment` — reactions exist on both |
| `content_id` | VARCHAR | FK → `post_id` or `comment_id` |
| `user_id` | VARCHAR | Platform user ID of the reactor |
| `reaction_type` | VARCHAR | LIKE, LOVE, HAHA, WOW, SAD, ANGRY (platform-dependent) |
| `reacted_at` | TIMESTAMP_TZ | Reaction timestamp (if available) |
| `ingested_at` | TIMESTAMP_TZ | Pipeline ingestion timestamp |

> **Note:** Per-user reactions require elevated permissions (e.g. `pages_read_engagement` on Facebook) and may not be available. Aggregated counts on `Post` and `Comment` are the reliable baseline.

---

### 5. `SentimentResult`

Stores sentiment classification output — populated after the sentiment analysis step.

| Column | Type | Description |
|---|---|---|
| `sentiment_id` | VARCHAR | Surrogate key (PK) |
| `source_type` | VARCHAR | `post` or `comment` — what is being scored |
| `source_id` | VARCHAR | FK → `post_id` (if post) or `comment_id` (if comment) |
| `origin_source_id` | VARCHAR | FK → Source — the page/channel the content came from |
| `text_analyzed` | TEXT | The exact text that was scored |
| `sentiment_label` | VARCHAR | `positive`, `negative`, `neutral` |
| `confidence_score` | FLOAT | Model confidence (0.0 – 1.0) |
| `is_reaction_flagged` | BOOLEAN | True if reaction pattern signals negative sentiment |
| `detected_language` | VARCHAR | Detected language code (e.g. `en`, `es`) |
| `model_name` | VARCHAR | Name/version of the model used |
| `analyzed_at` | TIMESTAMP_TZ | When sentiment was scored |

---

## Transformation Rules

### Post
- Build `post_id` as `{platform}_{native_post_id}`
- Use `message` if present; fall back to `story` if `message` is null or empty
- Parse `created_time` from ISO 8601 string to `TIMESTAMP_TZ`
- Extract reaction counts from each typed reaction summary block; sum all types → `reaction_total`
- Set `has_attachment = TRUE` if `attachments.data` is non-empty
- Populate `CleaningMeta` from the cleaning step (see `data_cleaning.md`)

### Comment
- Build `comment_id` as `{platform}_{native_comment_id}`
- Set `is_reply = TRUE` if `parent_comment_id` is not null
- Map `from.id` → `author_id`, `from.name` → `author_name` (Facebook); platform connectors handle equivalent fields
- Extract per-type reaction counts from summary blocks (same pattern as Post)
- Sum all reaction types → `reaction_total`
- Populate `CleaningMeta` from the cleaning step

### General
- All timestamps normalized to UTC
- Null-safe: missing optional fields default to `NULL` (not empty string or 0)
- Deduplication key: `post_id` for posts, `comment_id` for comments
- `platform` field is always set — required for cross-platform queries in Snowflake

---

## Python Model Classes (Pydantic)

```python
from pydantic import BaseModel, computed_field
from datetime import datetime
from typing import Optional
from enum import Enum

class Platform(str, Enum):
    FACEBOOK = "facebook"
    YOUTUBE  = "youtube"
    TWITTER  = "twitter"
    # extend here when adding a new platform


class ReactionSummary(BaseModel):
    """
    Platform-neutral reaction counts.
    Platforms that don't support a reaction type leave it at 0.
    e.g. YouTube only has 'like'; LOVE/HAHA/WOW/SAD/ANGRY stay 0.
    """
    like: int = 0
    love: int = 0
    haha: int = 0
    wow: int = 0
    sad: int = 0
    angry: int = 0

    @computed_field
    @property
    def total(self) -> int:
        return self.like + self.love + self.haha + self.wow + self.sad + self.angry


class CleaningMeta(BaseModel):
    """Attached to every cleaned record."""
    has_text: bool
    text_cleaned: Optional[str] = None
    is_share: bool = False
    quality_flag: Optional[str] = None   # comma-separated flags or None
    cleaning_version: str


class PostModel(BaseModel):
    post_id: str                          # {platform}_{native_post_id}
    platform: Platform
    native_post_id: str
    source_id: str                        # FK → Source
    message: Optional[str] = None
    story: Optional[str] = None
    created_time: datetime
    share_count: int = 0
    comment_count: int = 0
    reactions: ReactionSummary = ReactionSummary()
    has_attachment: bool = False
    attachment_type: Optional[str] = None
    cleaning: CleaningMeta
    raw_blob_path: str
    ingested_at: datetime


class CommentModel(BaseModel):
    comment_id: str                       # {platform}_{native_comment_id}
    platform: Platform
    native_comment_id: str
    post_id: str
    parent_comment_id: Optional[str] = None
    source_id: str                        # FK → Source
    message: str
    created_time: datetime
    author_id: Optional[str] = None
    author_name: Optional[str] = None
    reactions: ReactionSummary = ReactionSummary()
    reply_count: int = 0
    is_reply: bool = False
    cleaning: CleaningMeta
    raw_blob_path: str
    ingested_at: datetime
```

---

## ELT — Snowflake SQL Flattening

If raw JSON is loaded first, flatten with SQL. Reaction counts apply to both posts and comments.

### Posts
```sql
SELECT
    r.platform || '_' || f.value:id::VARCHAR                    AS post_id,   -- {platform}_{native_id}
    r.platform                                                  AS platform,
    f.value:id::VARCHAR                                         AS native_post_id,
    r.source_id                                                 AS source_id,
    COALESCE(f.value:message::TEXT, f.value:story::TEXT)        AS message,
    f.value:story::TEXT                                         AS story,
    f.value:created_time::TIMESTAMP_TZ                          AS created_time,
    f.value:shares:count::INT                                   AS share_count,
    f.value:comments:summary:total_count::INT                   AS comment_count,
    f.value:reaction_like:summary:total_count::INT              AS reaction_like,
    f.value:reaction_love:summary:total_count::INT              AS reaction_love,
    f.value:reaction_haha:summary:total_count::INT              AS reaction_haha,
    f.value:reaction_wow:summary:total_count::INT               AS reaction_wow,
    f.value:reaction_sad:summary:total_count::INT               AS reaction_sad,
    f.value:reaction_angry:summary:total_count::INT             AS reaction_angry,
    (
        COALESCE(f.value:reaction_like:summary:total_count::INT, 0) +
        COALESCE(f.value:reaction_love:summary:total_count::INT, 0) +
        COALESCE(f.value:reaction_haha:summary:total_count::INT, 0) +
        COALESCE(f.value:reaction_wow:summary:total_count::INT, 0)  +
        COALESCE(f.value:reaction_sad:summary:total_count::INT, 0)  +
        COALESCE(f.value:reaction_angry:summary:total_count::INT, 0)
    )                                                           AS reaction_total
FROM {{ source('raw', 'raw_social') }} r,
LATERAL FLATTEN(input => r.data) f
WHERE r.source_type = 'posts';
```

### Comments
```sql
SELECT
    r.platform || '_' || f.value:id::VARCHAR                    AS comment_id,  -- {platform}_{native_id}
    r.platform                                                  AS platform,
    f.value:id::VARCHAR                                         AS native_comment_id,
    r.platform || '_' || f.value:post_id::VARCHAR               AS post_id,
    f.value:parent_id::VARCHAR                                  AS parent_comment_id,
    r.source_id                                                 AS source_id,
    f.value:message::TEXT                                       AS message,
    f.value:created_time::TIMESTAMP_TZ                          AS created_time,
    f.value:from:id::VARCHAR                                    AS author_id,
    f.value:from:name::VARCHAR                                  AS author_name,
    f.value:reaction_like:summary:total_count::INT              AS reaction_like,
    f.value:reaction_love:summary:total_count::INT              AS reaction_love,
    f.value:reaction_haha:summary:total_count::INT              AS reaction_haha,
    f.value:reaction_wow:summary:total_count::INT               AS reaction_wow,
    f.value:reaction_sad:summary:total_count::INT               AS reaction_sad,
    f.value:reaction_angry:summary:total_count::INT             AS reaction_angry,
    (
        COALESCE(f.value:reaction_like:summary:total_count::INT, 0) +
        COALESCE(f.value:reaction_love:summary:total_count::INT, 0) +
        COALESCE(f.value:reaction_haha:summary:total_count::INT, 0) +
        COALESCE(f.value:reaction_wow:summary:total_count::INT, 0)  +
        COALESCE(f.value:reaction_sad:summary:total_count::INT, 0)  +
        COALESCE(f.value:reaction_angry:summary:total_count::INT, 0)
    )                                                           AS reaction_total,
    f.value:comment_count::INT                                  AS reply_count,
    (f.value:parent_id IS NOT NULL)::BOOLEAN                    AS is_reply
FROM {{ source('raw', 'raw_social') }} r,
LATERAL FLATTEN(input => r.data) f
WHERE r.source_type = 'comments';
```
