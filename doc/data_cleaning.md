# Data Cleaning

## Overview

Data cleaning runs after ingestion (raw JSON is in Azure Blob Storage) and before or during transformation. Its job is to produce reliable, consistent records regardless of the source platform. Because the pipeline is designed to expand beyond Facebook (e.g. YouTube, Twitter/X, Instagram), cleaning rules are split into **platform-agnostic** rules that apply to all sources and **platform-specific** rules that handle quirks of each platform's API.

---

## Where Cleaning Fits in the Pipeline

Three options are available for where cleaning runs. A **single consistent approach will be chosen for the whole pipeline** — not decided model by model. The decision is deferred until the full scope of cleaning rules is better understood.

```
Raw JSON (Azure Blob)
        │
        ▼
────────────────────────────────────────────────────────────
  OPTION A              OPTION B              OPTION C
  Python pre-load       Snowflake post-load   Split (both)
────────────────────────────────────────────────────────────
        │                     │                     │
  [Python Cleaning]      Snowflake Load        [Python Cleaning]
        │               (raw JSON → variant)    (blocking rules only)
        │                     │                     │
  Snowflake Load        [Snowflake SQL               │
  (clean rows)           Cleaning]             Snowflake Load
        │                     │               (partially cleaned)
        │                     │                     │
        │                     │               [Snowflake SQL
        │                     │                Cleaning]
        │                     │                     │
────────────────────────────────────────────────────────────
        │                     │                     │
        ▼                     ▼                     ▼
                   Transformation / Modelling
                              │
                              ▼
                         Snowflake
                     (Analytics Layer)
```

| Option | When to use | Examples |
|---|---|---|
| **A — Python pre-load** | Complex logic, ML-based rules, must block bad records from landing | Spam detection, bot identification, language detection |
| **B — Snowflake post-load** | Simple rules, safe to defer, raw data still useful as-is | Whitespace trimming, null coalescing, type casting, HTML entity decoding |
| **C — Split** | Mix of blocking and deferrable rules in the same model | Reject null PKs in Python; normalize text and flag quality issues in Snowflake |

> **Decision deferred:** A consistent option (A, B, or C) will be chosen for the **entire pipeline** — not decided model by model. The choice will be made once the full scope of cleaning rules across all models is understood. The summary in the [Before vs After Loading](#before-vs-after-loading--summary) section documents what each option would mean in practice to inform that decision.

---

## Before vs After Loading — Summary

### Do Before Loading (Python)

These must run in Python before data reaches Snowflake because they are either **blocking** (bad data must not land), **stateful** (require logic across multiple records), or **require external libraries**.

| Category | Rule | Reason |
|---|---|---|
| **Validation** | Reject null/empty PKs (`post_id`, `comment_id`) | Would silently corrupt Snowflake PKs or cause MERGE failures |
| **Validation** | Reject completely unparseable timestamps | Snowflake CAST on a malformed string silently returns NULL |
| **Validation** | Reject records where `message` is required but missing | Nothing to analyze — no point loading |
| **Encoding** | Fix binary encoding issues, mojibake, malformed UTF-8 | Must be fixed at byte level before writing to storage |
| **Structure** | Extract nested reaction counts from raw JSON into flat fields | Complex nested traversal; safer and clearer in Python |
| **Structure** | Parse `from.id` / `from.name` into flat author fields | Avoids complex JSON path handling repeated in SQL |
| **Routing** | Write rejected records to dead-letter table | Needs Python logic to separate good vs bad records before load |
| **ML / NLP** | Language detection | Requires external library (`langdetect`, Azure AI); not feasible in pure SQL |
| **ML / NLP** | Spam / bot detection | Stateful — requires checking count of comments per author across records |

---

### Do After Loading (Snowflake SQL)

These are safe to defer because the raw data lands cleanly and the rules are simple value transformations expressible in SQL.

| Category | Rule | Reason |
|---|---|---|
| **Text** | Strip leading/trailing whitespace | Trivial in SQL: `TRIM(message)` |
| **Text** | Collapse internal whitespace | SQL regex: `REGEXP_REPLACE(message, '\\s+', ' ')` |
| **Text** | Null coalescing — empty string → NULL | SQL: `NULLIF(message, '')` |
| **Text** | Decode HTML entities | Snowflake JavaScript UDF handles this cleanly post-load |
| **Text** | Truncate to max column length | SQL: `LEFT(message, 10000)` |
| **Timestamps** | Normalize to UTC | SQL: `CONVERT_TIMEZONE('UTC', created_time)` |
| **Timestamps** | Validate range (reject pre-2004 or future) | SQL: `WHERE created_time BETWEEN '2004-01-01' AND CURRENT_TIMESTAMP()` |
| **Numeric** | Default missing counts to 0 | SQL: `COALESCE(reaction_like, 0)` |
| **Numeric** | Compute `reaction_total` from individual counts | SQL: sum of all reaction columns |
| **Derived fields** | Set `is_reply = TRUE` if `parent_comment_id` is not null | Simple SQL condition |
| **Derived fields** | Set `has_attachment` from attachment array size | SQL: `ARRAY_SIZE > 0` |
| **Derived fields** | Set `has_text` from cleaned message | SQL: `message IS NOT NULL AND LENGTH(TRIM(message)) > 0` |
| **Quality flags** | Flag `too_short` — fewer than 3 words | SQL: `ARRAY_SIZE(SPLIT(message, ' ')) < 3` |
| **Quality flags** | Flag `url_only` — message contains only a URL | SQL regex match |
| **Quality flags** | Flag `high_emoji_ratio` | Snowflake JS UDF for Unicode emoji detection |
| **Deduplication** | Deduplicate via MERGE on PK across runs | Native Snowflake MERGE pattern |

---

### Summary Decision Matrix

```
                        Do in Python (pre-load)
                        ┌─────────────────────────────────────────┐
                        │  • Null / invalid PK rejection          │
                        │  • Malformed timestamp rejection        │
                        │  • Required field missing → dead-letter │
                        │  • Binary encoding / UTF-8 repair       │
                        │  • Nested JSON extraction (reactions,   │
                        │    author fields)                       │
                        │  • Language detection (ML)              │
                        │  • Spam / bot detection (stateful)      │
                        └─────────────────────────────────────────┘
                                        │
                               Load to Snowflake
                                        │
                        Do in Snowflake SQL (post-load)
                        ┌─────────────────────────────────────────┐
                        │  • Text trimming & whitespace collapse  │
                        │  • Empty string → NULL                  │
                        │  • HTML entity decoding (JS UDF)        │
                        │  • Timestamp UTC normalization          │
                        │  • Reaction / count defaulting to 0     │
                        │  • reaction_total computation           │
                        │  • Derived booleans (is_reply,          │
                        │    has_attachment, has_text)            │
                        │  • Simple quality flags (too_short,     │
                        │    url_only, high_emoji_ratio)          │
                        │  • Deduplication via MERGE              │
                        └─────────────────────────────────────────┘
```

---

## Platform-Agnostic Cleaning Rules

These rules apply to every platform connector.

### Text Fields

| Rule | Detail |
|---|---|
| Strip whitespace | Remove leading/trailing spaces and newlines from all text fields |
| Normalize Unicode | Normalize to NFC form; remove zero-width and non-printable characters |
| Collapse internal whitespace | Replace multiple consecutive spaces/newlines with a single space |
| Null coalescing | Treat empty string `""` as `NULL` — do not store empty strings |
| Encoding issues | Decode HTML entities (`&amp;` → `&`, `&lt;` → `<`, etc.) |
| Emoji handling | Preserve emojis — they carry sentiment signal; do not strip |
| Max length guard | Truncate text fields that exceed column size limits; log a warning |

### Timestamps

| Rule | Detail |
|---|---|
| Normalize to UTC | Convert all timestamps to UTC `TIMESTAMP_TZ` |
| Validate range | Reject timestamps before 2004-01-01 (pre-social-media era) or in the future |
| ISO 8601 parsing | Handle both `Z` suffix and `+00:00` offset formats |

### Numeric Fields

| Rule | Detail |
|---|---|
| Non-negative counts | Reaction counts, share counts, reply counts must be ≥ 0; set negatives to NULL |
| Integer overflow guard | Cap at `INT` max if source API returns unexpectedly large values; log warning |
| Missing counts | If a count field is absent from the API response, default to `0`, not NULL |

### IDs

| Rule | Detail |
|---|---|
| Non-empty check | `source_id`, `post_id`, `comment_id` must not be null or empty — reject the record |
| Type coercion | Cast all IDs to VARCHAR; never rely on numeric ID arithmetic |
| Composite IDs | For platforms that use composite IDs (e.g. Facebook `{page_id}_{post_id}`), store both the composite and the component parts |

### Deduplication

| Rule | Detail |
|---|---|
| Within a batch | Deduplicate by PK within a single file before loading |
| Across runs | Use MERGE (not INSERT) in Snowflake to handle re-ingested records |
| Late updates | For records that can be updated after creation (e.g. reaction counts), always MERGE and update mutable fields |

---

## Platform-Specific Cleaning Rules

### Facebook

| Field | Rule |
|---|---|
| `message` vs `story` | If `message` is null/empty, fall back to `story`; if both null, mark record as `has_text = FALSE` |
| Reaction counts | Legacy `like_count` on comments may differ from `reaction_like` — use `reaction_like` as canonical; keep `like_count` for auditability |
| Author fields | `from` object may be absent on comments from pages or deleted accounts — null-safe map to `author_id`, `author_name` |
| Post ID format | Strip page ID prefix from composite post IDs where needed (`{page_id}_{post_id}` → store both) |
| Deleted content | API returns `{"error": {"code": 100}}` for deleted posts — log and skip, do not fail the batch |
| Shared posts | Posts that are shares may have no `message` and a `story` with the original post's text — flag with `is_share = TRUE` |

### YouTube *(future)*

| Field | Rule |
|---|---|
| Comment text | Strip YouTube auto-generated timestamps (e.g. `2:34`) if they appear standalone |
| HTML in descriptions | Video descriptions may contain HTML — strip tags before sentiment analysis |
| Like count | YouTube comments have `likeCount` but no per-type reactions — map to `reaction_like`; all other reaction fields default to 0 |
| Author | `authorDisplayName` and `authorChannelId` map to `author_name` and `author_id` |
| Reply threads | YouTube uses `parentId` on reply comments — map to `parent_comment_id` |
| Deleted/spam | Comments with `moderationStatus = rejected` or `heldForReview` — exclude from sentiment unless explicitly included |

### Twitter / X *(future)*

| Field | Rule |
|---|---|
| Tweet text | Strip `t.co` URLs unless URL content is relevant; keep hashtags and mentions as-is (sentiment signal) |
| Truncation | Handle legacy 140-char truncated tweets — prefer `full_text` over `text` |
| Retweet prefix | Strip `RT @handle:` prefix from retweet text before sentiment scoring |
| Reaction mapping | `favorite_count` → `reaction_like`; no other reaction types — remaining fields default to 0 |

---

## Spam & Low-Quality Content Filtering

These records are not deleted — they are **flagged** with a `quality_flag` column so analysts can choose to include or exclude them.

| Flag | Condition |
|---|---|
| `spam_suspected` | Message is a duplicate of another comment on the same post within 24 hours |
| `too_short` | Message is fewer than 3 words — insufficient for sentiment analysis |
| `url_only` | Message contains only a URL with no surrounding text |
| `high_emoji_ratio` | Message is >80% emojis — may still carry sentiment but flagged for review |
| `foreign_language` | Detected language does not match the page's primary language |
| `bot_suspected` | Author posts >50 comments on the same page within 1 hour |

Quality flags are stored in the `STG_COMMENT` and `STG_POST` tables as a `VARCHAR` array or separate boolean columns, and surfaced in `FACT_SENTIMENT_RESULT`.

---

## Cleaning Output Fields Added to Models

The following fields are added to `Post` and `Comment` by the cleaning step:

| Field | Type | Description |
|---|---|---|
| `has_text` | BOOLEAN | True if usable text exists after cleaning |
| `is_share` | BOOLEAN | True if the post is a reshare of another post (platform-specific) |
| `text_cleaned` | TEXT | Cleaned version of the text used for sentiment analysis |
| `quality_flag` | VARCHAR | Comma-separated quality flags, or NULL if clean |
| `cleaning_version` | VARCHAR | Version of the cleaning rules applied (for reprocessing) |

---

## Cleaning Versioning

- Cleaning rules change over time (new flags, new platform quirks)
- Store `cleaning_version` on every record so old records can be identified and reprocessed when rules change
- Increment the version string (e.g. `v1.0`, `v1.1`) when any rule changes
- A `reprocess` pipeline mode re-runs cleaning on all records with a previous version

---

## Cleaning Rules by Model

Tracks the cleaning option chosen for each model. Updated during implementation.

> **Note:** The option column below shows what would apply to each model under each pipeline approach — it is **not a per-model decision**. A single option will be applied consistently across all models once the approach is chosen.

| Model | If Option A (Python only) | If Option B (Snowflake only) | If Option C (Split) |
|---|---|---|---|
| `Source` | All rules in Python | All rules in Snowflake SQL | Reject null `native_id` in Python; rest in Snowflake |
| `Post` | Reject null PK, extract reactions/author, language detect, dead-letter | All rules in Snowflake SQL (JS UDFs for complex rules) | Reject null PK + extract nested fields in Python; text/flag rules in Snowflake |
| `Comment` | Reject null PK, extract reactions/author, language detect, spam/bot check, dead-letter | All rules in Snowflake SQL (JS UDFs for complex rules) | Reject null PK + extract nested fields in Python; text/flag rules in Snowflake |
| `SentimentResult` | All cleaning in Python alongside ML scoring | N/A — ML scoring cannot run in Snowflake SQL | Python handles ML; Snowflake handles any post-score normalization |

---

## Rejected Records

Records that fail hard validation (null PK, completely empty text on a required field, unparseable timestamp) are:
1. Written to a **dead-letter table** in Snowflake: `RAW.REJECTED_RECORDS`
2. Logged with the rejection reason and source blob path
3. Never silently dropped

```sql
CREATE TABLE RAW.REJECTED_RECORDS (
    rejection_id        VARCHAR         NOT NULL DEFAULT UUID_STRING(),
    platform            VARCHAR         NOT NULL,
    source_type         VARCHAR         NOT NULL,   -- 'post' or 'comment'
    raw_record          VARIANT,
    rejection_reason    VARCHAR         NOT NULL,
    blob_path           VARCHAR,
    rejected_at         TIMESTAMP_TZ    NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
```
