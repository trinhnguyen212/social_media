# Snowflake Loading

## Overview

The Snowflake loading layer moves transformed data from Azure Blob Storage (or directly from Python models) into Snowflake tables. It supports both ETL (Python writes structured rows) and ELT (raw JSON loaded first, transformed via SQL).

---

## Database Structure

```
DATABASE: SOCIAL_MEDIA
│
├── SCHEMA: RAW              -- Raw JSON from blob (ELT path)
│   ├── RAW_SOCIAL           -- Platform-neutral raw table (all platforms)
│   └── REJECTED_RECORDS     -- Dead-letter table for failed records
│
├── SCHEMA: STAGING          -- Cleaned, typed models
│   ├── STG_FACEBOOK_PAGE    -- Monitored Facebook pages (metadata/reference)
│   ├── STG_POST             -- Facebook posts
│   ├── STG_COMMENT          -- Facebook comments and replies
│   └── STG_REACTION         -- Optional: per-user reaction detail (requires elevated API permissions)
│
├── SCHEMA: ANALYTICS        -- Final analytical tables
│   ├── FACT_POST
│   ├── FACT_COMMENT
│   └── FACT_SENTIMENT_RESULT
│
└── SCHEMA: AUDIT            -- Pipeline run logs
    └── PIPELINE_RUN_LOG
```

---

## Table Definitions

### `RAW.RAW_SOCIAL` (ELT path only)

Platform-neutral raw table — all social media platforms land here.

```sql
CREATE TABLE RAW.RAW_SOCIAL (
    load_id         VARCHAR         NOT NULL DEFAULT UUID_STRING(),
    platform        VARCHAR         NOT NULL,   -- 'facebook', 'youtube', 'twitter', etc.
    source_type     VARCHAR         NOT NULL,   -- 'posts' or 'comments'
    source_id       VARCHAR         NOT NULL,   -- {platform}_{native_page_id}
    blob_path       VARCHAR         NOT NULL,
    meta            VARIANT,
    data            VARIANT,
    loaded_at       TIMESTAMP_TZ    NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
```

---

### `STAGING.STG_FACEBOOK_PAGE`

One record per monitored Facebook public page. This is a metadata/reference table — small row count, updated on each pipeline run.

```sql
CREATE TABLE STAGING.STG_FACEBOOK_PAGE (
    source_id           VARCHAR         NOT NULL,   -- 'facebook_{page_id}' (PK)
    platform            VARCHAR         NOT NULL DEFAULT 'facebook',
    page_id             VARCHAR         NOT NULL,   -- Facebook native page ID
    page_name           VARCHAR         NOT NULL,
    handle              VARCHAR,                    -- Facebook vanity username / @handle
    category            VARCHAR,                    -- e.g. 'Fire Station', 'Government'
    country             VARCHAR,                    -- ISO country code, e.g. 'AU', 'US'
    language            VARCHAR,                    -- primary language, e.g. 'en'
    fan_count           INT,                        -- page followers/likes at ingestion time
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    first_ingested_at   TIMESTAMP_TZ    NOT NULL,
    last_ingested_at    TIMESTAMP_TZ    NOT NULL,
    _updated_at         TIMESTAMP_TZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_stg_facebook_page PRIMARY KEY (source_id)
);
```

---

### `STAGING.STG_POST`
```sql
CREATE TABLE STAGING.STG_POST (
    post_id             VARCHAR         NOT NULL,   -- {platform}_{native_post_id}
    platform            VARCHAR         NOT NULL,   -- 'facebook', 'youtube', 'twitter', etc.
    native_post_id      VARCHAR         NOT NULL,
    source_id           VARCHAR         NOT NULL,   -- FK → STG_FACEBOOK_PAGE
    message             TEXT,
    story               TEXT,
    created_time        TIMESTAMP_TZ    NOT NULL,
    share_count         INT             DEFAULT 0,
    comment_count       INT             DEFAULT 0,
    reaction_total      INT             DEFAULT 0,
    reaction_like       INT             DEFAULT 0,
    reaction_love       INT             DEFAULT 0,
    reaction_haha       INT             DEFAULT 0,
    reaction_wow        INT             DEFAULT 0,
    reaction_sad        INT             DEFAULT 0,
    reaction_angry      INT             DEFAULT 0,
    has_attachment      BOOLEAN         DEFAULT FALSE,
    attachment_type     VARCHAR,
    raw_blob_path       VARCHAR,
    ingested_at         TIMESTAMP_TZ    NOT NULL,
    _updated_at         TIMESTAMP_TZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_stg_post PRIMARY KEY (post_id)
);
```

---

### `STAGING.STG_COMMENT`
```sql
CREATE TABLE STAGING.STG_COMMENT (
    comment_id          VARCHAR         NOT NULL,   -- {platform}_{native_comment_id}
    platform            VARCHAR         NOT NULL,   -- 'facebook', 'youtube', 'twitter', etc.
    native_comment_id   VARCHAR         NOT NULL,
    post_id             VARCHAR         NOT NULL,
    parent_comment_id   VARCHAR,
    source_id           VARCHAR         NOT NULL,   -- FK → STG_FACEBOOK_PAGE
    message             TEXT            NOT NULL,
    created_time        TIMESTAMP_TZ    NOT NULL,
    author_id           VARCHAR,
    author_name         VARCHAR,
    reaction_total      INT             DEFAULT 0,
    reaction_like       INT             DEFAULT 0,   -- LIKE reactions on the comment 👍
    reaction_love       INT             DEFAULT 0,   -- LOVE reactions on the comment ❤️
    reaction_haha       INT             DEFAULT 0,   -- HAHA reactions on the comment 😆
    reaction_wow        INT             DEFAULT 0,   -- WOW reactions on the comment 😮
    reaction_sad        INT             DEFAULT 0,   -- SAD reactions on the comment 😢
    reaction_angry      INT             DEFAULT 0,   -- ANGRY reactions on the comment 😡
    reply_count         INT             DEFAULT 0,
    is_reply            BOOLEAN         DEFAULT FALSE,
    raw_blob_path       VARCHAR,
    ingested_at         TIMESTAMP_TZ    NOT NULL,
    _updated_at         TIMESTAMP_TZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_stg_comment PRIMARY KEY (comment_id)
);
```

---

### `ANALYTICS.FACT_SENTIMENT_RESULT`
```sql
CREATE TABLE ANALYTICS.FACT_SENTIMENT_RESULT (
    sentiment_id            VARCHAR         NOT NULL DEFAULT UUID_STRING(),
    source_type             VARCHAR         NOT NULL,   -- 'post' or 'comment'
    source_id               VARCHAR         NOT NULL,   -- FK → post_id or comment_id
    origin_source_id        VARCHAR         NOT NULL,   -- FK → STG_FACEBOOK_PAGE
    text_analyzed           TEXT            NOT NULL,
    sentiment_label         VARCHAR         NOT NULL,   -- 'positive', 'negative', 'neutral'
    confidence_score        FLOAT           NOT NULL,
    is_reaction_flagged     BOOLEAN         DEFAULT FALSE,
    detected_language       VARCHAR,
    model_name              VARCHAR         NOT NULL,
    analyzed_at             TIMESTAMP_TZ    NOT NULL,
    CONSTRAINT pk_sentiment PRIMARY KEY (sentiment_id)
);
```

---

## Loading Tool — Snowpipe vs COPY INTO vs Python Connector

Three tools are available. The right choice depends on how frequently data arrives and whether near-real-time loading is needed.

### Option Comparison

| Tool | How it works | Best for | Latency | Cost model |
|---|---|---|---|---|
| **Snowpipe** | Event-driven; auto-triggers on new blob files via Azure Event Grid notification | Continuous / high-frequency ingestion, near-real-time | Seconds to low minutes | Per-credit compute (serverless) + file notification cost |
| **COPY INTO** | Batch command run on a schedule; loads all new files in a stage path | Scheduled batch loads (hourly / daily) | Matches schedule interval | Standard warehouse compute only when running |
| **Python Connector (`write_pandas` / `executemany`)** | Python writes rows directly to Snowflake over the connector | ETL path where Python already holds transformed rows | Immediate on pipeline run | Standard warehouse compute |

---

### Recommendation for This Pipeline

```
Facebook API run (scheduled: e.g. every hour)
        │
        ▼
Files land in Azure Blob Storage
        │
        ▼
  ┌─────────────────────────────────────────────────┐
  │  COPY INTO  (recommended — batch, scheduled)    │
  │                                                 │
  │  • Triggered by the same pipeline job that      │
  │    ingested from Facebook                       │
  │  • Loads all new files since last run           │
  │  • Simple, auditable, no extra infra needed     │
  └─────────────────────────────────────────────────┘
        │
        ▼
  RAW schema (variant JSON)
        │
        ▼
  SQL transformation → STAGING / ANALYTICS
```

**COPY INTO is the recommended starting point** because:
- The Facebook pipeline already runs on a schedule (not a real-time stream)
- No additional infrastructure required (no Event Grid, no Snowpipe pipe objects)
- `COPY INTO` tracks which files have already been loaded — re-running the same path is safe and idempotent
- Full control over load timing within the pipeline run
- Easy to monitor via `COPY_HISTORY` and `LOAD_HISTORY` views

---

### When to Switch to Snowpipe

Upgrade to Snowpipe if:
- Ingestion frequency increases to near-continuous (multiple times per minute)
- Near-real-time sentiment dashboards are required (latency < 5 minutes)
- Azure Event Grid is already in use in the infrastructure

Snowpipe setup with Azure Event Grid:
```sql
-- Create pipe (one-time setup)
CREATE OR REPLACE PIPE RAW.SOCIAL_PIPE
  AUTO_INGEST = TRUE
  AS
  COPY INTO RAW.RAW_SOCIAL (platform, source_type, source_id, blob_path, meta, data)
  FROM (
      SELECT $1:meta:platform, $1:meta:source_type, $1:meta:source_id, METADATA$FILENAME, $1:meta, $1:data
      FROM @RAW.AZURE_SOCIAL_STAGE
  )
  FILE_FORMAT = (TYPE = 'JSON');
```
Then configure an Azure Event Grid subscription on the blob container to send `BlobCreated` notifications to the Snowpipe SQS/webhook endpoint.

---

### ETL Path — Python writes directly to staging tables

Use `snowflake-connector-python` or `snowflake-sqlalchemy`.

**Upsert pattern (MERGE):**
```sql
MERGE INTO STAGING.STG_POST AS target
USING (SELECT %s AS post_id, %s AS platform, %s AS source_id, ...) AS source
ON target.post_id = source.post_id
WHEN MATCHED THEN UPDATE SET
    message         = source.message,
    reaction_like   = source.reaction_like,
    reaction_love   = source.reaction_love,
    reaction_haha   = source.reaction_haha,
    reaction_wow    = source.reaction_wow,
    reaction_sad    = source.reaction_sad,
    reaction_angry  = source.reaction_angry,
    reaction_total  = source.reaction_total,
    comment_count   = source.comment_count,
    share_count     = source.share_count,
    _updated_at     = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (...) VALUES (...);
```

- Always MERGE, never plain INSERT — reaction counts and comment counts can change after the post is published
- Batch inserts: use `executemany` or write to a temp table and MERGE from it for large loads

---

### ELT Path — Stage raw JSON then transform with SQL

**Step 1: Load blob files into `RAW.RAW_SOCIAL`**

Use a single Snowflake external stage pointing to the root of Azure Blob Storage with SAS token:

```sql
-- Create external stage (one-time setup) — covers all platforms under raw/
CREATE OR REPLACE STAGE RAW.AZURE_SOCIAL_STAGE
  URL = 'azure://{storage_account}.blob.core.windows.net/{container}/raw/'
  CREDENTIALS = (AZURE_SAS_TOKEN = '{sas_token}')
  FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE);
```

```sql
-- Copy raw files into RAW_SOCIAL — path scoped per platform/source/date
COPY INTO RAW.RAW_SOCIAL (platform, source_type, source_id, blob_path, meta, data)
FROM (
    SELECT
        $1:meta:platform::VARCHAR,
        $1:meta:source_type::VARCHAR,
        $1:meta:source_id::VARCHAR,
        METADATA$FILENAME,
        $1:meta,
        $1:data
    FROM @RAW.AZURE_SOCIAL_STAGE/facebook/112233445566/posts/2026/04/03/
)
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';
```

**Step 2: Transform raw → staging using dbt**

dbt models read from `RAW.RAW_SOCIAL` and write to `STAGING.*`. The `platform` column drives any platform-specific logic. See `doc/orchestration.md` for the dbt model DAG.

Example dbt staging model (`models/staging/stg_post.sql`):
```sql
SELECT
    r.platform || '_' || f.value:id::VARCHAR                AS post_id,   -- {platform}_{native_id}
    r.platform                                              AS platform,
    f.value:id::VARCHAR                                     AS native_post_id,
    r.source_id                                             AS source_id,
    COALESCE(f.value:message::TEXT, f.value:story::TEXT)    AS message,
    f.value:story::TEXT                                     AS story,
    f.value:created_time::TIMESTAMP_TZ                      AS created_time,
    f.value:shares:count::INT                               AS share_count,
    f.value:comments:summary:total_count::INT               AS comment_count,
    f.value:reaction_like:summary:total_count::INT          AS reaction_like,
    f.value:reaction_love:summary:total_count::INT          AS reaction_love,
    f.value:reaction_haha:summary:total_count::INT          AS reaction_haha,
    f.value:reaction_wow:summary:total_count::INT           AS reaction_wow,
    f.value:reaction_sad:summary:total_count::INT           AS reaction_sad,
    f.value:reaction_angry:summary:total_count::INT         AS reaction_angry,
    (
        COALESCE(f.value:reaction_like:summary:total_count::INT, 0) +
        COALESCE(f.value:reaction_love:summary:total_count::INT, 0) +
        COALESCE(f.value:reaction_haha:summary:total_count::INT, 0) +
        COALESCE(f.value:reaction_wow:summary:total_count::INT, 0)  +
        COALESCE(f.value:reaction_sad:summary:total_count::INT, 0)  +
        COALESCE(f.value:reaction_angry:summary:total_count::INT, 0)
    )                                                       AS reaction_total,
    (ARRAY_SIZE(f.value:attachments:data) > 0)::BOOLEAN     AS has_attachment,
    f.value:attachments:data[0]:type::VARCHAR               AS attachment_type,
    r.blob_path                                             AS raw_blob_path,
    r.loaded_at                                             AS ingested_at
FROM {{ source('raw', 'raw_social') }} r,
LATERAL FLATTEN(input => r.data) f
WHERE r.source_type = 'posts'
```

> **Note:** The `{platform}_` prefix is applied here in the dbt staging model — this is where `native_post_id` becomes the canonical `post_id`.

---

## Snowflake External Stage (Azure Blob + SAS)

```sql
-- Rotate SAS token (run when token is renewed)
ALTER STAGE RAW.AZURE_SOCIAL_STAGE
  SET CREDENTIALS = (AZURE_SAS_TOKEN = '{new_sas_token}');
```

- Never embed the SAS token in version-controlled SQL files — pass via env var or Snowflake secrets
- Grant `USAGE` on the stage only to the pipeline service role

---

## Snowflake Connection — Python

```python
import snowflake.connector
import os

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],   # or use key-pair auth
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database="SOCIAL_MEDIA",
    schema="STAGING",
    role=os.environ["SNOWFLAKE_ROLE"]
)
```

---

## Environment Variables

| Variable | Description |
|---|---|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier (e.g. `xy12345.us-east-1`) |
| `SNOWFLAKE_USER` | Service account username |
| `SNOWFLAKE_PASSWORD` | Password (or use `SNOWFLAKE_PRIVATE_KEY_PATH` for key-pair) |
| `SNOWFLAKE_WAREHOUSE` | Virtual warehouse name |
| `SNOWFLAKE_ROLE` | Role granted access to pipeline schemas |
| `SNOWFLAKE_DATABASE` | `SOCIAL_MEDIA` |

---

## Idempotency & Data Quality

- All loads are **idempotent**: re-running the same blob file produces the same result (MERGE deduplicates by PK)
- `COPY INTO` with `ON_ERROR = CONTINUE` skips malformed records and logs them — review `COPY_HISTORY` after each load
- Post-load row count check: compare `record_count` in blob metadata envelope against rows inserted
- Run a daily reconciliation query comparing blob file counts vs Snowflake row counts per `page_id` and date partition
