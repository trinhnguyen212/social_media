# Orchestration

## Overview

Orchestration coordinates every step of the pipeline in the correct order — from pulling data out of Facebook through to sentiment results being available in Snowflake analytics tables. It handles scheduling, step dependencies, retries, and alerting.

---

## Full Pipeline Flow

The position of the cleaning stage depends on the approach chosen (see `data_cleaning.md`). Both variants are shown below.

---

### Option A — Python Cleaning (before Snowflake load)

Cleaning runs in Python immediately after ingestion. Only clean data is loaded into Snowflake.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATOR (scheduled)                     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  STAGE 1 — INGESTION                                                 │
│                                                                      │
│  1a. Call platform API → paginate posts                              │
│  1b. For each post → paginate comments + replies                     │
│  1c. Write raw JSON batches → Azure Blob Storage                     │
│  1d. Update ingestion state (last_ingested_timestamp per source)     │
│                                                                      │
│  Output: raw JSON files in Azure Blob  raw/{platform}/...            │
└──────────────────────────────┬───────────────────────────────────────┘
                               │  on success
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  STAGE 2 — DATA CLEANING (Python)           ← between blob & load    │
│                                                                      │
│  2a. Read raw JSON from Azure Blob                                   │
│  2b. Apply platform-agnostic + platform-specific cleaning rules      │
│  2c. Flag low-quality records                                        │
│  2d. Route rejected records → RAW.REJECTED_RECORDS (dead-letter)     │
│                                                                      │
│  Output: cleaned, validated records ready to load                    │
└──────────────────────────────┬───────────────────────────────────────┘
                               │  on success
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  STAGE 3 — LOAD TO SNOWFLAKE                                         │
│                                                                      │
│  3a. Write cleaned rows directly to STAGING tables via connector     │
│  3b. Verify row count matches expected clean record count            │
│  3c. Log load summary to AUDIT.LOAD_LOG                              │
│                                                                      │
│  Output: clean rows in STAGING schema                                │
└──────────────────────────────┬───────────────────────────────────────┘
                               │  on success
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  STAGE 4 — TRANSFORMATION (Snowflake Tasks)                          │
│  ...                                                                 │
└──────────────────────────────┬───────────────────────────────────────┘
                               │  on success
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  STAGE 5 — SENTIMENT ANALYSIS                                        │
│  ...                                                                 │
└──────────────────────────────┬───────────────────────────────────────┘
                               │  on success
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  STAGE 6 — POST-LOAD VALIDATION                                      │
│  ...                                                                 │
└─────────────────────────────────────────────────────────────────────┘
```

---

### Option B — Snowflake SQL Cleaning (after raw load)

Raw JSON lands in Snowflake first. Cleaning runs as SQL inside Snowflake before transformation.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATOR (scheduled)                     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  STAGE 1 — INGESTION                                                 │
│                                                                      │
│  1a. Call platform API → paginate posts                              │
│  1b. For each post → paginate comments + replies                     │
│  1c. Write raw JSON batches → Azure Blob Storage                     │
│  1d. Update ingestion state (last_ingested_timestamp per source)     │
│                                                                      │
│  Output: raw JSON files in Azure Blob  raw/{platform}/...            │
└──────────────────────────────┬───────────────────────────────────────┘
                               │  on success
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  STAGE 2 — LOAD RAW TO SNOWFLAKE                                     │
│                                                                      │
│  2a. COPY INTO RAW.RAW_SOCIAL (all new blob files since last run)    │
│  2b. Verify row count matches blob metadata envelope                 │
│  2c. Log load summary to AUDIT.LOAD_LOG                              │
│                                                                      │
│  Output: raw variant rows in RAW schema                              │
└──────────────────────────────┬───────────────────────────────────────┘
                               │  on success
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  STAGE 3 — DATA CLEANING (Snowflake SQL)    ← inside Snowflake       │
│                                                                      │
│  3a. Apply cleaning rules via SQL / JS UDFs on RAW variant data      │
│  3b. Flag low-quality records                                        │
│  3c. Route rejected records → RAW.REJECTED_RECORDS                   │
│                                                                      │
│  Output: cleaned records in RAW or intermediate clean table          │
└──────────────────────────────┬───────────────────────────────────────┘
                               │  on success
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  STAGE 4 — TRANSFORMATION (Snowflake Tasks)                          │
│  ...                                                                 │
└──────────────────────────────┬───────────────────────────────────────┘
                               │  on success
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  STAGE 5 — SENTIMENT ANALYSIS                                        │
│  ...                                                                 │
└──────────────────────────────┬───────────────────────────────────────┘
                               │  on success
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  STAGE 6 — POST-LOAD VALIDATION                                      │
│  ...                                                                 │
└─────────────────────────────────────────────────────────────────────┘
```

---

### Stages 4–6 (common to both options)

```
┌──────────────────────────────────────────────────────────────────────┐
│  STAGE 4 — TRANSFORMATION (Snowflake Tasks)                          │
│                                                                      │
│  Run as Snowflake Task DAG in dependency order:                      │
│                                                                      │
│  4a. MERGE into STAGING.STG_FACEBOOK_PAGE                                   │
│  4b. MERGE into STAGING.STG_POST       (depends on 4a)               │
│  4c. MERGE into STAGING.STG_COMMENT    (depends on 4b)               │
│  4d. MERGE into STAGING.STG_REACTION   (depends on 4b, 4c) [optional]│
│  4e. MERGE into ANALYTICS.FACT_POST    (depends on 4b)               │
│  4f. MERGE into ANALYTICS.FACT_COMMENT (depends on 4c)               │
│                                                                      │
│  Output: clean, typed rows in STAGING + ANALYTICS schemas            │
└──────────────────────────────┬───────────────────────────────────────┘
                               │  on success
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  STAGE 5 — SENTIMENT ANALYSIS                                        │
│                                                                      │
│  5a. Query STAGING for all posts/comments without a sentiment result │
│  5b. Call sentiment model (Claude API / Azure AI / Hugging Face)     │
│  5c. Apply reaction enrichment flagging                              │
│  5d. MERGE results → ANALYTICS.FACT_SENTIMENT_RESULT                 │
│                                                                      │
│  Output: sentiment labels + confidence scores in analytics table     │
└──────────────────────────────┬───────────────────────────────────────┘
                               │  on success
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  STAGE 6 — POST-LOAD VALIDATION                                      │
│                                                                      │
│  6a. Row count reconciliation (blob vs RAW vs STAGING vs ANALYTICS)  │
│  6b. Check % negative sentiment — alert if above threshold           │
│  6c. Check for stale data (no new posts in expected window)          │
│  6d. Write run summary → AUDIT.PIPELINE_RUN_LOG                      │
│                                                                      │
│  Output: audit log entry; alerts fired if thresholds breached        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Stage Dependencies

### Option A — Python Cleaning
```
STAGE 1 (Ingestion → Blob)
    │
    └──► STAGE 2 (Python Cleaning)      ← between blob and Snowflake
              │
              └──► STAGE 3 (Load clean rows → Snowflake STAGING)
                        │
                        └──► STAGE 4 (Transformation — dbt)
                                  │
                   ┌──────────────┴──────────────┐
                   ▼                             ▼
            STG_POST / STG_COMMENT        (parallel branches)
                   │
                   └──► STAGE 5 (Sentiment Analysis)
                                  │
                                  └──► STAGE 6 (Validation)
```

### Option B — Snowflake SQL Cleaning
```
STAGE 1 (Ingestion → Blob)
    │
    └──► STAGE 2 (Load raw JSON → Snowflake RAW)
              │
              └──► STAGE 3 (Snowflake SQL Cleaning)  ← inside Snowflake
                        │
                        └──► STAGE 4 (Transformation — dbt)
                                  │
                   ┌──────────────┴──────────────┐
                   ▼                             ▼
            STG_POST / STG_COMMENT        (parallel branches)
                   │
                   └──► STAGE 5 (Sentiment Analysis)
                                  │
                                  └──► STAGE 6 (Validation)
```

- Each stage only starts when the previous succeeds
- Within Stage 4, tasks run in parallel where dependencies allow (e.g. `STG_POST` and `STG_COMMENT` concurrently once `STG_FACEBOOK_PAGE` is done)
- Stage 5 runs after staging tables are ready — not blocked by analytics fact table build
- Stage 6 always runs, even if Stage 5 partially fails, to capture audit state

---

## Inside Snowflake — dbt (recommended)

**dbt (data build tool)** is the recommended tool for all SQL work inside Snowflake — covering Stage 3 (cleaning, if Snowflake path chosen) and Stage 4 (transformation). It replaces raw SQL scripts and Snowflake native Tasks with a version-controlled, testable model DAG.

### Why dbt over raw Snowflake Tasks

| Concern | Snowflake Tasks (raw SQL) | dbt |
|---|---|---|
| Dependency management | Manual `AFTER` chaining in DDL | Automatic — inferred from `ref()` calls between models |
| Testing | Manual SQL assertions | Built-in `dbt test` — not-null, unique, accepted values, relationships |
| Documentation | None by default | Auto-generated lineage graph + column descriptions |
| Version control | SQL files only, no DAG awareness | Full project structure, models, tests, seeds in git |
| Incremental loads | Manual `MERGE` in each script | `incremental` materialization strategy built in |
| Re-running | Re-run whole script | `dbt run --select model_name` or `dbt run --select +downstream` |
| Orchestrator integration | Call `EXECUTE TASK` | Call `dbt run` via CLI or dbt Cloud API |

---

### dbt Project Structure

```
dbt/
├── dbt_project.yml
├── profiles.yml              # Snowflake connection (credentials via env vars)
├── models/
│   ├── raw/                  # Sources — point at RAW schema tables
│   │   └── sources.yml
│   ├── cleaning/             # Stage 3 (if Snowflake cleaning path chosen)
│   │   ├── clean_post.sql
│   │   └── clean_comment.sql
│   ├── staging/              # Stage 4a–4d — STG_ models
│   │   ├── stg_facebook_page.sql
│   │   ├── stg_post.sql
│   │   ├── stg_comment.sql
│   │   └── stg_reaction.sql
│   └── analytics/            # Stage 4e–4f — FACT_ models
│       ├── fact_post.sql
│       ├── fact_comment.sql
│       └── fact_sentiment_result.sql
└── tests/
    ├── stg_post_pk_unique.sql
    └── stg_comment_message_not_null.sql
```

---

### dbt Model DAG (mirrors Stage 4 dependencies)

```
RAW.RAW_SOCIAL  (source)
      │
      ▼
cleaning/clean_post          cleaning/clean_comment
      │                              │
      ▼                              ▼
staging/stg_facebook_page
      │
      ├──► staging/stg_post ──────────────────┐
      │          │                            │
      │          ▼                            ▼
      │    staging/stg_comment         analytics/fact_post
      │          │
      │          ├──► staging/stg_reaction
      │          │
      │          └──► analytics/fact_comment
      │
      └──► analytics/fact_sentiment_result  (after sentiment Python step writes scores)
```

dbt automatically resolves this DAG from `{{ ref('stg_post') }}` calls — no manual dependency wiring needed.

---

### Key dbt Model Patterns

**Incremental model (stg_post)** — only processes new rows each run:
```sql
-- models/staging/stg_post.sql
{{ config(
    materialized='incremental',
    unique_key='post_id',
    on_schema_change='append_new_columns'
) }}

SELECT
    ...
FROM {{ source('raw', 'raw_social') }}
{% if is_incremental() %}
WHERE loaded_at > (SELECT MAX(ingested_at) FROM {{ this }})
{% endif %}
```

**dbt tests (schema.yml)** — run after each model build:
```yaml
models:
  - name: stg_post
    columns:
      - name: post_id
        tests:
          - unique
          - not_null
      - name: platform
        tests:
          - not_null
          - accepted_values:
              values: ['facebook', 'youtube', 'twitter']
      - name: reaction_total
        tests:
          - not_null
  - name: stg_comment
    columns:
      - name: comment_id
        tests:
          - unique
          - not_null
      - name: message
        tests:
          - not_null
```

---

### How the Orchestrator Calls dbt

The external orchestrator (Airflow / Prefect) replaces `EXECUTE TASK` with a `dbt run` call:

```
Airflow DAG
  │
  ├── Stage 1: PythonOperator       → ingestion (Facebook API → Blob)
  ├── Stage 2: SnowflakeOperator    → COPY INTO RAW.RAW_SOCIAL
  ├── Stage 3: BashOperator         → dbt run --select cleaning.*        (if Snowflake cleaning)
  ├── Stage 4: BashOperator         → dbt run --select staging.* analytics.*
  │            BashOperator         → dbt test --select staging.* analytics.*
  ├── Stage 5: PythonOperator       → sentiment scoring → write to Snowflake
  │            BashOperator         → dbt run --select fact_sentiment_result
  └── Stage 6: BashOperator         → dbt test (validation) + audit log write
```

- `dbt run` handles the model DAG order internally — Airflow only calls it once per stage group
- `dbt test` runs after each `dbt run` to catch data quality issues before proceeding downstream
- On test failure, Airflow marks the task failed and halts the pipeline

---

## Orchestration Tool Options

| Tool | Approach | Best for |
|---|---|---|
| **Apache Airflow** | Python DAG defines all stages; calls Python scripts for ingestion/sentiment, runs SQL operators for Snowflake tasks | Full control, complex branching, already familiar with Airflow |
| **Azure Data Factory (ADF)** | Pipeline with activities chained in sequence; native Azure Blob and Snowflake connectors | All-Azure stack, low-code preference, native blob event triggers |
| **Prefect** | Python-native workflows; easy local testing, cloud execution available | Python-first teams, lightweight setup, good for iterative development |
| **Snowflake Tasks only** | Snowflake handles the transformation DAG internally; external scheduler only triggers Stage 1 | Minimal external tooling; transformation fully owned by Snowflake |

### Recommendation

Use **Airflow** (or Prefect if lighter weight is preferred) for Stages 1–3 and 5–6 (Python steps), and **dbt** for all SQL work inside Snowflake (cleaning if Snowflake path, and transformation). dbt manages its own model DAG — the orchestrator simply calls `dbt run` and `dbt test`.

```
Airflow DAG
  │
  ├── Stage 1: PythonOperator    → ingestion (platform API → Blob)
  ├── Stage 2: SnowflakeOperator → COPY INTO RAW.RAW_SOCIAL
  ├── Stage 3: BashOperator      → dbt run --select cleaning.*   (if Snowflake cleaning)
  │            PythonOperator    → Python cleaning                (if Python cleaning)
  ├── Stage 4: BashOperator      → dbt run --select staging.* analytics.*
  │            BashOperator      → dbt test --select staging.* analytics.*
  ├── Stage 5: PythonOperator    → sentiment scoring
  │            BashOperator      → dbt run --select fact_sentiment_result
  └── Stage 6: BashOperator      → dbt test (validation) + audit log write
```

---

## Incremental Run — End to End

An incremental run only processes **new data since the last successful run**. This keeps each run fast and cheap. Here is exactly how each stage knows what is "new".

---

```
STAGE 1 — INGESTION (Python)
┌─────────────────────────────────────────────────────────────────────┐
│  How it knows what's new:                                           │
│  • Read last_ingested_at from AUDIT.PIPELINE_RUN_LOG per page_id   │
│  • Pass since={last_ingested_at} to Facebook Graph API             │
│  • API returns only posts created after that timestamp              │
│  • For comments: re-fetch posts from last N days to catch           │
│    late-arriving comments (people comment days after a post)        │
│  • After successful blob write → update last_ingested_at            │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ only new JSON files written to blob
                           ▼
STAGE 2 — COPY INTO RAW.RAW_SOCIAL (Snowflake)
┌─────────────────────────────────────────────────────────────────────┐
│  How it knows what's new:                                           │
│  • COPY INTO natively tracks every file it has already loaded       │
│    (stored in Snowflake's load history)                             │
│  • Re-running COPY INTO on the same stage path is safe —           │
│    already-loaded files are automatically skipped                   │
│  • No manual tracking needed here                                   │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ only new rows in RAW_SOCIAL
                           ▼
STAGE 3 — CLEANING (Python or Snowflake SQL)
┌─────────────────────────────────────────────────────────────────────┐
│  How it knows what's new:                                           │
│  • Filter RAW_SOCIAL by loaded_at > last successful clean run       │
│  • OR: cleaning runs on the same batch just loaded in Stage 2       │
│    (passed as a parameter from the orchestrator)                    │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ only new cleaned records
                           ▼
STAGE 4 — dbt TRANSFORMATION (Snowflake)
┌─────────────────────────────────────────────────────────────────────┐
│  How it knows what's new:                                           │
│  • dbt incremental models use is_incremental() filter:             │
│                                                                     │
│    {% if is_incremental() %}                                        │
│    WHERE loaded_at > (SELECT MAX(ingested_at) FROM {{ this }})      │
│    {% endif %}                                                      │
│                                                                     │
│  • On first run: full load (no filter)                              │
│  • On subsequent runs: only rows newer than what's already in       │
│    the staging table                                                │
│  • unique_key = post_id / comment_id → MERGE handles updates        │
│    (e.g. reaction counts that changed since last run)               │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ only new/updated rows in STG_POST, STG_COMMENT
                           ▼
STAGE 5 — SENTIMENT SCORING (Python → Claude API)
┌─────────────────────────────────────────────────────────────────────┐
│  How it knows what's new:                                           │
│  • Query Snowflake for posts/comments with no sentiment result:     │
│                                                                     │
│    SELECT c.comment_id, c.text_cleaned, ...                         │
│    FROM STAGING.STG_COMMENT c                                       │
│    LEFT JOIN ANALYTICS.FACT_SENTIMENT_RESULT r                      │
│      ON r.source_id = c.comment_id                                  │
│      AND r.source_type = 'comment'                                  │
│    WHERE r.sentiment_id IS NULL   ← not yet scored                  │
│      AND c.has_text = TRUE                                          │
│      AND c.quality_flag IS NULL                                     │
│                                                                     │
│  • Only these records are sent to Claude API in batches             │
│  • Already-scored comments are never re-sent (saves API cost)       │
│  • Results written via MERGE → idempotent if run twice              │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ new rows in FACT_SENTIMENT_RESULT only
                           ▼
STAGE 6 — VALIDATION
┌─────────────────────────────────────────────────────────────────────┐
│  • Reconcile counts for this run only (not all-time)                │
│  • Log run summary to AUDIT.PIPELINE_RUN_LOG                        │
└─────────────────────────────────────────────────────────────────────┘
```

---

### State Tracking — Checkpoint File in Azure Blob

The ingestion checkpoint is stored as a JSON file in **Azure Blob Storage**, not in Snowflake. Stage 1 only touches Facebook and Azure Blob — no Snowflake connection needed during ingestion. Fewer dependencies, fewer breaking points.

```
checkpoints/facebook/state.json

{
  "last_loaded_at": "2026-04-03T10:00:00Z"
}
```

- Single file for the whole Facebook pipeline — not per page
- `last_loaded_at` is used as `since=` for all pages and all posts on each run
- Read at the **start** of Stage 1 → used as `since=` on all Facebook API calls
- Overwritten at the **end** of Stage 1 → only after all raw JSON files are successfully written to blob
- If Stage 1 fails, the checkpoint is not updated — next run retries the same window
- Uses the same SAS token already configured for the `raw/` container — no extra credentials

---

### What Each Stage Skips on an Incremental Run

| Stage | What is skipped |
|---|---|
| Stage 1 — Ingestion | All posts/comments older than checkpoint timestamp (read from Azure Blob `checkpoints/` file) |
| Stage 2 — COPY INTO | All blob files already in Snowflake load history |
| Stage 3 — Cleaning | All RAW_SOCIAL rows already cleaned |
| Stage 4 — dbt | All posts/comments already in STG_POST / STG_COMMENT (unless reaction counts changed → MERGE updates them) |
| Stage 5 — Sentiment | All posts/comments already in FACT_SENTIMENT_RESULT |

---

### Late-Arriving Comments

Facebook allows people to comment on posts days or weeks after the post was published. A comment posted today on a post from 3 weeks ago would be missed if the ingestion cursor only looked at new *posts*.

**How we handle it:**

- Ingestion always re-fetches comments on posts created within the last **N days** (configurable, default 7)
- Stage 4 dbt uses MERGE with `unique_key = comment_id` — new comments on old posts are inserted, existing comments with updated reaction counts are updated
- Stage 5 sentiment query picks them up automatically — they have no entry in `FACT_SENTIMENT_RESULT` yet

```sql
-- AUDIT.INGESTION_STATE: effective comment window
last_ingested_at - INTERVAL '7 days'
```

---

## Scheduling

| Pipeline | Schedule | Notes |
|---|---|---|
| Full pipeline (incremental) | Every hour | Default run; fetches only new posts/comments since last run |
| Full backfill | On-demand | One-time historical load; run manually per platform/page |
| Sentiment re-score | On-demand | Triggered when model version changes |
| Validation / reconciliation | Daily | Standalone check independent of pipeline run |

---

## Retry & Failure Handling

| Stage | Retry policy | On final failure |
|---|---|---|
| Stage 1 — Ingestion | 3 retries with exponential backoff | Alert + halt; do not proceed to Stage 2 |
| Stage 2 — COPY INTO | 2 retries | Alert; malformed files logged to COPY_HISTORY |
| Stage 3 — Cleaning | 2 retries | Alert; rejected records go to dead-letter table |
| Stage 4 — Transformation | dbt retry on failed models (`dbt retry`) | Alert; previous successful run data remains intact; re-run only failed models |
| Stage 5 — Sentiment | 3 retries per batch | Alert; partial results written; re-run on next schedule |
| Stage 6 — Validation | 1 retry | Alert only; non-blocking |

- A failure in any stage **halts downstream stages** for that run
- The ingestion state cursor is only advanced after Stage 2 succeeds — a failed run re-processes the same window on the next trigger
- All failures write to `AUDIT.PIPELINE_RUN_LOG` with stage, error message, and timestamp

---

## Audit Table

```sql
CREATE TABLE AUDIT.PIPELINE_RUN_LOG (
    run_id              VARCHAR         NOT NULL DEFAULT UUID_STRING(),
    run_started_at      TIMESTAMP_TZ    NOT NULL,
    run_completed_at    TIMESTAMP_TZ,
    platform            VARCHAR,
    stage               VARCHAR         NOT NULL,
    status              VARCHAR         NOT NULL,   -- 'success', 'failed', 'partial'
    records_ingested    INT,
    records_loaded      INT,
    records_rejected    INT,
    records_scored      INT,
    error_message       VARCHAR,
    CONSTRAINT pk_run_log PRIMARY KEY (run_id)
);
```
