# Sentiment Analysis

## Overview

The sentiment analysis layer classifies each Facebook post and comment as **positive**, **negative**, or **neutral**. This is the primary analytical output of the pipeline and feeds dashboards and alerting for fire & emergency public page monitoring.

Sentiment scoring is **not done inside Snowflake or dbt**. It is a Python step in the pipeline (Stage 5) that:
1. Reads unscored posts and comments from Snowflake staging tables
2. Calls an external AI API in batches
3. Writes results back to `ANALYTICS.FACT_SENTIMENT_RESULT` in Snowflake

dbt does not make external API calls — it only handles SQL transformations inside Snowflake. The sentiment Python step runs between the dbt transformation stage and the final validation stage.

---

## Where It Sits in the Pipeline

```
dbt run (Stage 4 — Transformation)
    │  STG_POST and STG_COMMENT are now populated
    ▼
Python sentiment job (Stage 5)
    │
    ├── 1. Query Snowflake: SELECT unscored posts/comments from STG_POST, STG_COMMENT
    │
    ├── 2. Filter: skip low-quality records (quality_flag IS NOT NULL)
    │
    ├── 3. Batch comments (e.g. 50 per API call)
    │
    ├── 4. Call AI API → get label + confidence per comment
    │
    ├── 5. Apply reaction enrichment (is_reaction_flagged)
    │
    └── 6. MERGE results → ANALYTICS.FACT_SENTIMENT_RESULT
```

---

## What You Need to Make the API Call

### Claude API (Anthropic) — recommended

| Requirement | Detail |
|---|---|
| **API key** | `ANTHROPIC_API_KEY` — obtained from console.anthropic.com |
| **Python SDK** | `pip install anthropic` |
| **Model** | `claude-haiku-4-5` (fast, cheap) or `claude-sonnet-4-6` (higher accuracy) |
| **Internet access** | The Python job must be able to reach `api.anthropic.com` |
| **Rate limits** | Anthropic enforces token-per-minute and request-per-minute limits depending on tier |

### Azure AI Language — alternative

| Requirement | Detail |
|---|---|
| **Endpoint** | `AZURE_LANGUAGE_ENDPOINT` — from Azure portal (Cognitive Services resource) |
| **API key** | `AZURE_LANGUAGE_API_KEY` — from Azure portal |
| **Python SDK** | `pip install azure-ai-textanalytics` |
| **Batch size** | Up to 10 documents per request |

### Hugging Face — local/offline alternative

| Requirement | Detail |
|---|---|
| **Model** | `cardiffnlp/twitter-roberta-base-sentiment-latest` (download once) |
| **Python SDK** | `pip install transformers torch` |
| **Hardware** | GPU recommended for reasonable throughput; CPU works for small volumes |
| **No API key** | Runs fully locally — no external calls |

---

## Batching Logic

Every comment needs a sentiment score, but you do **not** make one API call per comment. Comments are grouped into batches and sent in a single call.

```
All unscored comments from Snowflake
            │
            ▼
  Filter out low-quality comments       ← skip: too_short, url_only, spam_suspected
            │
            ▼
  Split into batches of N comments      ← N = 50 for Claude, 10 for Azure AI
            │
  ┌─────────┴──────────┐
  │  Batch 1 (50)      │  Batch 2 (50)  │  Batch 3 (50)  │  ...
  └─────────┬──────────┘
            │  1 API call per batch
            ▼
  AI API returns label + confidence
  for each comment in the batch
            │
            ▼
  Apply reaction enrichment flag
            │
            ▼
  MERGE into FACT_SENTIMENT_RESULT
```

### Prompt template (Claude — batch)

```
You are a sentiment classifier for public feedback on emergency services.

For each item below, classify the text as exactly one of: positive, negative, neutral.
Provide a confidence score from 0.0 to 1.0.

Respond in JSON only — an array matching the input order:
[{"id": "...", "label": "...", "confidence": 0.0}, ...]

Comments:
[
  {"id": "comment_001", "text": "Great response time from the crew!"},
  {"id": "comment_002", "text": "No one showed up for 2 hours!!"},
  {"id": "comment_003", "text": "Stay safe everyone."}
]
```

Response:
```json
[
  {"id": "comment_001", "label": "positive",  "confidence": 0.94},
  {"id": "comment_002", "label": "negative",  "confidence": 0.97},
  {"id": "comment_003", "label": "neutral",   "confidence": 0.81}
]
```

---

## Incremental Scoring — Only Score New Records

On every pipeline run, only **new** unscored comments are sent to the API. Already-scored comments are skipped.

```sql
-- Query Snowflake for unscored comments
SELECT
    c.comment_id,
    c.text_cleaned,
    c.reaction_total,
    c.reaction_angry,
    c.reaction_sad
FROM STAGING.STG_COMMENT c
LEFT JOIN ANALYTICS.FACT_SENTIMENT_RESULT r
    ON r.source_id = c.comment_id
    AND r.source_type = 'comment'
WHERE r.sentiment_id IS NULL          -- not yet scored
  AND c.has_text = TRUE               -- has usable text
  AND c.quality_flag IS NULL          -- passed cleaning
```

Same query for posts, replacing `STG_COMMENT` with `STG_POST`.

---

## Skipping Low-Quality Records

Comments that were flagged during cleaning are skipped — no point calling the API for them:

| Flag | Reason to skip |
|---|---|
| `too_short` | Fewer than 3 words — not enough signal |
| `url_only` | Just a link, no opinion text |
| `spam_suspected` | Duplicate comment — unreliable signal |
| `bot_suspected` | Automated post — not genuine public feedback |
| `high_emoji_ratio` | Optional — score these if emoji sentiment is relevant |

Skipped records are still inserted into `FACT_SENTIMENT_RESULT` with `sentiment_label = 'skipped'` and the skip reason in `model_name` — so they are accounted for, not missing.

---

## Reaction Enrichment (after API call)

After getting the AI label, apply reaction enrichment before writing to Snowflake:

```python
def apply_reaction_flag(reaction_angry: int, reaction_sad: int, reaction_total: int) -> bool:
    if reaction_total == 0:
        return False
    return (reaction_angry + reaction_sad) / reaction_total > 0.20
```

- `is_reaction_flagged = TRUE` means the reaction pattern suggests negative sentiment regardless of text label
- Applies to both posts and comments wherever `reaction_total > 0`

---

## Writing Results to Snowflake

Results are written using MERGE (idempotent — safe to re-run):

```python
import snowflake.connector

merge_sql = """
MERGE INTO ANALYTICS.FACT_SENTIMENT_RESULT AS target
USING (SELECT %s AS sentiment_id, %s AS source_type, %s AS source_id,
              %s AS origin_source_id, %s AS text_analyzed,
              %s AS sentiment_label, %s AS confidence_score,
              %s AS is_reaction_flagged, %s AS detected_language,
              %s AS model_name, CURRENT_TIMESTAMP() AS analyzed_at) AS source
ON target.source_id = source.source_id
AND target.source_type = source.source_type
AND target.model_name = source.model_name
WHEN NOT MATCHED THEN INSERT (...)  VALUES (...)
"""
```

- MERGE key: `(source_id, source_type, model_name)` — allows multiple model versions to coexist
- Never update existing scores — insert a new row when re-scoring with a new model version

---

## Model Options Comparison

| Option | API key needed | Cost model | Accuracy | Best for |
|---|---|---|---|---|
| **Claude API (Anthropic)** | `ANTHROPIC_API_KEY` | Per token (input + output) | High — understands nuanced emergency language | Recommended starting point |
| **Azure AI Language** | `AZURE_LANGUAGE_API_KEY` + endpoint | Per 1,000 text records | Medium — good for simple positive/negative | All-Azure stack preference |
| **Hugging Face (local)** | None | Infrastructure cost only | Medium — depends on model chosen | High volume, cost-sensitive |

---

## Re-scoring with a New Model

When switching model versions:
1. Set `SENTIMENT_PROVIDER` or model name to the new version
2. Run pipeline in `rescore` mode — queries all records regardless of existing scores
3. New scores are inserted as new rows with the new `model_name`
4. Old scores remain — analysts can compare model versions in Snowflake
5. Update a `CURRENT_MODEL` config table or view to point dashboards at the latest model version

---

## Volume & Cost Estimates

| Scenario | Comments/run | API calls (batches of 50) | Est. Claude Haiku cost |
|---|---|---|---|
| Incremental (daily) | ~500 new comments | ~10 calls | < $0.01 |
| Weekly catch-up | ~3,500 | ~70 calls | < $0.05 |
| Historical backfill (1 year) | ~500,000 | ~10,000 calls | ~$5–10 |

> Costs are approximate and depend on average comment length (token count). Claude Haiku is significantly cheaper than Sonnet — use Haiku for batch scoring, Sonnet only if accuracy on ambiguous text matters.

---

## Language Handling

- Detect language before scoring using `langdetect` or Azure Language Detection
- Store `detected_language` on every `FACT_SENTIMENT_RESULT` row
- For non-English comments: use a multilingual model or translate first (Azure Translator) before passing to Claude
- Flag records where language detection confidence is low — score with lower weight in dashboards

---

## Quality & Monitoring

- Log label distribution per run: `% positive / % negative / % neutral / % skipped`
- Alert if `% negative` exceeds `NEGATIVE_ALERT_THRESHOLD` (default 40%) — may indicate an active incident driving public criticism
- Track average `confidence_score` per run — sustained low confidence suggests model drift or unusual language patterns
- All results include `model_name` for full auditability and version comparison

---

## Environment Variables

| Variable | Description |
|---|---|
| `SENTIMENT_PROVIDER` | `claude`, `azure`, or `huggingface` |
| `ANTHROPIC_API_KEY` | API key from console.anthropic.com (Claude) |
| `ANTHROPIC_MODEL` | Model ID e.g. `claude-haiku-4-5-20251001` or `claude-sonnet-4-6` |
| `AZURE_LANGUAGE_ENDPOINT` | Azure AI Language service endpoint |
| `AZURE_LANGUAGE_API_KEY` | Azure AI Language API key |
| `HF_MODEL_NAME` | Hugging Face model name (local option) |
| `SENTIMENT_BATCH_SIZE` | Number of comments per API call (default: `50`) |
| `SENTIMENT_CONFIDENCE_THRESHOLD` | Minimum confidence to accept a label (default: `0.6`) |
| `NEGATIVE_ALERT_THRESHOLD` | % negative in a batch that triggers an alert (default: `0.4`) |
