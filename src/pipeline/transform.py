import json
import logging
from typing import List, Tuple, Dict, Any, Optional
from pathlib import Path
from datetime import datetime
import pandas as pd
from pydantic import ValidationError

from models.post import PostModel, ReactionSummary
from models.comment import CommentModel

logger = logging.getLogger(__name__)

# --- Canonical Mappers ---

def map_raw_post_to_canonical(record: Dict[str, Any], meta: Dict[str, Any], file_path: Path) -> Dict[str, Any]:
    """Maps raw Facebook post record and ingestion metadata to the canonical PostModel schema."""
    native_id = record.get("id", "unknown")

    # Extract reaction counts
    reactions = {
        "like": record.get("reaction_like", {}).get("summary", {}).get("total_count", 0),
        "love": record.get("reaction_love", {}).get("summary", {}).get("total_count", 0),
        "haha": record.get("reaction_haha", {}).get("summary", {}).get("total_count", 0),
        "wow": record.get("reaction_wow", {}).get("summary", {}).get("total_count", 0),
        "sad": record.get("reaction_sad", {}).get("summary", {}).get("total_count", 0),
        "angry": record.get("reaction_angry", {}).get("summary", {}).get("total_count", 0),
    }

    attachments = record.get("attachments", {})
    attachment_type = None
    if attachments:
        # Simple extraction of the first attachment type if available
        att_data = attachments.get("data", [])
        if att_data and isinstance(att_data, list):
            attachment_type = att_data[0].get("type")

    return {
        "post_id": f"facebook_{native_id}",
        "platform": "facebook",
        "native_post_id": native_id,
        "page_id": meta.get("page_id", "unknown"),
        "message": record.get("message"),
        "story": record.get("story"),
        "created_time": record.get("created_time"),
        "share_count": record.get("shares", {}).get("summary", {}).get("total_count", 0),
        "comment_count": record.get("comments", {}).get("summary", {}).get("total_count", 0),
        "reactions": reactions,
        "has_attachment": bool(attachments),
        "attachment_type": attachment_type,
        "raw_blob_path": str(file_path.absolute()),
        "ingested_at": meta.get("run_timestamp"),
    }

def map_raw_comment_to_canonical(record: Dict[str, Any], meta: Dict[str, Any], file_path: Path) -> Dict[str, Any]:
    """Maps raw Facebook comment record and ingestion metadata to the canonical CommentModel schema."""
    native_id = record.get("id", "unknown")

    reactions = {
        "like": record.get("reaction_like", {}).get("summary", {}).get("total_count", 0),
        "love": record.get("reaction_love", {}).get("summary", {}).get("total_count", 0),
        "haha": record.get("reaction_haha", {}).get("summary", {}).get("total_count", 0),
        "wow": record.get("reaction_wow", {}).get("summary", {}).get("total_count", 0),
        "sad": record.get("reaction_sad", {}).get("summary", {}).get("total_count", 0),
        "angry": record.get("reaction_angry", {}).get("summary", {}).get("total_count", 0),
    }

    return {
        "comment_id": f"facebook_{native_id}",
        "platform": "facebook",
        "native_comment_id": native_id,
        "post_id": f"facebook_{meta.get('post_id', 'unknown')}",
        "parent_comment_id": f"facebook_{record['parent_id']}" if "parent_id" in record else None,
        "page_id": meta.get("page_id", "unknown"),
        "message": record.get("message", ""),
        "created_time": record.get("created_time"),
        "author_id": record.get("from", {}).get("id"),
        "author_name": record.get("from", {}).get("name"),
        "reactions": reactions,
        "reply_count": record.get("replies", {}).get("summary", {}).get("total_count", 0),
        "is_reply": "parent_id" in record,
        "raw_blob_path": str(file_path.absolute()),
        "ingested_at": meta.get("run_timestamp"),
    }

# --- Flattening Helpers ---

def flatten_post(model: PostModel) -> Dict[str, Any]:
    """Maps nested PostModel to flat Warehouse schema."""
    return {
        "post_id": model.post_id,
        "platform": model.platform,
        "native_post_id": model.native_post_id,
        "page_id": model.page_id,
        "message": model.message,
        "story": model.story,
        "created_time": model.created_time,
        "share_count": model.share_count,
        "comment_count": model.comment_count,
        "reaction_like": model.reactions.like,
        "reaction_love": model.reactions.love,
        "reaction_haha": model.reactions.haha,
        "reaction_wow": model.reactions.wow,
        "reaction_sad": model.reactions.sad,
        "reaction_angry": model.reactions.angry,
        "has_attachment": model.has_attachment,
        "attachment_type": model.attachment_type,
        "raw_blob_path": model.raw_blob_path,
        "ingested_at": model.ingested_at,
    }

def flatten_comment(model: CommentModel) -> Dict[str, Any]:
    """Maps nested CommentModel to flat Warehouse schema."""
    return {
        "comment_id": model.comment_id,
        "platform": model.platform,
        "native_comment_id": model.native_comment_id,
        "post_id": model.post_id,
        "parent_comment_id": model.parent_comment_id,
        "page_id": model.page_id,
        "message": model.message,
        "created_time": model.created_time,
        "author_id": model.author_id,
        "author_name": model.author_name,
        "reaction_like": model.reactions.like,
        "reaction_love": model.reactions.love,
        "reaction_haha": model.reactions.haha,
        "reaction_wow": model.reactions.wow,
        "reaction_sad": model.reactions.sad,
        "reaction_angry": model.reactions.angry,
        "reply_count": model.reply_count,
        "is_reply": model.is_reply,
        "raw_blob_path": model.raw_blob_path,
        "ingested_at": model.ingested_at,
    }

# --- Registry Configuration ---

ENTITY_REGISTRY = {
    "posts": {
        "model": PostModel,
        "flatten": flatten_post,
        "map": map_raw_post_to_canonical,
    },
    "comments": {
        "model": CommentModel,
        "flatten": flatten_comment,
        "map": map_raw_comment_to_canonical,
    }
}

# Precise column definitions to ensure empty DataFrames have the correct schema
POST_COLUMNS = [
    "post_id", "platform", "native_post_id", "page_id", "message", "story",
    "created_time", "share_count", "comment_count", "reaction_like",
    "reaction_love", "reaction_haha", "reaction_wow", "reaction_sad",
    "reaction_angry", "has_attachment", "attachment_type", "raw_blob_path", "ingested_at"
]

COMMENT_COLUMNS = [
    "comment_id", "platform", "native_comment_id", "post_id", "parent_comment_id",
    "page_id", "message", "created_time", "author_id", "author_name",
    "reaction_like", "reaction_love", "reaction_haha", "reaction_wow",
    "reaction_sad", "reaction_angry", "reply_count", "is_reply",
    "raw_blob_path", "ingested_at"
]

SCHEMA_MAP = {
    "posts": POST_COLUMNS,
    "comments": COMMENT_COLUMNS
}

# --- Processing Logic ---

def find_raw_files(root_dir: str, entity_type: str) -> List[Path]:
    """Returns all JSON files in the entity-specific landing zone recursively."""
    path = Path(root_dir)
    pattern = f"**/{entity_type}/**/*.json"
    return list(path.glob(pattern))

def load_json_content(file_path: Path) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Reads a file and extracts the metadata envelope and records."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = json.load(f)
            return content.get("meta", {}), content.get("data", [])
    except json.JSONDecodeError as e:
        logger.error(f"JSON Parse Error in {file_path}: {e}")
    except (OSError, PermissionError) as e:
        logger.error(f"I/O Error reading {file_path}: {e}")
    return {}, []

def validate_and_flatten(record: Dict[str, Any], meta: Dict[str, Any], entity_type: str, file_path: Path) -> Optional[Dict[str, Any]]:
    """
    Maps a raw record to canonical schema, validates against the model, and flattens it.
    """
    config = ENTITY_REGISTRY.get(entity_type)
    if not config:
        return None

    rec_id = record.get("id") or "Unknown ID"

    try:
        # 1. Canonical Mapping
        logger.debug(f"Processing {entity_type} record {rec_id} from {file_path}")
        canonical_data = config["map"](record, meta, file_path)

        # 2. Model Validation
        model = config["model"](**canonical_data)

        # 3. Flattening
        flat = config["flatten"](model)

        logger.debug(f"Successfully mapped and validated {entity_type} record {rec_id}")
        return flat

    except ValidationError as e:
        logger.warning(f"Validation failed for {entity_type} record {rec_id} in {file_path}: {e.json()}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error transforming {entity_type} record {rec_id} in {file_path}: {e}", exc_info=True)
        return None

def transform_entity(root_dir: str, entity_type: str) -> pd.DataFrame:
    """Orchestrates the transformation for a specific entity type."""
    files = find_raw_files(root_dir, entity_type)

    stats = {"found": len(files), "processed": 0, "skipped": 0}
    flattened_records = []

    for file_path in files:
        meta, records = load_json_content(file_path)
        for rec in records:
            flat = validate_and_flatten(rec, meta, entity_type, file_path)
            if flat:
                flattened_records.append(flat)
                stats["processed"] += 1
            else:
                stats["skipped"] += 1

    # Deduplicate records by their primary key to avoid PostgreSQL CardinalityViolation
    # (same record appearing in multiple raw files)
    if flattened_records:
        df = pd.DataFrame(flattened_records, columns=SCHEMA_MAP.get(entity_type, []))
        pk = "post_id" if entity_type == "posts" else "comment_id"
        df = df.drop_duplicates(subset=[pk], keep="last")
        return df

    return pd.DataFrame(columns=SCHEMA_MAP.get(entity_type, []))

def transform_raw_data(raw_data_dir: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Transforms all raw data from the landing zone.
    Returns: (df_posts, df_comments)
    """
    logger.info(f"Starting transformation pipeline in {raw_data_dir}...")

    df_posts = transform_entity(raw_data_dir, "posts")
    df_comments = transform_entity(raw_data_dir, "comments")

    logger.info("Transformation pipeline complete.")
    return df_posts, df_comments