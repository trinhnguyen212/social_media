import json
import logging
import os
from datetime import datetime, timedelta, timezone

from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class LocalStorage:
    """
    Handles reading/writing raw JSON files and the checkpoint file
    to the local filesystem.

    Directory layout mirrors the intended Azure Blob structure so
    switching to Azure later requires no changes to the ingestion logic —
    only this class is swapped out.

    raw/
      facebook/
        {page_id}/
          posts/{year}/{month}/{day}/posts_{page_id}_{ts}.json
          comments/{year}/{month}/{day}/comments_{page_id}_{post_id}_{ts}.json

    checkpoints/
      facebook/
        state.json
    """

    def __init__(self):
        self.output_dir = Path(os.environ.get("OUTPUT_DIR", "data/raw"))
        self.checkpoint_dir = Path(os.environ.get("CHECKPOINT_DIR", "checkpoints"))

    # ------------------------------------------------------------------
    # Checkpoint
    # ------------------------------------------------------------------

    def read_checkpoint(self) -> tuple[Optional[datetime], dict[str, dict[str, dict]]]:
        """
        Read last_loaded_at and known_posts from the checkpoint file.
        Returns (None, {}) on first run (full backfill).

        known_posts structure:
          {page_id: {post_id: {created_time, comment_count, reaction_like, ...}}}

        Handles migration from the old format where each post entry was a plain
        created_time string. Migrated entries have all counts set to 0, which
        triggers a post re-fetch and re-write on the first Track 2 run.
        """
        path = self.checkpoint_dir / "facebook" / "state.json"
        if not path.exists():
            logger.info("No checkpoint found. This will be a full backfill run.")
            return None, {}

        with open(path) as f:
            state = json.load(f)

        ts = datetime.fromisoformat(state["last_loaded_at"])
        raw_known = state.get("known_posts", {})
        known_posts = {
            page_id: {pid: _migrate_post_state(v) for pid, v in posts.items()}
            for page_id, posts in raw_known.items()
        }
        logger.info(
            f"Checkpoint loaded: last_loaded_at={ts.isoformat()} "
            f"known_posts={sum(len(v) for v in known_posts.values())} total"
        )
        return ts, known_posts

    def write_checkpoint(
        self,
        last_loaded_at: datetime,
        known_posts: dict[str, dict[str, dict]],
    ) -> None:
        """
        Overwrite the checkpoint file with the new timestamp and known post state.
        If ACTIVE_WINDOW_DAYS is set, posts older than the window are pruned.
        Only called after all files for this run are successfully written.

        known_posts structure:
          {page_id: {post_id: {created_time, comment_count, reaction_like, ...}}}
        """
        path = self.checkpoint_dir / "facebook" / "state.json"
        path.parent.mkdir(parents=True, exist_ok=True)

        window_days_str = os.environ.get("ACTIVE_WINDOW_DAYS")
        if window_days_str:
            cutoff = last_loaded_at - timedelta(days=int(window_days_str))
            known_posts = {
                page_id: {
                    pid: state for pid, state in posts.items()
                    if _parse_iso(state["created_time"]) >= cutoff
                }
                for page_id, posts in known_posts.items()
            }
            total = sum(len(v) for v in known_posts.values())
            logger.info(f"ACTIVE_WINDOW_DAYS={window_days_str}: {total} posts retained after pruning")

        with open(path, "w") as f:
            json.dump(
                {"last_loaded_at": last_loaded_at.isoformat(), "known_posts": known_posts},
                f,
                indent=2,
            )

        logger.info(f"Checkpoint updated: last_loaded_at = {last_loaded_at.isoformat()}")

    # ------------------------------------------------------------------
    # Raw JSON files
    # ------------------------------------------------------------------

    def write_posts(
        self,
        page_id: str,
        records: list[dict],
        run_ts: datetime,
        since: Optional[datetime],
    ) -> str:
        """
        Write a batch of raw post records to a JSON file.
        Returns the file path (used as raw_blob_path in the metadata envelope).
        """
        date_path = self.output_dir / "facebook" / page_id / "posts" / _date_partition(run_ts)
        date_path.mkdir(parents=True, exist_ok=True)

        filename = f"posts_{page_id}_{_ts_suffix(run_ts)}.json"
        file_path = date_path / filename

        self._write_file(
            file_path=file_path,
            records=records,
            meta={
                "source": "facebook_graph_api",
                "source_type": "posts",
                "page_id": page_id,
                "since": since.isoformat() if since else None,
                "run_timestamp": run_ts.isoformat(),
                "record_count": len(records),
            },
        )

        return str(file_path)

    def write_comments(
        self,
        page_id: str,
        post_id: str,
        records: list[dict],
        run_ts: datetime,
        since: Optional[datetime],
    ) -> str:
        """
        Write a batch of raw comment records to a JSON file.
        Returns the file path.
        """
        date_path = self.output_dir / "facebook" / page_id / "comments" / _date_partition(run_ts)
        date_path.mkdir(parents=True, exist_ok=True)

        filename = f"comments_{page_id}_{post_id}_{_ts_suffix(run_ts)}.json"
        file_path = date_path / filename

        self._write_file(
            file_path=file_path,
            records=records,
            meta={
                "source": "facebook_graph_api",
                "source_type": "comments",
                "page_id": page_id,
                "post_id": post_id,
                "since": since.isoformat() if since else None,
                "run_timestamp": run_ts.isoformat(),
                "record_count": len(records),
            },
        )

        return str(file_path)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def read_post_ids_since(
        self,
        page_id: str,
        since: datetime,
        exclude_ids: set[str],
    ) -> list[str]:
        """
        Read post IDs from previously written local JSON files.
        Returns IDs for posts written since the given cutoff, excluding any in exclude_ids.
        No API call — reads from local files only.
        """
        posts_root = self.output_dir / "facebook" / page_id / "posts"
        if not posts_root.exists():
            return []

        post_ids = []
        for json_file in sorted(posts_root.rglob("*.json")):
            with open(json_file, encoding="utf-8") as f:
                envelope = json.load(f)

            run_ts_str = envelope.get("meta", {}).get("run_timestamp")
            if not run_ts_str:
                continue

            file_run_ts = datetime.fromisoformat(run_ts_str)
            if file_run_ts.tzinfo is None:
                file_run_ts = file_run_ts.replace(tzinfo=timezone.utc)

            # Only consider files written within the active window
            if file_run_ts < since:
                continue

            for record in envelope.get("data", []):
                pid = record.get("id")
                if pid and pid not in exclude_ids:
                    post_ids.append(pid)

        # Deduplicate — same post can appear in multiple files if re-fetched
        return list(dict.fromkeys(post_ids))

    def _write_file(self, file_path: Path, records: list[dict], meta: dict) -> None:
        envelope = {"meta": meta, "data": records}
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(envelope, f, ensure_ascii=False, indent=2, default=str)
        logger.info(f"Written {meta['record_count']} records → {file_path}")


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _migrate_post_state(value) -> dict:
    """
    Migrate old checkpoint format (plain created_time string) to the current
    dict format. Counts are set to 0 so that Track 2 always re-fetches and
    re-writes the post on the first run after migration.
    """
    if isinstance(value, str):
        return {
            "created_time": value,
            "comment_count": 0,
            "reaction_like": 0,
            "reaction_love": 0,
            "reaction_haha": 0,
            "reaction_wow": 0,
            "reaction_sad": 0,
            "reaction_angry": 0,
        }
    return value


def _parse_iso(value: str) -> datetime:
    """Parse a Facebook ISO 8601 string to an aware UTC datetime. Returns epoch on failure."""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)


def _date_partition(ts: datetime) -> Path:
    return Path(str(ts.year)) / f"{ts.month:02d}" / f"{ts.day:02d}"


def _ts_suffix(ts: datetime) -> str:
    return ts.strftime("%Y%m%dT%H%M%SZ")
