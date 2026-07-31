import logging
import os
from datetime import datetime, timedelta, timezone

from facebook import FacebookClient
from storage import LocalStorage

logger = logging.getLogger(__name__)

# Reaction fields tracked per post — used for change detection and checkpoint storage
_REACTION_FIELDS = [
    "reaction_like", "reaction_love", "reaction_haha",
    "reaction_wow", "reaction_sad", "reaction_angry",
]

# Buffer to handle clock skew between API and local system (10 minutes)
# Subtract this from last_loaded_at when requesting new data to ensure no gaps
CHECKPOINT_BUFFER_MINUTES = 10


def run() -> None:
    """
    Main ingestion entry point.

    Flow:
      1. Read checkpoint (last_loaded_at + known_posts) from local file
      2. For each configured page:
         a. Track 1 — Fetch new posts (since last_loaded_at); fetch ALL comments on each;
                       add new post IDs + full state (created_time, reaction counts,
                       comment_count) to known_posts
         b. Track 2 — For every post in known_posts (checkpoint from previous run, does not
                       contain Track 1 new posts):
                       - Re-fetch post record; compare reaction counts + comment_count
                         against stored state; write new file only if any count changed
                       - Fetch new comments (since last_loaded_at)
                       If ACTIVE_WINDOW_DAYS is set, posts outside the window are skipped
      3. Write raw JSON batches to local filesystem
      4. Update checkpoint — only after all writes succeed
         If ACTIVE_WINDOW_DAYS is set, posts older than the window are pruned from known_posts
    """
    run_ts = datetime.now(timezone.utc)
    page_ids = _get_page_ids()

    client = FacebookClient()
    storage = LocalStorage()

    # Step 1 — read checkpoint
    last_loaded_at, known_posts = storage.read_checkpoint()

    # Validate token for all pages before doing any work — prevents the
    # checkpoint from advancing when the token is expired or invalid.
    for page_id in page_ids:
        client.validate_access(page_id)

    logger.info(
        f"Starting ingestion run at {run_ts.isoformat()} | "
        f"pages={page_ids} | since={last_loaded_at}"
    )

    total_posts = 0
    total_comments = 0

    for page_id in page_ids:
        page_known = known_posts.get(page_id, {})
        posts, comments, updated_known = _ingest_page(
            page_id=page_id,
            client=client,
            storage=storage,
            run_ts=run_ts,
            last_loaded_at=last_loaded_at,
            known_posts=page_known,
        )
        total_posts += posts
        total_comments += comments
        known_posts[page_id] = updated_known

    # Step 4 — update checkpoint only after all pages succeed
    storage.write_checkpoint(run_ts, known_posts)

    logger.info(
        f"Ingestion complete. "
        f"posts={total_posts} comments={total_comments} "
        f"checkpoint updated to {run_ts.isoformat()}"
    )


def _ingest_page(
    page_id: str,
    client: FacebookClient,
    storage: LocalStorage,
    run_ts: datetime,
    last_loaded_at: datetime | None,
    known_posts: dict[str, dict],  # post_id → {created_time, comment_count, reaction_*}
) -> tuple[int, int, dict[str, dict]]:
    """
    Ingest one Facebook page. Returns (post_count, comment_count, updated_known_posts).
    """
    post_count = 0
    comment_count = 0
    updated_known = dict(known_posts)  # copy — will add new post IDs + update changed state

    # Calculate overlap buffer to prevent gaps due to clock skew
    buffered_since = last_loaded_at
    if last_loaded_at:
        buffered_since = last_loaded_at - timedelta(minutes=CHECKPOINT_BUFFER_MINUTES)

    # ----------------------------------------------------------------
    # Track 1: new posts (since last run)
    # ----------------------------------------------------------------
    new_post_ids: list[str] = []

    for batch in client.get_posts(page_id, since=buffered_since):
        post_count += len(batch)
        for p in batch:
            pid = p["id"]
            new_post_ids.append(pid)
            updated_known[pid] = _extract_post_state(p)
        storage.write_posts(
            page_id=page_id,
            records=batch,
            run_ts=run_ts,
            since=last_loaded_at,
        )

    logger.info(f"Page {page_id}: {post_count} new posts")

    # Comments on new posts — no since filter (post is brand new, fetch everything)
    for post_id in new_post_ids:
        count = _fetch_and_write_comments(
            page_id=page_id,
            post_id=post_id,
            since=None,
            client=client,
            storage=storage,
            run_ts=run_ts,
        )
        comment_count += count

    # ----------------------------------------------------------------
    # Track 2: known posts — refresh reaction counts + fetch new comments
    #
    # known_posts comes from the checkpoint written at the end of the
    # previous run — it does not contain new posts from Track 1 above.
    # No exclusion needed; iterate known_posts directly.
    #
    # For each known post:
    #   - Re-fetch the post record and compare reaction counts + comment_count
    #     against the stored state. Write a new file only if any count changed.
    #     Update the checkpoint state with the latest counts.
    #   - Always fetch new comments (since last_loaded_at).
    #
    # If ACTIVE_WINDOW_DAYS is set, posts outside the window are skipped
    # before any API call — no point refreshing posts that will be pruned anyway.
    # ----------------------------------------------------------------
    if last_loaded_at:
        active_posts = _apply_window(known_posts, run_ts)
        skipped = len(known_posts) - len(active_posts)
        logger.info(
            f"Page {page_id}: checking {len(active_posts)} known posts"
            + (f" ({skipped} skipped outside window)" if skipped else "")
        )

        refreshed = 0
        for post_id, stored_state in active_posts.items():
            current_post = client.get_post(post_id)

            if _counts_changed(stored_state, current_post):
                storage.write_posts(
                    page_id=page_id,
                    records=[current_post],
                    run_ts=run_ts,
                    since=last_loaded_at,
                )
                updated_known[post_id] = _extract_post_state(current_post)
                refreshed += 1

            count = _fetch_and_write_comments(
                page_id=page_id,
                post_id=post_id,
                since=buffered_since,
                client=client,
                storage=storage,
                run_ts=run_ts,
            )
            comment_count += count

        logger.info(f"Page {page_id}: {refreshed} posts refreshed (counts changed)")

    logger.info(f"Page {page_id}: {comment_count} comments written")
    return post_count, comment_count, updated_known


def _fetch_and_write_comments(
    page_id: str,
    post_id: str,
    since: datetime | None,
    client: FacebookClient,
    storage: LocalStorage,
    run_ts: datetime,
) -> int:
    count = 0
    for batch in client.get_comments(post_id, since=since):
        count += len(batch)
        storage.write_comments(
            page_id=page_id,
            post_id=post_id,
            records=batch,
            run_ts=run_ts,
            since=since,
        )
    return count


def _extract_post_state(post: dict) -> dict:
    """
    Extract counts from a raw Facebook post object to store in the checkpoint.
    Used for change detection on subsequent runs.
    """
    state = {"created_time": post.get("created_time", "")}
    state["comment_count"] = post.get("comments", {}).get("summary", {}).get("total_count", 0)
    for field in _REACTION_FIELDS:
        state[field] = post.get(field, {}).get("summary", {}).get("total_count", 0)
    return state


def _counts_changed(stored: dict, current_post: dict) -> bool:
    """
    Return True if any reaction count or comment_count differs from the stored state.
    """
    if stored.get("comment_count", 0) != current_post.get("comments", {}).get("summary", {}).get("total_count", 0):
        return True
    for field in _REACTION_FIELDS:
        if stored.get(field, 0) != current_post.get(field, {}).get("summary", {}).get("total_count", 0):
            return True
    return False


def _apply_window(known_posts: dict[str, dict], run_ts: datetime) -> dict[str, dict]:
    """
    If ACTIVE_WINDOW_DAYS is set, return only posts whose created_time falls
    within the window. If not set, return known_posts unchanged.
    """
    window_days_str = os.environ.get("ACTIVE_WINDOW_DAYS")
    if not window_days_str:
        return known_posts

    cutoff = run_ts - timedelta(days=int(window_days_str))
    return {
        pid: state for pid, state in known_posts.items()
        if _parse_created_time(state["created_time"]) >= cutoff
    }


def _parse_created_time(value: str) -> datetime:
    """Parse a Facebook ISO 8601 created_time string to an aware UTC datetime."""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)


def _get_page_ids() -> list[str]:
    raw = os.environ.get("FACEBOOK_PAGE_IDS", "")
    ids = [p.strip() for p in raw.split(",") if p.strip()]
    if not ids:
        raise ValueError("FACEBOOK_PAGE_IDS is not set or empty in environment")
    return ids
