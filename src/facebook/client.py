import os
import time
import logging
import requests
from datetime import datetime, timezone
from typing import Optional, Generator

# Business Use Case rate limit threshold — back off when usage exceeds this %
_BUC_BACKOFF_THRESHOLD = 75

logger = logging.getLogger(__name__)

# Fields requested for each post
POST_FIELDS = ",".join([
    "id",
    "message",
    "story",
    "created_time",
    "shares",
    "attachments",
    "comments.limit(0).summary(total_count)",
    "reactions.type(LIKE).limit(0).summary(total_count).as(reaction_like)",
    "reactions.type(LOVE).limit(0).summary(total_count).as(reaction_love)",
    "reactions.type(HAHA).limit(0).summary(total_count).as(reaction_haha)",
    "reactions.type(WOW).limit(0).summary(total_count).as(reaction_wow)",
    "reactions.type(SAD).limit(0).summary(total_count).as(reaction_sad)",
    "reactions.type(ANGRY).limit(0).summary(total_count).as(reaction_angry)",
])

# Fields requested for each comment.
# parent_id identifies the parent comment for replies (filter=stream flattens
# all levels into one list — parent_id is required to reconstruct hierarchy).
COMMENT_FIELDS = ",".join([
    "id",
    "message",
    "created_time",
    "from",
    "parent",
    "comment_count",
    "reactions.type(LIKE).limit(0).summary(total_count).as(reaction_like)",
    "reactions.type(LOVE).limit(0).summary(total_count).as(reaction_love)",
    "reactions.type(HAHA).limit(0).summary(total_count).as(reaction_haha)",
    "reactions.type(WOW).limit(0).summary(total_count).as(reaction_wow)",
    "reactions.type(SAD).limit(0).summary(total_count).as(reaction_sad)",
    "reactions.type(ANGRY).limit(0).summary(total_count).as(reaction_angry)",
])


class FacebookClient:
    MAX_RETRIES = 5
    RETRY_BACKOFF_BASE = 2      # seconds — doubles on each retry
    PAGE_LIMIT = 100            # records per API page

    def __init__(self):
        self.access_token = os.environ["FACEBOOK_ACCESS_TOKEN"]
        self.api_version = os.environ.get("FACEBOOK_API_VERSION", "v19.0")
        self.base_url = f"https://graph.facebook.com/{self.api_version}"

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def validate_access(self, page_id: str) -> None:
        """
        Verify the access token is valid and can reach the given page.
        Raises RuntimeError on auth failure so the pipeline halts before
        doing any work — preventing the checkpoint from advancing on a
        broken token.
        """
        url = f"{self.base_url}/{page_id}"
        params = {"fields": "id", "access_token": self.access_token}
        try:
            data = self._request(url, params)
        except Exception as e:
            raise RuntimeError(
                f"Token validation failed for page {page_id}: {e}. "
                "Refresh FACEBOOK_ACCESS_TOKEN and retry."
            ) from e
        if "id" not in data:
            raise RuntimeError(
                f"Token validation returned unexpected response for page {page_id}: {data}"
            )
        logger.info(f"Token validated for page {page_id}")

    def get_post(self, post_id: str) -> dict:
        """
        Fetch a single post record with all standard fields.
        Used in Track 2 to check for reaction/comment count changes.
        """
        url = f"{self.base_url}/{post_id}"
        params = {"fields": POST_FIELDS, "access_token": self.access_token}
        return self._request(url, params)

    def get_posts(
        self,
        page_id: str,
        since: Optional[datetime] = None,
    ) -> Generator[list[dict], None, None]:
        """
        Yield batches of raw post objects for a page.
        If since is provided, only posts created after that timestamp are returned.
        """
        params = {
            "fields": POST_FIELDS,
            "limit": self.PAGE_LIMIT,
            "access_token": self.access_token,
        }
        if since:
            params["since"] = int(since.timestamp())

        url = f"{self.base_url}/{page_id}/posts"
        yield from self._paginate(url, params)

    def get_feed(
        self,
        page_id: str,
        since: Optional[datetime] = None,
    ) -> Generator[list[dict], None, None]:
        """
        Yield batches of feed items for a page since the given timestamp.
        Feed items include posts that have any new activity (new posts OR new
        comments on posts of any age) — covers what /posts?since= misses.
        Each item is a raw post object; caller is responsible for extracting IDs.
        """
        params = {
            "fields": POST_FIELDS,
            "limit": self.PAGE_LIMIT,
            "access_token": self.access_token,
        }
        if since:
            params["since"] = int(since.timestamp())

        url = f"{self.base_url}/{page_id}/feed"
        yield from self._paginate(url, params)

    def get_comments(
        self,
        post_id: str,
        since: Optional[datetime] = None,
    ) -> Generator[list[dict], None, None]:
        """
        Yield batches of raw comment objects for a post.
        filter=stream returns all levels (top-level + replies) in chronological
        order. parent field on each record identifies the parent comment for replies.

        When since is set, the API switches to time-based pagination. Known
        Facebook quirk: subsequent pages can leak records older than the since
        boundary. Records are filtered client-side to enforce the cutoff.
        """
        params = {
            "fields": COMMENT_FIELDS,
            "limit": self.PAGE_LIMIT,
            "filter": "stream",     # includes replies inline, parent field identifies hierarchy
            "access_token": self.access_token,
        }
        if since:
            params["since"] = int(since.timestamp())

        since_ts = since.timestamp() if since else None
        url = f"{self.base_url}/{post_id}/comments"

        for batch in self._paginate(url, params):
            if since_ts is None:
                yield batch
            else:
                # Filter out leaked records older than the since boundary
                filtered = [
                    c for c in batch
                    if _parse_ts(c.get("created_time")) >= since_ts
                ]
                if filtered:
                    yield filtered

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _paginate(
        self,
        url: str,
        params: dict,
    ) -> Generator[list[dict], None, None]:
        """
        Follow Facebook cursor-based pagination.
        Yields one list of records per API page.
        """
        while url:
            data = self._request(url, params)
            records = data.get("data", [])

            if records:
                yield records

            # Follow next page cursor if present; clear params (they are
            # already encoded in the next URL returned by Facebook)
            next_url = data.get("paging", {}).get("next")
            url = next_url
            params = {}     # params are embedded in the next URL

            if next_url:
                time.sleep(0.5)     # stay well underrate limit

    def _request(self, url: str, params: dict) -> dict:
        """
        Make a single GET request with exponential backoff on failures.
        Checks both the legacy X-App-Usage header and the Business Use Case
        (BUC) X-Business-Use-Case-Usage header for rate limit pressure.
        """
        for attempt in range(self.MAX_RETRIES):
            try:
                response = requests.get(url, params=params, timeout=30)

                # Check BUC rate limit header before inspecting status code —
                # Facebook may return 200 with a nearly-exhausted BUC budget
                self._check_rate_limit_headers(response)

                # Explicit rate limit response
                if response.status_code == 429 or (
                    response.status_code == 400
                    and "Application request limit" in response.text
                ):
                    wait = self.RETRY_BACKOFF_BASE ** attempt
                    logger.warning(f"Rate limit hit. Waiting {wait}s before retry.")
                    time.sleep(wait)
                    continue

                if not response.ok:
                    logger.error(f"API error {response.status_code}: {response.text}")
                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                if attempt == self.MAX_RETRIES - 1:
                    raise
                wait = self.RETRY_BACKOFF_BASE ** attempt
                logger.warning(f"Request failed ({e}). Retry {attempt + 1}/{self.MAX_RETRIES} in {wait}s.")
                time.sleep(wait)

        raise RuntimeError(f"All {self.MAX_RETRIES} retries exhausted for {url}")

    def _check_rate_limit_headers(self, response: requests.Response) -> None:
        """
        Log rate limit usage from Facebook response headers.
        Backs off proactively when BUC usage exceeds the threshold.
        Facebook uses X-Business-Use-Case-Usage for page-token requests
        (the relevant limit for this pipeline) and X-App-Usage for app-level.
        """
        import json as _json

        buc_header = response.headers.get("X-Business-Use-Case-Usage")
        if buc_header:
            try:
                buc = _json.loads(buc_header)
                for _biz_id, entries in buc.items():
                    for entry in entries:
                        pct = entry.get("call_count", 0)
                        if pct >= _BUC_BACKOFF_THRESHOLD:
                            wait = 60
                            logger.warning(
                                f"BUC rate limit at {pct}% — backing off {wait}s "
                                f"(type={entry.get('type')}, estimated_time_to_regain_access={entry.get('estimated_time_to_regain_access')}s)"
                            )
                            time.sleep(wait)
            except Exception:
                pass  # malformed header — do not crash the pipeline

        app_header = response.headers.get("X-App-Usage")
        if app_header:
            try:
                app = _json.loads(app_header)
                pct = max(app.get("call_count", 0), app.get("total_time", 0), app.get("total_cputime", 0))
                if pct >= _BUC_BACKOFF_THRESHOLD:
                    logger.warning(f"App-level rate limit at {pct}% of capacity")
            except Exception:
                pass


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _parse_ts(value: str | None) -> float:
    """Parse a Facebook ISO 8601 timestamp to a Unix float. Returns 0.0 on failure."""
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0
