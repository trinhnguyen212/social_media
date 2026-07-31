from pydantic import BaseModel, computed_field
from datetime import datetime
from typing import Optional
from .post import ReactionSummary


class CommentModel(BaseModel):
    comment_id: str                     # facebook_{native_comment_id}
    platform: str = "facebook"
    native_comment_id: str
    post_id: str
    parent_comment_id: Optional[str] = None
    page_id: str
    message: str
    created_time: datetime
    author_id: Optional[str] = None
    author_name: Optional[str] = None
    reactions: ReactionSummary = ReactionSummary()
    reply_count: int = 0
    is_reply: bool = False
    raw_blob_path: str
    ingested_at: datetime
