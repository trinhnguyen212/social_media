from pydantic import BaseModel, computed_field
from datetime import datetime
from typing import Optional


class ReactionSummary(BaseModel):
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


class PostModel(BaseModel):
    post_id: str                        # facebook_{native_post_id}
    platform: str = "facebook"
    native_post_id: str
    page_id: str
    message: Optional[str] = None
    story: Optional[str] = None
    created_time: datetime
    share_count: int = 0
    comment_count: int = 0
    reactions: ReactionSummary = ReactionSummary()
    has_attachment: bool = False
    attachment_type: Optional[str] = None
    raw_blob_path: str
    ingested_at: datetime
