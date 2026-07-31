from typing import Optional
from datetime import datetime
from sqlmodel import SQLModel, Field

class PostTable(SQLModel, table=True):
    """
    Relational representation of a Facebook Post.
    Maps from src.models.post.PostModel.

    This table is optimized for analytical queries and supports
    idempotent Upserts via the post_id primary key.
    """
    __tablename__ = "posts"

    # Primary Key: {platform}_{native_post_id}
    post_id: str = Field(primary_key=True, index=True)
    platform: str = "facebook"
    native_post_id: str
    page_id: str = Field(index=True)
    message: Optional[str] = None
    story: Optional[str] = None
    created_time: datetime = Field(index=True)
    share_count: int = 0
    comment_count: int = 0

    # Flattened ReactionSummary for high-performance SQL aggregation
    reaction_like: int = 0
    reaction_love: int = 0
    reaction_haha: int = 0
    reaction_wow: int = 0
    reaction_sad: int = 0
    reaction_angry: int = 0

    has_attachment: bool = False
    attachment_type: Optional[str] = None
    raw_blob_path: str
    ingested_at: datetime

class CommentTable(SQLModel, table=True):
    """
    Relational representation of a Facebook Comment.
    Maps from src.models.comment.CommentModel.

    Designed for fast retrieval of comments per post and
    chronological analysis of engagement.
    """
    __tablename__ = "comments"

    # Primary Key: {platform}_{native_comment_id}
    comment_id: str = Field(primary_key=True, index=True)
    platform: str = "facebook"
    native_comment_id: str

    # Foreign Key linking to the parent post
    post_id: str = Field(foreign_key="posts.post_id", index=True)
    parent_comment_id: Optional[str] = None
    page_id: str = Field(index=True)
    message: str
    created_time: datetime = Field(index=True)
    author_id: Optional[str] = None
    author_name: Optional[str] = None

    # Flattened ReactionSummary
    reaction_like: int = 0
    reaction_love: int = 0
    reaction_haha: int = 0
    reaction_wow: int = 0
    reaction_sad: int = 0
    reaction_angry: int = 0

    reply_count: int = 0
    is_reply: bool = False
    raw_blob_path: str
    ingested_at: datetime
