import logging
import time
from typing import Tuple
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

from warehouse.postgres import PostgresStorage
from warehouse.warehouse import PostTable, CommentTable

logger = logging.getLogger(__name__)

def load_data(storage: PostgresStorage, df_posts: pd.DataFrame, df_comments: pd.DataFrame) -> Tuple[int, int]:
    """
    Coordinates the loading of transformed DataFrames into the PostgreSQL warehouse.

    Args:
        storage: An initialized PostgresStorage instance.
        df_posts: Cleaned posts DataFrame.
        df_comments: Cleaned comments DataFrame.

    Returns:
        A tuple containing (total_posts_loaded, total_comments_loaded).
    """
    logger.info("Starting the Load phase of the ETL pipeline...")
    start_time = time.perf_counter()

    posts_loaded = 0
    comments_loaded = 0

    # 1. Load Posts
    try:
        posts_loaded = storage.upsert_dataframe(df_posts, PostTable)
    except SQLAlchemyError as e:
        logger.error(f"Database error while loading posts: {e}")
    except Exception as e:
        logger.error(f"Unexpected error loading posts: {e}")

    # 2. Load Comments
    try:
        comments_loaded = storage.upsert_dataframe(df_comments, CommentTable)
    except SQLAlchemyError as e:
        logger.error(f"Database error while loading comments: {e}")
    except Exception as e:
        logger.error(f"Unexpected error loading comments: {e}")

    elapsed = time.perf_counter() - start_time
    logger.info(f"Load phase complete in {elapsed:.2f}s. Total records synced: {posts_loaded + comments_loaded} "
                f"(Posts: {posts_loaded}, Comments: {comments_loaded})")

    return posts_loaded, comments_loaded
