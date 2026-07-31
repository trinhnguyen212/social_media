import logging
import os
import time
from typing import Type, List, Any
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import SQLModel, create_engine, Session

logger = logging.getLogger(__name__)

class PostgresStorage:
    """
    Handles the connection and idempotent loading of data
    from Pandas DataFrames into the PostgreSQL warehouse.
    """

    def __init__(self):
        """
        Initializes the SQLModel engine using environment variables.
        """
        user = os.environ.get("POSTGRES_USER", "postgres")
        password = os.environ.get("POSTGRES_PASSWORD", "postgres")
        host = os.environ.get("POSTGRES_HOST", "postgres")
        port = os.environ.get("POSTGRES_PORT", "5432")
        db = os.environ.get("POSTGRES_DB", "social_media")

        database_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"

        try:
            self.engine = create_engine(database_url)
            # Test connection
            with self.engine.connect() as conn:
                pass
            logger.info(f"Successfully connected to PostgreSQL warehouse at {host}:{port}/{db}")
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def create_db_and_tables(self) -> None:
        """
        Bootstraps the database by creating all tables defined in the warehouse models.
        """
        try:
            SQLModel.metadata.create_all(self.engine)
            logger.info("Warehouse tables verified/created successfully.")
        except SQLAlchemyError as e:
            logger.error(f"Error creating warehouse tables: {e}")
            raise

    def upsert_dataframe(self, df: pd.DataFrame, table_model: Type[SQLModel]) -> int:
        """
        Performs a bulk Upsert (Insert or Update) of a DataFrame into the specified table.
        Supports composite primary keys for idempotency.

        Args:
            df: The cleaned Pandas DataFrame to load.
            table_model: The SQLModel class representing the target table.

        Returns:
            The number of records processed.
        """
        if df.empty:
            logger.info(f"DataFrame for {table_model.__tablename__} is empty. Skipping load.")
            return 0

        table_name = table_model.__tablename__
        records = df.to_dict(orient="records")
        start_time = time.perf_counter()

        session = Session(self.engine)
        try:
            # 1. Construct the base INSERT statement
            stmt = insert(table_model).values(records)

            # 2. Identify ALL primary key columns for the conflict target (Composite Key support)
            index_elements = [col.name for col in table_model.__table__.primary_key.columns]

            # 3. Define the update mapping: update every column except the PKs
            update_dict = {
                col.name: stmt.excluded[col.name]
                for col in table_model.__table__.columns
                if not col.primary_key
            }

            # 4. Apply the ON CONFLICT DO UPDATE logic
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=index_elements,
                set_=update_dict
            )

            session.execute(upsert_stmt)
            session.commit()

            elapsed = time.perf_counter() - start_time
            row_count = len(records)

            logger.info(
                f"Successfully upserted {row_count} records into [{table_name}] "
                f"in {elapsed:.2f}s ({row_count/max(elapsed, 0.001):.0f} rows/sec)."
            )
            return row_count

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error during upsert into [{table_name}]: {e}")
            raise
        finally:
            session.close()
