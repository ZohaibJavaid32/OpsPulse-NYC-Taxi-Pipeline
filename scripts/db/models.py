from contextlib import contextmanager
import pyodbc
from scripts.config import DB_CONFIG , TABLE_NAME
from scripts.logger import setup_logger
from scripts.db.connection import get_sql_connection
import logging


setup_logger()

logger = logging.getLogger("MODELS MODULE")

def create_table(cursor: pyodbc.Cursor) -> None:
    """
    Create 'taxi_trips' table.

    Args: 
        Database Cursor Object.
    """

    create_table_query = f"""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = '{TABLE_NAME}' and xtype = 'U')
    CREATE TABLE {TABLE_NAME} (
        id INT IDENTITY(1,1) PRIMARY KEY,
        tpep_pickup_datetime DATETIME2,
        tpep_dropoff_datetime DATETIME2,
        pickup_hour INT,
        pickup_day VARCHAR(20),
        trip_distance FLOAT,
        POLocationID INT,
        DOLocationID INT,
        trip_duration_min FLOAT,
        trip_speed_kmh FLOAT,
        fare_amount FLOAT,
        fare_per_km FLOAT,
        passenger_count FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        improvement_surcharge FLOAT,
        congestion_surcharge FLOAT,
        total_amount FLOAT,
        extracted_at DATETIME DEFAULT GETDATE()
        )
    """

    cursor.execute(create_table_query)
    logger.info(f"Table {TABLE_NAME} created or already exists.")

    cursor.execute(f"""
    IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'idx_pickup_datetime')
    CREATE INDEX idx_pickup_datetime ON {TABLE_NAME}(tpep_pickup_datetime)
    """)

    cursor.execute(f"""
    IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'idx_day_hour')
    CREATE INDEX idx_day_hour ON {TABLE_NAME}(pickup_day, pickup_hour)
    """)

    cursor.execute(f"""
    IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'idx_total_amount')
    CREATE INDEX idx_total_amount ON {TABLE_NAME}(total_amount)
    """)

    cursor.execute(f"""
    IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'idx_extracted_at')
    CREATE INDEX idx_extracted_at ON {TABLE_NAME}(extracted_at)
    """)

    logger.info(f"Indexes created for {TABLE_NAME}.")


if __name__ == "__main__":
    try:
        with get_sql_connection() as conn:
            cursor = conn.cursor()

            create_table(cursor)
    except Exception as e:
        logger.error(f"DB Connection Failed. {e}")