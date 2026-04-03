from contextlib import contextmanager
import pyodbc
from config import DB_CONFIG , TABLE_NAME
from scripts.logger import setup_logger
import logging
from datetime import datetime
import pandas as pd

setup_logger()

logger = logging.getLogger("DB CONNECTION MODULE")


@contextmanager
def get_sql_connection():
    """
    Context manager to ensure proper cleanup of SQL Server Connection.
    Yields:
        Active DB Connection
    """

    connection = None
    try:
        # Build Connection String
        if DB_CONFIG['username']:
            conn_str = (
                f"DRIVER={DB_CONFIG['driver']};"
                f"SERVER={DB_CONFIG['server']};"
                f"DATABASE={DB_CONFIG['database']};"
                f"UID={DB_CONFIG['username']};"
                f"PWD={DB_CONFIG['password']}"
            )
        else:
            # Window Authentication
            conn_str = (
                f"DRIVER={DB_CONFIG['driver']};"
                f"SERVER={DB_CONFIG['server']};"
                f"DATABASE={DB_CONFIG['database']};"
                f"Trusted_Connection=yes"
            )
        connection = pyodbc.connect(conn_str)
        logger.info("Database Connection Established")
        yield connection
    except pyodbc.Error as e:
        logger.error(f"Database Connection Error {e}")
        raise


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

def insert_taxi_data(connection:pyodbc.Connection , file_path:str) -> int:
    """
        Read Data from Transformed Parquet File and Insert into Table.
        
        Args:
            connection : Active DB Connection
            file_path : Path of Transformed Parquet File.
        
        Returns : Number of Records inserted.
    """

    cursor = connection.cursor()

    try:

        # Create table if not exists
        create_table(cursor)
        connection.commit()

        # Prepate Insert Statements

        insert_query = f"""
        INSERT INTO {TABLE_NAME} 
        (tpep_pickup_datetime,tpep_dropoff_datetime,pickup_hour,pickup_day,trip_distance,trip_duration_min,
        trip_speed_kmh,fare_amount,fare_per_km,passenger_count,extra,mta_tax,tip_amount,tolls_amount,
        improvement_surcharge,congestion_surcharge,total_amount,extracted_at) 
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        
        # prepare data for inserts
        current_time  = datetime.now()

        logger.info(f"Reading Data from Parquet File")

        df = pd.read_parquet(file_path)

        # Handle NaN -> None (Important for SQL Server)
        df = df.where(pd.notnull(df) , None)
        
        logger.info(f"Preparing Data for insertion....")

        data = [
            (
                row["tpep_pickup_datetime"],
                row["tpep_dropoff_datetime"],
                row["pickup_hour"],
                row["pickup_day"],
                row["trip_distance"],
                row["trip_duration_min"],
                row["trip_speed_kmh"],
                row["fare_amount"],
                row["fare_per_km"],
                row["passenger_count"],
                row["extra"],
                row["mta_tax"],
                row["tip_amount"],
                row["tolls_amount"],
                row["improvement_surcharge"],
                row["congestion_surcharge"],
                row["total_amount"],
                current_time
            )
            for _, row in df.iterrows()
        ]
        
        cursor.fast_executemany = True
        cursor.executemany(insert_query , data)

        connection.commit()

        rows_inserted = len(data)
        logger.info(f"Successfully inserted {rows_inserted} rows from {file_path} file.")

        return rows_inserted
    except pyodbc.Error as e:
        connection.rollback()
        logger.error(f"Error inserting Data {e}.")
        raise

    finally:
        connection.close()
