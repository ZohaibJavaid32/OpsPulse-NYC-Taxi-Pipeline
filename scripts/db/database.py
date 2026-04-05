from contextlib import contextmanager
import pyodbc
from scripts.config import DB_CONFIG , TABLE_NAME
from scripts.logger import setup_logger
from scripts.db.connection import get_sql_connection
from scripts.db.models import create_table
import logging
from datetime import datetime
import pandas as pd

setup_logger()

logger = logging.getLogger("DATA INSERTION MODULE")


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
        improvement_surcharge,congestion_surcharge,total_amount) 
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        
        logger.info(f"Reading Data from Parquet File")

        df = pd.read_parquet(file_path)

        # Handle NaN -> None (Important for SQL Server)
        df = df.where(pd.notnull(df) , None)
        
        logger.info(f"Preparing Data for insertion....")

        data = list(df.itertuples(index=False, name=None))

        batch_size = 10000
        cursor.fast_executemany = True
        for i in range(0 , len(data) , batch_size):
            batch = data[i:i+batch_size]
            cursor.executemany(insert_query , batch)
            connection.commit() # commit per batch
            logger.info(f"Inserted batch {i} to {i+len(batch)}")

        rows_inserted = len(data)
        logger.info(f"Successfully inserted {rows_inserted} rows from {file_path} file.")

        return rows_inserted
    except pyodbc.Error as e:
        connection.rollback()
        logger.error(f"Error inserting Data {e}.")
        raise


if __name__ == "__main__":
    print("Testing Database Connection...")

    try:
        with get_sql_connection() as conn:
            cursor = conn.cursor()

            insert_taxi_data(conn , "data\\transformed\yellow_tripdata_2024-08.parquet")
            conn.commit()
            
            #print("✅ Database Connection Successfull.")
    except Exception as e:
        print(f"Connection Failed {e}")
    