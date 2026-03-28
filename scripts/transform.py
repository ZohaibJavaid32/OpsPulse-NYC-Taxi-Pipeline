import pandas as pd
import logging
import os
from logger import setup_logger


setup_logger()

logger = logging.getLogger("TRANSFORM MODULE")

def transform_data(input_path:str , output_dir: str = r"data\\transformed") -> pd.DataFrame:
    """
    Transform raw NYC Taxi data:
    - Handle missing values
    - Create features: trip duration, speed, pickup hour/day, fare per km
    - Filter invalid/outlier trips
    - Save transformed parquet file
    """

    logger.info(f"Reading Data from Raw File: {input_path}...")
    df = pd.read_parquet(input_path)

    # Handling Missing Values

    df = df.dropna(subset=[
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "payment_type"
    ])

    # Drop Duplicates
    df = df.drop_duplicates()
    
    # Fill Missing Values
    df["passenger_count"] = df["passenger_count"].fillna(1)
    df["RatecodeID"] = df["RatecodeID"].fillna(1)

    # Datetime Conversions
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    # Feature Engineering
    df["trip_duration_min"] = (df["tpep_dropoff_datetime"] - 
                           df["tpep_pickup_datetime"]).dt.total_seconds() / 60
    
    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    df["pickup_day"] = df["tpep_pickup_datetime"].dt.day_name()

    df["trip_speed_kmh"] = df["trip_distance"] / (df["trip_duration_min"] / 60)

    df["fare_per_km"] = df["fare_amount"] / df["trip_distance"]

    df = df[
    (df["trip_duration_min"] > 0) & 
    (df["trip_duration_min"] < 1440) &
    (df["trip_distance"] > 0)
    ]

    # Final Columns

    df = df[
    [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "pickup_hour",
        "pickup_day",
        "trip_distance",
        "trip_duration_min",
        "trip_speed_kmh",
        "fare_amount",
        "fare_per_km",
        "passenger_count",
        'extra',
        'mta_tax',
        'tip_amount',
        'tolls_amount', 
        'improvement_surcharge',
        'total_amount', 
        'congestion_surcharge'
        ]
    ]

    # Saved Transformed Data
    os.makedirs(output_dir , exist_ok=True)
    output_file = os.path.join(output_dir , os.path.basename(input_path))
    df.to_parquet(output_file , index=False)
    logger.info(f"Transformed File Saved to {output_file}.")

    return df

if __name__ == "__main__":
    RAW_FILE = "data/raw/yellow_tripdata_2024-08.parquet"

    df_transformed = transform_data(RAW_FILE)
    print(df_transformed.head())
