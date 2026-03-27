import pandas as pd
import os
import logging
import json
import seaborn as sns
import matplotlib.pyplot as plt
from logger import setup_logger

setup_logger()

logger = logging.getLogger("LOAD MODULE")

def load_insights(df : pd.DataFrame , output_dir: str = "data/insights"):
    """
    Compute key analytics and save them in:
    CSV
    JSON
    Plots
    """

    os.makedirs(output_dir , exist_ok=True)

    try:
        logger.info("Generating Insights...")
        trips_per_day = df["pickup_day"].value_counts().sort_index()

        trips_per_hour = df["pickup_hour"].value_counts().sort_index()

        avg_duration_per_hour = df.groupby('pickup_hour')["trip_duration_min"].mean()

        avg_distance_per_hour = df.groupby("pickup_hour")["trip_distance"].mean()

        high_speed_count = int((df["trip_speed_kmh"] > 110).sum())

        expensive_trip_count = int((df["total_amount"]  > 250).sum())


        insights = {
            "trips_per_day":trips_per_day.to_dict(),
            "trips_per_hour":trips_per_hour.to_dict(),
            "avg_duration_per_hour": avg_duration_per_hour.to_dict(),
            "avg_distance_per_hour":avg_distance_per_hour.to_dict(),
            "high_speed_count":high_speed_count,
            "expensive_trip_count":expensive_trip_count
        }

        with open(os.path.join(output_dir , "insights.json") , "w") as f:
            json.dump(insights , f , indent=4)
        
        logger.info(f"Insights Saved {output_dir}")
    except Exception as e:
         logger.error(f"Error while generating insights: {str(e)}" , exc_info=True)


if __name__ == "__main__":
        transformed_file = "data/transformed/yellow_tripdata_2024-08.parquet"
        df = pd.read_parquet(transformed_file)
        load_insights(df)
        print("Total rows:", len(df))
