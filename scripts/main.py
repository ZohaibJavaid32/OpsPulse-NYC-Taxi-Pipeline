from scripts.extract import download_data
from scripts.transform import transform_data
from scripts.load import load_insights
from scripts.db.connection import get_sql_connection
from scripts.db.database import insert_taxi_data
import sys
import matplotlib
from datetime import datetime
from scripts.logger import setup_logger
import logging 


setup_logger()
logger = logging.getLogger("MAIN MODULE")

def take_input():
    try:
        year = int(input("Enter year (e.g. 2024): "))
        month = int(input("Enter Month (e.g. 12): "))

        logging.info("Starting ETL Pipeline...")
        
        curr_year = datetime.now().year

        if year < 1000 or year > 9999:
            logger.error(f"Invalid Year: {year}")
            sys.exit()
        if year > curr_year:
            logger.error(f"Year Cannot be Greater than Current Year. {year}")
            sys.exit()

        if month < 1 or month > 12:
            logger.error("Invalid Month!")
            sys.exit()

        return (year , month)
    except KeyboardInterrupt:
        print("Program Interrupted by User.")
        return None
    except ValueError:
        print("Please Enter Valid Numeric Input!")
        return None

    
if __name__ == "__main__":

    result = take_input()
    if result is None:
        sys.exit()
    year, month = result

    logging.info("Extracting Data...")

    raw_file = download_data(year , month)
    if raw_file == None:
        logger.info("Extraction Failed. Pipeline Stopping...")
        sys.exit(1)
    logging.info("Transforming Data...")
    transformed_file_path = transform_data(raw_file)

    try:
        with get_sql_connection() as conn:
            cursor = conn.cursor

            insert_taxi_data(conn , transformed_file_path)
            conn.commit()
    except Exception as e:
        logger.error(f"DB Connection Failed. {e}")
    
