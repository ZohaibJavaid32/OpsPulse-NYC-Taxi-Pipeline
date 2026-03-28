import requests
import os
import sys
from config import BASE_URL , SAVE_DIR
import logging
from logger import setup_logger

setup_logger()

logger = logging.getLogger("EXTRACT MODULE")


def download_data(year: int , month: int):

    month = str(month).zfill(2)

    url = BASE_URL.format(year=year, month=month)
    save_path = os.path.join(SAVE_DIR , 
                            "yellow_tripdata_{year}-{month}.parquet"
                            .format(year=year , month=month))
    try:
        logger.info(f"Starting Download for {year}-{month}...")
        logger.debug(f"URL: {url}")

    # Create Directory for raw data if not exists.
        os.makedirs(SAVE_DIR , exist_ok=True)

        try:
            response = requests.get(url)
            response.raise_for_status()

            if response.status_code != 200:
                logger.error(f"Unexpected Status Code: {response.status_code}.")
        except requests.exceptions.HTTPError as http_err:
            status_code = http_err.response.status_code

            if status_code == 403:
                logger.error(f"Data not available for {year}-{month} (403 Forbidden).")
            elif status_code == 404:
                logger.error(f"File not Found for {year}-{month} (404).")
            else:
                logger.error(f"HTTP Error: {http_err}.")
            
            return None
    
    except Exception as e:
        logger.error(f"Download Failed: {e}")
        return None

    with open(save_path , "wb")as f:
            f.write(response.content)

    logger.info(f"Download Completed! Saved to path {save_path}.")
    return save_path
    
if __name__ == "__main__":
    try:
        year = int(input("Enter year (e.g. 2024): "))

        if year < 1000 or year > 9999:
            print("Invalid Year!")
            sys.exit()

        month = int(input("Enter Month (e.g. 12): "))
        if month < 1 or month > 12:
            print("Invalid Month!")
            sys.exit()
    except KeyboardInterrupt:
        print("Program Interrupted by User.")
    except ValueError:
        print("Please Enter Valid Numeric Input!")

    download_data(year , month)