from extract import download_data
from transform import transform_data
from load import load_insights
import sys
import matplotlib
from logger import setup_logger
import logging 

# matplotlib.rcParams.update({'verbose.level': 'silent'})

setup_logger()
logger = logging.getLogger("MAIN MODULE")


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

    logging.info("Extracting Data...")

    raw_file = download_data(year , month)
    
    logging.info("Transforming Data...")
    transformed_df = transform_data(raw_file)

    logging.info("Generating and Loading Insights...")
    load_insights(transformed_df)
