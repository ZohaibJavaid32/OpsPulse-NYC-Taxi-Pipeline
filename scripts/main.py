from extract import download_data
from transform import transform_data
from load import load_insights
from logger import setup_logger
import logging 

setup_logger()
logger = logging.getLogger("MAIN MODULE")


