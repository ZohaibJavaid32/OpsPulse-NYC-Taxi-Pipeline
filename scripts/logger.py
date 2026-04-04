import os 
import logging 
from scripts.config import LOG_DIR , LOG_FILE

def setup_logger():
    os.makedirs(LOG_DIR , exist_ok=True)

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler() #print logs on console.
        ]
    )