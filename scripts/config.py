
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
SAVE_DIR = r"data\raw"

LOG_DIR = "logs"
LOG_FILE = "logs/opspulse.log"


# SQL Server Configuration
DB_CONFIG = {
    'server': 'YOUR SQL SERVER',  
    'database': 'NYCTaxiAnalytics',  
    'username': '',  
    'password': '', 
    'driver': '{ODBC Driver 17 for SQL Server}'
}

# Table Schema 
TABLE_NAME = 'taxi_trips'


