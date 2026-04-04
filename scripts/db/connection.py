from contextlib import contextmanager
import pyodbc
from scripts.config import DB_CONFIG , TABLE_NAME
from scripts.logger import setup_logger
import logging

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


if __name__ == "__main__":

    try:
        with get_sql_connection() as conn:
            cursor =  conn.cursor()
    except Exception as e:
        print(f"DB Connection Failed. {e}")