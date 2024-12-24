import psycopg2
import os
from dotenv import load_dotenv

def truncated_tables() -> None:

    # load in stuff from .env file
    load_dotenv()

    tables = ["weapon", "area", "location", "status", "premisis", "crime", "report", "crime_report"]
    tables = tables[::-1]

    conn = psycopg2.connect(
            database = os.getenv("DB"),
            user = os.getenv("USER"),
            password = os.getenv("PASSWORD"),
            host = os.getenv("HOST"),
            port = os.getenv("POST")
    )

    if conn:
        print("connected")

    cursor = conn.cursor()
    if cursor:
        print("cursor created")

    for table in tables:
        cursor.execute(f"TRUNCATE TABLE {table} CASCADE")
    
    try:
        conn.commit()
        print("Tables have been truncated")
    except Exception as err:
        print(err)
        conn.rollback()

    conn.close()

if __name__ == "__main__":
    truncated_tables()

