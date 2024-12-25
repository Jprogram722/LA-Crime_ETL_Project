import psycopg2
import os
from dotenv import load_dotenv

def connect():
    load_dotenv()
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

    return conn, cursor

def truncated_tables() -> None:

    tables = ["weapon", "area", "location", "status", "premisis", "crime", "report", "crime_report"]
    tables = tables[::-1]

    conn, cursor = connect()

    for table in tables:
        cursor.execute(f"TRUNCATE TABLE {table} CASCADE")
    
    try:
        conn.commit()
        print("Tables have been truncated")
    except Exception as err:
        print(err)
        conn.rollback()

    cursor.close()
    conn.close()


def merge():
    conn, cursor = connect()

    # excute the stored procedure
    cursor.execute("CALL merge_data()")

    try:
        conn.commit()
        print("Data has been merged")
    except:
        print("Something went wrong")
        conn.rollback()

    cursor.close()
    conn.close()

if __name__ == "__main__":
    ...

