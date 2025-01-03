import psycopg2
import os
from dotenv import load_dotenv
import logging
import time

def connect():

    load_dotenv()

    for _ in range(5):
        try:
            conn = psycopg2.connect(
                    database = os.getenv("DB"),
                    user = os.getenv("USER"),
                    password = os.getenv("PASSWORD"),
                    host = os.getenv("HOST"),
                    port = os.getenv("POST")
            )
            logging.info("Connected to DB")
            cursor = conn.cursor()
            return conn, cursor
        except:
            logging.info("Could Not Connect To DB. Retrying")
            time.sleep(5)

    return None

def create_tables() -> None:
    conn, cursor = connect()

    with open("LACrime_db.sql", "r") as file:
        contents = file.read()
        commands = contents.split(";")
        for command in commands:
            try:
                cursor.execute(command)
                conn.commit()
            except:
                conn.rollback()
                print("Somthing went wrong:", command)

    cursor.close()
    conn.close()

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

    # read all the commands in the sql file and split into a list of commands
    with open("./sql_scripts/merge_data.sql", "r") as sql:
        content = sql.read()
        commands = content.split(";")
        for command in commands:
            try:
                cursor.execute(command)
                conn.commit()
                print("Data has been merged")
            except:
                print("Something went wrong")
                conn.rollback()

    cursor.close()
    conn.close()

def drop_tmp_tables(tables: list[str]):
    conn, cursor = connect()

    # excute the stored procedure
    for table in tables:
        cursor.execute(f"DROP TABLE {table}_tmp")

    try:
        conn.commit()
        print("Tmp tables has been deleted")
    except:
        print("Something went wrong")
        conn.rollback()

    cursor.close()
    conn.close()

if __name__ == "__main__":
    ...

