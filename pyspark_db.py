"""
This module contains functions that will allow pyspark to interact with the database:
This includes inserting data into the database and fetching data
"""

def insert_data(df, url: str, table: str, properties: dict[str]) -> None:
    """
    This function will insert data into the corrisponding table
    """
    df.repartition(8) \
    .write \
    .mode("append") \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", table) \
    .option("user", properties["user"]) \
    .option("password", properties["password"]) \
    .option("driver", properties["driver"]) \
    .save()

    print(f"{table} data has been inserted into database")


def get_db_data(spark, url: str, table: str, properties: dict[str]):
    """
    This function will read data from a table in the database
    """
    df = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", table) \
    .option("user", properties["user"]) \
    .option("password", properties["password"]) \
    .option("driver", properties["driver"]) \
    .load()

    return df