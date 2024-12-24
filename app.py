from urllib import request
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as sf
from pyspark.conf import SparkConf
import time

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

def prepare_dataframe(df, **cols):
    if "new_id" in cols.keys():
        df_mod = df.select(
            sf.col(cols["old_id"]).alias(cols["new_id"]),
            sf.col(cols["old_val"]).alias(cols["new_val"])
        ) \
        .na.drop(subset=cols["new_id"]) \
        .dropDuplicates() \
        .sort(sf.asc(cols["new_id"]))
    else:
        df_mod = df.select(
            sf.col(cols["id"]),
            sf.col(cols["val"])
        ) \
        .na.drop(subset=cols["val"]) \
        .dropDuplicates() \
        .sort(sf.asc(cols["id"]))

    return df_mod

def main() -> None:

    start_time = time.time()

    load_dotenv()

    PROPERTIES = {
        "user": os.getenv('USER'),
        "password": os.getenv('PASSWORD'),
        "driver": "org.postgresql.Driver"
    }

    DB_URL = os.getenv('URL')

    folder = "data"
    file_name = "LACrimeData.csv"
    WEB_URL = "https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD"


    ################## PART 1: download the data ####################

    if not os.path.isdir(folder):
        os.mkdir(folder)
        print(f"{folder} has been created")
    else:
        print(f"{folder} already exists")

    # download the data
    if not os.path.isfile(f"./{folder}/{file_name}"):
        print("Saving file.")
        request.urlretrieve(WEB_URL, f"./{folder}/{file_name}")
    else:
        print("File already exists")

    ################## PART 2: Cleanse the data with pyspark ####################

    conf = SparkConf()
    # specify the java postgres driver to spark
    conf.set("spark.driver.extraClassPath", "./drivers/postgresql-42.7.2.jar")
    # this is needed to print the timestamps in the dataframe in pyspark
    conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    
    spark = SparkSession.builder.appName("Data Wrangling").config(conf=conf).getOrCreate()

    df = spark.read.csv("./data/LACrimeData.csv", header=True, inferSchema=True)

    df = df.withColumn(
        "TIME OCC", 
        sf.col("TIME OCC").cast("string")
    ) \
    .withColumn(
        "TIME OCC", 
        sf.concat(
            sf.substring("TIME OCC", 0, 2),
            sf.lit(":"),
            sf.substring("TIME OCC", 3, 2),
        ),
    ) \
    .withColumn(
    "TIME OCC",
    sf.to_timestamp("TIME OCC", "HH:mm")
    ) \
    .withColumn(
        "TIME OCC",
        sf.col("TIME OCC").cast("string")
    ).na.fill(value="1970-01-01 00:00:00", subset="TIME OCC") \
    .withColumn(
        "Date Rptd",
        sf.substring("Date Rptd", 0, 10)
    ) \
    .withColumn(
        "DATE AND TIME OCC",
        sf.concat(
            sf.substring("DATE OCC", 0, 10),
            sf.substring("TIME OCC", 11, 9)
        )
    ) \
    .drop("DATE OCC", "TIME OCC") \
    .na.fill(value="X", subset="Vict Sex").na.fill(value="X", subset="Vict Descent") \
    .withColumn(
        "location_name",
        sf.regexp_replace(
            sf.regexp_replace(df.LOCATION, r'\s+', " "),
            r'PLACE',
            "PL"
        ),
    ) \
    .orderBy(sf.asc("location_name")) \
    .drop("LOCATION")

    # need to give rank
    # source: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rank.html
    w = Window.orderBy(df["location_name"])
    # add ranks to each value in location_name column which will be there primary key
    df = df.withColumn("location_id_pk", sf.rank().over(w))

    print("Clensing is done")

    ############################ PART 3 insert data into tables ##############################

    df_dict = {}

    # weapon data

    df_dict["weapon"] = prepare_dataframe(df, 
        old_id="Weapon Used Cd",
        old_val="Weapon Desc",
        new_id="weapon_id_pk",
        new_val="weapon")

    # area data

    df_dict["area"] = prepare_dataframe(df, 
        old_id="AREA",
        old_val="AREA NAME",
        new_id="area_id_pk",
        new_val="area_name")

    # status data

    df_dict["status"] = prepare_dataframe(df, 
        old_id="Status",
        old_val="Status Desc",
        new_id="status_code",
        new_val="status_desc")

    # Premisis Data

    df_dict["premisis"] = prepare_dataframe(df, 
        old_id="Premis Cd",
        old_val="Premis Desc",
        new_id="premisis_id_pk",
        new_val="premisis_desc")

    # Crime Data

    df_dict["crime"] = prepare_dataframe(df, 
        old_id="Crm Cd",
        old_val="Crm Cd Desc",
        new_id="crime_id_pk",
        new_val="crime_desc")

    # Location Data

    df_dict["location"] = prepare_dataframe(df, 
        id="location_id_pk",
        val="location_name"
        )

    # Report Data

    df_dict["report"] = df.select(
        sf.col("DR_NO").alias("report_id_pk"),
        sf.to_date("Date Rptd", "MM/dd/yyyy").alias("date_reported"),
        sf.to_timestamp("DATE AND TIME OCC", "MM/dd/yyyy HH:mm:ss").alias("date_occured"),
        sf.col("Vict Age").alias("victim_age"),
        sf.col("Vict Sex").alias("victim_sex"),
        sf.col("Vict Descent").alias("victim_decent"),
        sf.col("AREA").alias("area_id_fk"),
        sf.col("location_id_pk").alias("location_id_fk"),
        sf.col("Premis Cd").alias("premisis_id_fk"),
        sf.col("Weapon Used Cd").alias("weapon_id_fk"),
        sf.col("Status").alias("status_code"),
        sf.col("LAT").alias("latitude"),
        sf.col("LON").alias("longitude"),
    ) \
    .filter(df["DR_NO"].isNotNull()) \
    .dropDuplicates(subset=["report_id_pk"])

    # Crime Report Bridge Table Data

    df_dict["crime_report"] = df.dropDuplicates(subset=["DR_NO"]) \
    .withColumn('crime_id_fk', sf.explode(sf.array("Crm Cd 1", "Crm Cd 2", "Crm Cd 3", "Crm Cd 4"))) \
    .select(
        sf.col("DR_NO").alias("report_id_fk"),
        sf.col("crime_id_fk")
    ) \
    .na.drop(subset=["crime_id_fk"])

    df_dict["crime_report"] = df_dict["crime_report"].join(
        df_dict["crime"], df_dict["crime_report"]["crime_id_fk"] == df_dict["crime"]["crime_id_pk"], "left"
    ) \
    .filter("crime_id_pk IS NOT NULL") \
    .drop("crime_id_pk", "crime_desc")

    for key in df_dict.keys():
        insert_data(df_dict[key], DB_URL, key, PROPERTIES)
        print(f"{key} data inserted into database")

    spark.stop()

    program_time = time.time() - start_time

    print("\nCOMPLETE!!!!!!")

    print(f"Completion Time: {program_time:.2f} seconds")
    
if __name__ == "__main__":
    main()