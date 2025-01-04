# LA Crime ETL Pipeline

### Overview

This application takes data from a csv file and transforms the data to be inserted into a postgresql database. The data for this project is located at [Data.gov](https://catalog.data.gov/dataset/crime-data-from-2020-to-present) which contains data about crimes in LA from 2020 to 2024. Here is also the [metadata](https://data.lacity.org/Public-Safety/Crime-Data-from-2020-to-Present/2nrs-mtv8/about_data) on the dataset. 

### Goals

1. The main goal was to get fimiliar with [Apache Spark](https://spark.apache.org/). Alot of companies are using spark to handle large amounts of data. One of the features implemented in this project is the use of partitioning for writing data to the database.

2. Have the entire application in [docker](https://www.docker.com/) containers. this makes it so any computer that has docker on it can run no matter the operating system.

3. Think about optimizations to the code. Some examples include using threading for inserting data into the database as it is a IObound task and using partioning previously mentioned

### Process

1. Create an **Entity Relationship Diagram** that shows how the tables will look in the database. The main thing here is to have the database be in 3NF

2. Create the database so that data can be stored in it

3. Download the data using **Python**

4. Clean the data for unwanted values and format the data for **Postgresql** using **Pyspark** which is a Spark wraper for Python

5. Split the data into different dataframes so that corresponding to a certain table in the database

6. Insert a temporary table in database for each table as its faster than appending

7. Merge any data points that were previously missing or had different values

8. Delete the temporary tables

### To Run

First you must have docker installed which can be done [here](https://www.docker.com/). 

then you must clone the repo which is done by using [git](https://git-scm.com/)

The command is `git clone https://github.com/Jprogram722/LA-Crime_ETL_Project.git`

You must then provide your own credentials in a `.env` file like so
```
USER=<your username>
PASSWORD=<your password>
DB=<your db>
PORT=<your port>
HOST="lacrime_postdb"
```

the `.env` file **MUST BE IN THE SAME DIRECTORY AS THE DOCKER COMPOSE FILE**

finally run `docker-compose up` and the program should run