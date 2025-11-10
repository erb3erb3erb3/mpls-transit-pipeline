import os
import requests
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

# Spark init
# Config for S3 connection
spark = (
    SparkSession.builder
    .appName("NOAA Daily Weather Ingestion")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
     .config("spark.hadoop.fs.s3a.aws.credentials.provider", "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
    .getOrCreate()
)

# NOAA API settings: Station ID is for Daily Minneapolis/St.Paul
NOAA_TOKEN = os.getenv("NOAA_TOKEN")  
STATION_ID = "GHCND:USW00014922"      
BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"

# Schema for the weather data
weather_schema = StructType([
    StructField("station", StringType(), True),
    StructField("datatype", StringType(), True),
    StructField("value", DoubleType(), True),   
    StructField("date", StringType(), True)      
])

# Bronze output path
bronze_path = "s3a://minneapolis-transit-lake/bronze/noaa_daily_weather"

# Function to fetch Weather Data for a given date
def fetch_noaa_daily(date):
    headers = {"token": NOAA_TOKEN}
    params = {
        "datasetid": "GHCND",
        "stations": STATION_ID,
        "startdate": date,
        "enddate": date,
        "units": "standard",
        "limit": 1000
    }
    r = requests.get(BASE_URL, headers=headers, params=params)
    if r.status_code != 200:
        logging.error(f"Failed to fetch NOAA data: {r.status_code}, {r.text}")
        return []
    
    results = r.json().get("results", [])
    rows = [Row(
        station=rec["station"],
        datatype=rec["datatype"],
        value=float(rec["value"]), # Ensure value is float to avoid merge issue in Spark
        date=rec["date"][:10] # Extract YYYY-MM-DD from date timestamp
    ) for rec in results]
    return rows

# Function to check if Partion exists for a given date

# !!! NEED TO REWRITE TO CHECK PARTITION ON S3 !!!
# def partition_exists(date):
#     partition_folder = os.path.join(bronze_path, f"date={date}")
#     return os.path.exists(partition_folder)


# Function to write data to S3 bronze layer
def write_to_bronze(rows):
    if not rows:
        print("No weather data to write.")
        return
    
    # Check if partition for the date already exists, skips write if it does.
    # !!! NEED TO REWRITE partition_exists FUNC TO CHECKS S3 !!!
    ### partition_date = rows[0].date if rows else None
    ### if partition_exists(partition_date):
    ###     print(f"Partition for date {partition_date} already exists. Skipping write.")
    ###     return 

    # Create spark Data Frame and write to S3 
    df = spark.createDataFrame(rows, schema=weather_schema)
    df = df.withColumn("date", df["date"].cast("date"))
    df = df.withColumn("ingestion_time", current_timestamp())
    df.write.mode("append").partitionBy("date").parquet(bronze_path)
    print(f"Wrote {len(rows)} rows to {bronze_path}")


if __name__ == "__main__":

    # Write daily going back as far as the set number of backfill days.
    backfill_days = 5
    for i in range(backfill_days):
        date = (datetime.now() - timedelta(days=i+1)).strftime("%Y-%m-%d")
        print(f"Fetching NOAA data for {date}...")
        rows = fetch_noaa_daily(date)
        write_to_bronze(rows)
