import os
import requests
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

# Spark init
spark = SparkSession.builder.appName("NOAA Daily Weather Ingestion").getOrCreate()

# NOAA API settings
NOAA_TOKEN = os.getenv("NOAA_TOKEN")  # export this in your environment 
STATION_ID = "GHCND:USW00014922"      # Minneapolis/St. Paul International
BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"

# Schema for the weather data
weather_schema = StructType([
    StructField("station", StringType(), True),
    StructField("datatype", StringType(), True),
    StructField("value", DoubleType(), True),   # always numeric
    StructField("date", StringType(), True)      # YYYY-MM-DD
])

# Bronze output path
bronze_path = "./bronze/weather/daily/"

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
        date=rec["date"][:10] # Extract YYYY-MM-DD
    ) for rec in results]
    return rows

def write_to_bronze(rows):
    if not rows:
        print("No weather data to write.")
        return
    
    df = spark.createDataFrame(rows, schema=weather_schema)
    df = df.withColumn("date", df["date"].cast("date"))
    df = df.withColumn("ingestion_time", current_timestamp())
    df.write.mode("append").partitionBy("date").parquet(bronze_path)
    print(f"Wrote {len(rows)} rows to {bronze_path}")


if __name__ == "__main__":
    print(f"token = {NOAA_TOKEN}")
    backfill_days = 3
    for i in range(backfill_days):
        date = (datetime.now() - timedelta(days=i+1)).strftime("%Y-%m-%d")
        print(f"Fetching NOAA data for {date}...")
        rows = fetch_noaa_daily(date)
        write_to_bronze(rows)
