import time
import os
import requests
import logging
import sys
from datetime import datetime
from google.transit import gtfs_realtime_pb2
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp
from pyspark.sql.utils import AnalysisException

# Initialize Spark
spark = (
    SparkSession.builder
    .appName("RT GTFS Vehicle Positions")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
     .config("spark.hadoop.fs.s3a.aws.credentials.provider", "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
    .getOrCreate()
)

# GTFS_RT URL
GTFS_RT_URL = "https://svc.metrotransit.org/mtgtfs/VehiclePositions.pb"

# Output path for bronze S3 bucket
bronze_path = "s3a://minneapolis-transit-lake/bronze/realtime_gtfs/vehicle_positions"

# Logging setup
logging.basicConfig(
    filename="ingest_vehicle_position_errors.log",
    level=logging.ERROR,
    format="%(asctime)s %(levelname)s %(message)s"
)

# Function to fetch vehicle position data from GTFS. Returns data as an array of Rows
def fetch_vehicle_positions():
    try:
        response = requests.get(GTFS_RT_URL)
        if response.status_code != 200:
            print(f"Failed to fetch GTFS RT data: {response.status_code}")
            return []

        # Using gtfs_realtime_pb2 library to read message contents
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        vehicle_positions = []
        for entity in feed.entity:
            if entity.HasField("vehicle"):
                try:
                    vehicle = entity.vehicle
                    pos = vehicle.position

                    # Casting values to string to avoid Spark merge issue due to inconsistant data type inference from Python Row object
                    vehicle_positions.append(Row(
                        vehicle_id = str(vehicle.vehicle.id) if vehicle.vehicle.HasField("id") else None,
                        trip_id = str(vehicle.trip.trip_id) if vehicle.trip.HasField("trip_id") else None,
                        route_id = str(vehicle.trip.route_id) if vehicle.trip.HasField("route_id") else None,
                        latitude = str(pos.latitude) if pos.HasField("latitude") else None,
                        longitude = str(pos.longitude) if pos.HasField("longitude") else None,
                        bearing = str(pos.bearing) if pos.HasField("bearing") else None,
                        speed = str(pos.speed) if pos.HasField("speed") else None,
                        timestamp = str(vehicle.timestamp) if vehicle.HasField("timestamp") else None
                    ))
                except Exception as e:
                    logging.error(f"Error processing vehicle entity: {e}")
                    print(f"Error processing vehicle entity: {e}")

        return vehicle_positions

    except Exception as e:
        logging.error(f"Exception during GTFS fetch: {e}")
        return []

# Function to write vehicle positions to bronze S3 bucket. Takes an array of Rows as argument.
def write_to_bronze(vehicle_positions):
    if not vehicle_positions:
        print("No vehicle positions to write.")
        return

    try:
        df = spark.createDataFrame(vehicle_positions)
        df = df.withColumn("ingestion_time", current_timestamp())

        # Coalesce to avoid small files and Spark EOF issues
        df.coalesce(1).write.mode("append").parquet(bronze_path)

        print(f"Successfully wrote {len(vehicle_positions)} records to {bronze_path} at {datetime.now()}")

    except AnalysisException as ae:
        logging.error(f"Spark Analysis Error: {ae}")
        print(f"Spark Analysis Error: {ae}")
    except Exception as e:
        logging.error(f"Error writing to bronze: {e}")
        print(f"Error writing to bronze: {e}")


if __name__ == "__main__":
    while True:
        print("Fetching vehicle positions...")
        try:
            vehicle_positions = fetch_vehicle_positions()
            write_to_bronze(vehicle_positions)
        except Exception as e:
            logging.error(f"Top-level error during ingestion loop: {e}")
            print(f"Top-level error: {e}")

        print("Waiting for next fetch cycle...")
        time.sleep(60)


