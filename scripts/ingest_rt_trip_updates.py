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

# Ensure the script is running with the correct Python environment
print(sys.executable)
print("PYSPARK_PYTHON:", os.environ.get("PYSPARK_PYTHON"))

# Initialize Spark
spark = SparkSession.builder \
    .appName("Realtime GTFS Ingestion") \
    .config("spark.python.worker.faulthandler.enabled", "true") \
    .getOrCreate()

# GTFS_RT URL
GTFS_RT_URL = "https://svc.metrotransit.org/mtgtfs/tripupdates.pb"

# Output path for bronze data
bronze_path = "./bronze/realtime_gtfs/trip_updates"

# Logging setup
logging.basicConfig(
    filename="ingest_trip_updates_errors.log",
    level=logging.ERROR,
    format="%(asctime)s %(levelname)s %(message)s"
)

def fetch_trip_updates():
    response = requests.get(GTFS_RT_URL)
    if response.status_code != 200:
        print(f"Failed to fetch GTFS RT data: {response.status_code}")
        return []
        
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    trip_updates = []
    for entity in feed.entity:
        if entity.HasField("trip_update"):
            trip_update = entity.trip_update
            trip = trip_update.trip
            vehicle = trip_update.vehicle  # sometimes populated

            for stu in trip_update.stop_time_update:
                try:
                    trip_updates.append(Row(
                        trip_id = trip.trip_id,
                        route_id = trip.route_id,
                        start_time = trip.start_time,
                        start_date = trip.start_date,
                        schedule_relationship = trip.schedule_relationship,
                        stop_id = stu.stop_id if stu.HasField("stop_id") else None,
                        stop_sequence = stu.stop_sequence if stu.HasField("stop_sequence") else None,
                        arrival_time = stu.arrival.time if stu.HasField("arrival") and stu.arrival.HasField("time") else None,
                        departure_time = stu.departure.time if stu.HasField("departure") and stu.departure.HasField("time") else None,
                        vehicle_id = vehicle.id if vehicle.HasField("id") else None,
                    ))
                except Exception as e:
                    print(f"Failed to process stop_time_update: {e}")
    return trip_updates

def write_to_bronze(trip_updates):
    if not trip_updates:
        print("No trip updates to write.")
        return

    try:
        df = spark.createDataFrame(trip_updates)
        df = df.withColumn("ingestion_time", current_timestamp())

        print(df.toPandas().head())  # Debugging output

        # Coalesce to avoid small files and Spark EOF issues
        df.repartition(4).write.mode("append").parquet(bronze_path)

        print(f"Successfully wrote {len(trip_updates)} records to {bronze_path} at {datetime.now()}")

    except AnalysisException as ae:
        logging.error(f"Spark Analysis Error: {ae}")
        print(f"Spark Analysis Error: {ae}")
    except Exception as e:
        logging.error(f"Error writing to bronze: {e}")
        print(f"Error writing to bronze: {e}")

if __name__ == "__main__":
    while True:
        print("Fetching trip updates...")
        try:
            trip_updates = fetch_trip_updates()
            write_to_bronze(trip_updates)
        except Exception as e:
            logging.error(f"Top-level error during ingestion loop: {e}")
            print(f"Top-level error: {e}")

        print("Waiting for next fetch cycle...")
        time.sleep(60)
