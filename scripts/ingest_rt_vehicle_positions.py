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
GTFS_RT_URL = "https://svc.metrotransit.org/mtgtfs/VehiclePositions.pb"

# Output path for bronze data
bronze_path = "./bronze/realtime_gtfs/vehicle_positions"

# Logging setup
logging.basicConfig(
    filename="ingest_alerts_errors.log",
    level=logging.ERROR,
    format="%(asctime)s %(levelname)s %(message)s"
)

def fetch_vehicle_positions():
    try:
        response = requests.get(GTFS_RT_URL)
        if response.status_code != 200:
            print(f"Failed to fetch GTFS RT data: {response.status_code}")
            return []
        
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        vehicle_positions = []
        for entity in feed.entity:
            if entity.HasField("vehicle"):
                try:
                    vehicle = entity.vehicle
                    pos = vehicle.position
                    vehicle_positions.append(Row(
                        vehicle_id=str(vehicle.vehicle.id) if vehicle.vehicle.HasField("id") else None,
                        trip_id=str(vehicle.trip.trip_id) if vehicle.trip.HasField("trip_id") else None,
                        route_id=str(vehicle.trip.route_id) if vehicle.trip.HasField("route_id") else None,
                        latitude=str(pos.latitude) if pos.HasField("latitude") else None,
                        longitude=str(pos.longitude) if pos.HasField("longitude") else None,
                        bearing=str(pos.bearing) if pos.HasField("bearing") else None,
                        speed=str(pos.speed) if pos.HasField("speed") else None,
                        timestamp=str(vehicle.timestamp) if vehicle.HasField("timestamp") else None
                    ))
                except Exception as e:
                    logging.error(f"Error processing vehicle entity: {e}")
                    print(f"Error processing vehicle entity: {e}")

        return vehicle_positions

    except Exception as e:
        logging.error(f"Exception during GTFS fetch: {e}")
        return []

def write_to_bronze(vehicle_positions):
    if not vehicle_positions:
        print("No vehicle positions to write.")
        return

    try:
        df = spark.createDataFrame(vehicle_positions)
        df = df.withColumn("ingestion_time", current_timestamp())

        print(df.toPandas().head())  # Debugging output

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
