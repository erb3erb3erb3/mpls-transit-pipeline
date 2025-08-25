import time
import os
import requests
import logging
import sys
from datetime import datetime
from google.transit import gtfs_realtime_pb2
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, col, explode
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
GTFS_RT_URL = "https://svc.metrotransit.org/mtgtfs/alerts.pb"

# Output path for bronze data
bronze_path = "./bronze/realtime_gtfs/alerts"

# Logging setup
logging.basicConfig(
    filename="ingest_vp_errors.log",
    level=logging.ERROR,
    format="%(asctime)s %(levelname)s %(message)s"
)

def fetch_alerts():
    try:
        response = requests.get(GTFS_RT_URL)
        if response.status_code != 200:
            print(f"Failed to fetch GTFS RT data: {response.status_code}")
            return []
        
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
    
    

        alerts = []
        for entity in feed.entity:
            print(f"Entity ID: {entity.id}")
            
            if entity.HasField("alert"):
                alert = entity.alert
                try:
                    # Find the latest active period to pass with the alert
                    now = int(time.time())
                    active_periods = [ap for ap in alert.active_period if ap.start <= now <= (ap.end if ap.end else now + 1)]
                    latest_active = max(active_periods, key=lambda ap: ap.start, default=None)

                    if latest_active:    
                        print(f"Latest Active Period: {latest_active.start} to {latest_active.end}")
                    else:
                        print("No active periods found.")
                    alerts.append(Row(
                        cause =alert.cause if alert.HasField("cause") else None,
                        effect = alert.effect if alert.HasField("effect") else None,
                        header = alert.header_text.translation[0].text if alert.header_text.translation else None,
                        description = alert.description_text.translation[0].text if alert.description_text.translation else None,
                        severity = alert.severity_level if alert.HasField("severity_level") else None,
                        start = latest_active.start if latest_active else None,
                        end = latest_active.end if latest_active else None,
                        informed_entities = [{"agency_id": ie.agency_id, "route_id": ie.route_id, "stop_id": ie.stop_id} for ie in alert.informed_entity] if alert.informed_entity else []
                    ))
                
                except Exception as e:
                    logging.error(f"Error processing alert entity: {e}")
                    print(f"Error processing alert entity: {e}")
                
        return alerts
    except Exception as e:
        logging.error(f"Error fetching or parsing GTFS RT data: {e}")
        return []

def write_to_bronze(alerts):
    if not alerts:
        print("No alerts to write.")
        return
    
    try:
        df = spark.createDataFrame(alerts)
        df = df.withColumn("ingestion_time", current_timestamp())

        # Explode informed_entities into separate rows if needed
        df = df.withColumn("informed_entity", explode(col("informed_entities")))

        # Flatten the informed_entity struct into separate columns
        df = df.withColumn("agency_id", col("informed_entity.agency_id")) \
               .withColumn("route_id", col("informed_entity.route_id")) \
               .withColumn("stop_id", col("informed_entity.stop_id")) \
               .drop("informed_entities", "informed_entity")

        # Debugging output
        print(df.toPandas().head())  

        df.write.mode("append").parquet(bronze_path)

        print(f"Successfully ingested {len(alerts)} alerts into {bronze_path} at {datetime.now()}")

    except AnalysisException as ae:
        logging.error(f"Spark Analysis Error: {ae}")
        print(f"Spark Analysis Error: {ae}")
    except Exception as e:
        logging.error(f"Error writing to bronze: {e}")
        print(f"Error writing to bronze: {e}")


if __name__ == "__main__":
    while True:
        print("Fetching alerts...")
        try:
            alerts = fetch_alerts()
            write_to_bronze(alerts)
        except Exception as e:
            logging.error(f"Top-level error during ingestion loop: {e}")
            print(f"Top-level error: {e}")  
        
        print("Waiting for next fetch cycle...")
        time.sleep(60)