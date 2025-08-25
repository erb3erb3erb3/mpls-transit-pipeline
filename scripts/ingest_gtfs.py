import os
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("GTFS Ingestion") \
    .getOrCreate() 

# GTFS File Names
gtfs_files = [
    "agency",
    "stops",
    "routes",
    "trips",
    "stop_times",
    "calendar",
    "calendar_dates"]

# Path to GTFS data
gtfs_path = "./data/gtfs"
bronze_path = "./bronze"

# Loop through each GTFS folder 
for folder in sorted(os.listdir(gtfs_path)):
    folder_path = os.path.join(gtfs_path, folder)

    # Check if the folder is a directory and starts with "GTFS"
    if not folder.startswith("GTFS") or not os.path.isdir(folder_path):
        continue

    # Extract partition date from folder name
    partition_date = folder.replace("GTFS", "")

    print(f"Ingesting GTFS data from {folder_path} for date {partition_date}")

    for file_name in gtfs_files:
        file_path = os.path.join(folder_path, f"{file_name}.txt")

        # Check if the file exists
        if not os.path.isfile(file_path):
            print(f"File {file_path} does not exist, skipping.")
            continue
        
        # Read the GTFS file into a DataFrame
        df = spark.read.csv(file_path, header=True)

        # Add partition date column
        df = df.withColumn("partition_date", lit(partition_date))   


        # Write the DataFrame to the bronze path with partitioning
        output_path = os.path.join(bronze_path, file_name)
        df.write.mode("append").parquet(output_path, partitionBy=["partition_date"])

        print(f"Successfully ingested {file_name} data for date {partition_date} into {output_path}")
    print(f"Completed ingestion for GTFS data from {folder_path}")


