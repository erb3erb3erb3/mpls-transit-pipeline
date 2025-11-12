import os
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Weather Ingestion").getOrCreate() 

# Path to Weather data
weather_path = "./data/weather"
bronze_path = "./bronze"

# Loop through each weather file in the directory
for file in sorted(os.listdir(weather_path)):
    
    # Check if the file is a CSV file
    if not file.endswith(".csv"):
        print(f"File {file} is not a CSV, skipping.")
        continue

    # Extract partition date from folder name
    partition_year = file.split("-")[0]
    file_path = os.path.join(weather_path, file)
    
    print(f"Ingesting weather data from {file_path} for year {partition_year}")
        
    # Read the weather file into a DataFrame
    df = spark.read.csv(file_path, header=True)

    # Add partition date column
    df = df.withColumn("partition_year", lit(partition_year))   


    # Write the DataFrame to the bronze path with partitioning
    output_path = os.path.join(bronze_path, "weather")
    df.write.mode("append").parquet(output_path, partitionBy=["partition_year"])

    print(f"Successfully ingested {file} data for date {partition_year} into {output_path}")

