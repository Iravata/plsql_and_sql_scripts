from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract
import os

# Initialize Spark session
spark = SparkSession.builder.appName("Read and Process CSV").getOrCreate()

# Path where the CSV files are stored
raw_data_path = "/path/to/raw/data"

# Read all CSV files with the specific pattern
csv_files_path = f"{raw_data_path}/feat_final_*.csv"
df = spark.read.option("header", "true").csv(csv_files_path)

# Extract the date from the file name
df = df.withColumn("filename", input_file_name())
df = df.withColumn("prcsng_dt", regexp_extract("filename", r"feat_final_(\d{8})\.csv", 1))

# Drop the filename column as it is no longer needed
df = df.drop("filename")

# Save the data as a Delta table
df.write.format("delta").mode("overwrite").saveAsTable("ltp_features")
