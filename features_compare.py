from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CompareParquetFiles") \
    .getOrCreate()

# S3 paths to the original and new parquet files
original_path = "s3://your-bucket/original_features/"
new_path = "s3://your-bucket/new_features/"

# Read parquet files into DataFrames
original_df = spark.read.parquet(original_path)
new_df = spark.read.parquet(new_path)

# Join the DataFrames on their common keys (assuming 'id' is the common key)
joined_df = original_df.alias('original').join(new_df.alias('new'), on='id', how='outer')

# Select columns to compare (excluding the key column 'id')
columns_to_compare = [col for col in original_df.columns if col != 'id']

# Initialize an empty DataFrame to collect differences
diffs = []

# Compare each column and collect differences
for col_name in columns_to_compare:
    diff_df = joined_df.withColumn(
        f"{col_name}_diff",
        when(col(f"original.{col_name}") != col(f"new.{col_name}"), lit("DIFFERENT")).otherwise(lit("SAME"))
    ).select('id', f"original.{col_name}", f"new.{col_name}", f"{col_name}_diff")
    
    # Filter rows where the values are different
    diff_df_filtered = diff_df.filter(col(f"{col_name}_diff") == "DIFFERENT")
    
    # Convert to Pandas DataFrame and append to diffs list
    diffs.append(diff_df_filtered.toPandas())

# Concatenate all differences into a single DataFrame
if diffs:
    final_diff_df = pd.concat(diffs, axis=0)
else:
    final_diff_df = pd.DataFrame(columns=['id', 'original_value', 'new_value', 'difference'])

# Save differences to an HTML file
html_output_path = "/path/to/save/differences.html"
final_diff_df.to_html(html_output_path, index=False)

# Stop the Spark session
spark.stop()

print(f"Differences logged to {html_output_path}")
