from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat_ws
from pyspark.sql.types import StringType
import io

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Feature Comparison") \
    .getOrCreate()

# Function to read parquet files from S3
def read_parquet(path):
    return spark.read.parquet(path)

# Paths to your parquet files
original_path = "s3://your-bucket/original-features/"
new_path = "s3://your-bucket/new-features/"

# S3 path for the output CSV report
output_path = "s3://your-output-bucket/reports/feature_comparison_report.csv"

# Read both datasets
df_original = read_parquet(original_path)
df_new = read_parquet(new_path)

# Ensure both dataframes have the same columns
common_columns = sorted(set(df_original.columns) & set(df_new.columns))
df_original = df_original.select(*common_columns)
df_new = df_new.select(*common_columns)

# Function to compare dataframes
def compare_dataframes(df1, df2):
    # Combine dataframes
    df_combined = df1.alias("df1").join(df2.alias("df2"), on=df1.columns, how="full_outer")
    
    # Create comparison columns
    for col_name in df1.columns:
        df_combined = df_combined.withColumn(
            f"{col_name}_diff",
            when(col(f"df1.{col_name}") != col(f"df2.{col_name}"), lit(True)).otherwise(lit(False))
        )
    
    return df_combined

# Perform comparison
df_compared = compare_dataframes(df_original, df_new)

# Create a dataframe with differences
diff_columns = []
for col_name in common_columns:
    diff_columns.extend([
        col(f"df1.{col_name}").alias(f"{col_name}_original"),
        col(f"df2.{col_name}").alias(f"{col_name}_new"),
        col(f"{col_name}_diff").alias(f"{col_name}_is_different")
    ])

df_differences = df_compared.select(*diff_columns) \
    .filter(" OR ".join([f"{col}_is_different" for col in common_columns]))

# Add a column with all differences as a string
def create_diff_string(row):
    diffs = []
    for col in common_columns:
        if row[f"{col}_is_different"]:
            diffs.append(f"{col}: {row[f'{col}_original']} -> {row[f'{col}_new']}")
    return ", ".join(diffs)

create_diff_string_udf = spark.udf.register("create_diff_string", create_diff_string, StringType())

df_report = df_differences.withColumn("differences", create_diff_string_udf(struct(*df_differences.columns))) \
    .select("differences")

# Write the report to S3 as CSV
df_report.write.csv(output_path, header=True, mode="overwrite")

print(f"Comparison complete. Report saved to {output_path}")

# Stop Spark session
spark.stop()