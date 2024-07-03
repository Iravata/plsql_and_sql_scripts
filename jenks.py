from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType
import xgboost as xgb
import pickle
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("XGBoost Inference") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

# Read the input CSV file from S3
input_path = "s3://your-bucket/input-data.csv"
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Load the XGBoost model
with open("model.pkl", "rb") as model_file:
    xgb_model = pickle.load(model_file)

# Define the feature columns
feature_cols = ["feature1", "feature2", "feature3", ...]

# Define a pandas_udf for prediction
@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def predict_pandas_udf(*cols):
    # Combine input columns into a single DataFrame
    X = pd.concat(cols, axis=1)
    
    # Create DMatrix
    dmatrix = xgb.DMatrix(X)
    
    # Make predictions
    return pd.Series(xgb_model.predict(dmatrix))

# Apply the pandas_udf to perform predictions
result_df = df.select(*feature_cols).select(predict_pandas_udf(*feature_cols).alias("prediction"), "*")

# Write the results back to S3
output_path = "s3://your-bucket/output-data"
result_df.write.csv(output_path, header=True, mode="overwrite")

# Stop the Spark session
spark.stop()


---

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType
import pandas as pd

@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def predict_proba_pandas_udf(*cols):
    # Combine input columns into a single DataFrame
    X = pd.concat(cols, axis=1)
    
    # Make predictions directly on the DataFrame
    # Assuming xgb_model is a scikit-learn compatible XGBoost model
    probabilities = xgb_model.predict_proba(X)[:, 1]  # Probability of positive class
    
    return pd.Series(probabilities)

# Apply the pandas_udf to perform predictions
result_df = df.select(*feature_cols).select(predict_proba_pandas_udf(*feature_cols).alias("probability_positive_class"), "*")

spark = SparkSession.builder \
    .appName("XGBoost Inference") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
    .config("spark.hadoop.fs.s3a.multipart.threshold", "104857600") \
    .config("spark.sql.files.maxPartitionBytes", "134217728") \
    .config("spark.sql.files.openCostInBytes", "134217728") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    .config("spark.hadoop.fs.s3a.experimental.input.fadvise", "sequential") \
    .getOrCreate()