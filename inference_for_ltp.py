"""
This module provides functionality for performing XGBoost model inference on Spark DataFrame data.

The `main` function is the entry point for the script, which takes the following arguments:
- `input_path`: The path to the input Parquet file containing the data to be scored.
- `output_path`: The path to write the scored output data.
- `model_path`: The path to the serialized XGBoost model and threshold.
- `run_dt`: The date to use for the `PRCSNG_DT` column in the output.

The `load_model` function loads the serialized XGBoost model and threshold from the specified `model_path`.

The `get_decision_path` function returns a list of dictionaries defining the decision path bins used to categorize the predicted probabilities.

The `calc_group_bin` function takes a DataFrame with a `PREDICTED_PROBA` column and assigns a `BIN` value based on the decision path defined in `get_decision_path`.

The `predict_and_transform` function is a Pandas UDF that applies the loaded XGBoost model to the input data, calculates the predicted probabilities, and transforms the output into a DataFrame with the required columns.

The `main` function orchestrates the entire process, including loading the model, reading the input data, applying the prediction and transformation, and writing the output to a CSV file.
"""
import os
import pickle
from datetime import datetime
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType, StringType, StructType, StructField
import xgboost as xgb

# Initialize Spark session
spark = SparkSession.builder \
    .appName("XGBoost Inference") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

def load_model(model_path):
    print(f"Loading model using pkl file: {model_path}")
    try:
        with open(model_path, "rb") as file:
            model, threshold = pickle.load(file)
        return model
    except Exception as e:
        print(f"Error loading the pkl file: {e}")

def get_decision_path():
    return [
        {"LOW_P": 0.0, "HIGH_P": 0.100, "BIN": 1, "DECISION_PATH_FEATURE_LIST": []},
        {"LOW_P": 0.100, "HIGH_P": 0.150, "BIN": 2, "DECISION_PATH_FEATURE_LIST": []},
        {"LOW_P": 0.150, "HIGH_P": 0.260, "BIN": 3, "DECISION_PATH_FEATURE_LIST": []},
        {"LOW_P": 0.260, "HIGH_P": 0.400, "BIN": 4, "DECISION_PATH_FEATURE_LIST": []},
        {"LOW_P": 0.400, "HIGH_P": 0.550, "BIN": 5, "DECISION_PATH_FEATURE_LIST": []},
        {"LOW_P": 0.550, "HIGH_P": 0.660, "BIN": 6, "DECISION_PATH_FEATURE_LIST": []},
        {"LOW_P": 0.660, "HIGH_P": 0.750, "BIN": 7, "DECISION_PATH_FEATURE_LIST": []},
        {"LOW_P": 0.750, "HIGH_P": 0.830, "BIN": 8, "DECISION_PATH_FEATURE_LIST": []},
        {"LOW_P": 0.830, "HIGH_P": 0.910, "BIN": 9, "DECISION_PATH_FEATURE_LIST": []},
        {"LOW_P": 0.910, "HIGH_P": 1.0, "BIN": 10, "DECISION_PATH_FEATURE_LIST": []},
    ]

def calc_group_bin(df):
    dp_df = pd.DataFrame(get_decision_path())
    for i in range(len(dp_df)):
        df = df.withColumn(
            "BIN",
            F.when(
                (col("PREDICTED_PROBA") >= dp_df.loc[i, "LOW_P"]) & 
                (col("PREDICTED_PROBA") <= dp_df.loc[i, "HIGH_P"]),
                lit(i + 1)
            ).otherwise(col("BIN"))
        )
    return df

@pandas_udf(DoubleType())
def predict_proba_pandas_udf(*cols):
    X = pd.concat(cols, axis=1)
    if isinstance(broadcast_model.value, xgb.Booster):
        dmatrix = xgb.DMatrix(X)
        probabilities = broadcast_model.value.predict(dmatrix)
    else:
        # Assuming it's a scikit-learn compatible model
        probabilities = broadcast_model.value.predict_proba(X)[:, 1]  # Probability of positive class
    return pd.Series(probabilities)

def main(input_path, output_path, model_path, run_dt):
    # Load the XGBoost model
    xgb_model = load_model(model_path)
    
    # Read the input parquet file from S3
    df = spark.read.parquet(input_path)
    
    # Get feature names from the model
    feature_cols = xgb_model.get_booster().feature_names
    
    # Create a closure that captures xgb_model and run_dt
    def predict_and_transform_wrapper(*cols):
        return predict_and_transform(xgb_model, run_dt)(*cols)
    
    # Apply the pandas_udf to perform predictions and transformations
    result_df = df.select(*feature_cols).withColumn(
        "PREDICTED_PROBA", predict_proba_pandas_udf(*[F.col(c) for c in feature_cols])
    )
    
    # Calculate the BIN and other columns
    result_df = result_df.withColumn("BIN", calc_group_bin(F.col("PREDICTED_PROBA")))
    result_df = result_df.withColumn("PRCSNG_DT", F.lit(run_dt))
    result_df = result_df.withColumn("SCNRO_ID", F.lit('71621'))
    result_df = result_df.withColumn("CUST_ID", F.lit(''))  # Adjust as needed
    result_df = result_df.withColumn("AA_NODE_PATH_ATTR_CHAR", F.lit(''))  # Adjust as needed
    result_df = result_df.withColumn("CLASS_ID", F.lit(''))  # Adjust as needed
    result_df = result_df.withColumn("DECISION_PATH_FEATURE_LIST", F.lit(''))  # Adjust as needed
    
    # Write the results back to S3
    result_df.write.csv(output_path, header=True, mode="overwrite", sep="~")
    
    # Stop the Spark session
    spark.stop()

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) != 5:
        print("Usage: python ltp_inference.py <input_path> <output_path> <model_path> <run_dt>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    model_path = sys.argv[3]
    run_dt = sys.argv[4]
    
    main(input_path, output_path, model_path, run_dt)

----------------------

@pandas_udf(StructType([
    StructField("CUST_ID", StringType()),
    StructField("AA_NODE_PATH_ATTR_CHAR", StringType()),
    StructField("PREDICTED_PROBA", DoubleType()),
    StructField("BIN", DoubleType()),
    StructField("CLASS_ID", StringType()),
    StructField("PRCSNG_DT", StringType()),
    StructField("SCNRO_ID", StringType()),
    StructField("DECISION_PATH_FEATURE_LIST", StringType())
]))
def predict_and_transform(*cols):
    model = broadcast_model.value
    X = pd.concat(cols, axis=1)
    if isinstance(model, xgb.Booster):
        dmatrix = xgb.DMatrix(X)
        probabilities = model.predict(dmatrix)
    else:
        # Assuming it's a scikit-learn compatible model
        probabilities = model.predict_proba(X)[:, 1]  # Probability of positive class
    
    prediction_df = pd.DataFrame({
        "CUST_ID": X.index,
        "AA_NODE_PATH_ATTR_CHAR": '',
        "PREDICTED_PROBA": probabilities,
        "BIN": 1,
        "CLASS_ID": '',
        "PRCSNG_DT": '',
        "SCNRO_ID": '71621',
        "DECISION_PATH_FEATURE_LIST": ''
    })
    
    dp_df = pd.DataFrame(get_decision_path())
    for i in range(len(dp_df)):
        prediction_df.loc[
            (prediction_df["PREDICTED_PROBA"] >= dp_df.loc[i, "LOW_P"]) & 
            (prediction_df["PREDICTED_PROBA"] <= dp_df.loc[i, "HIGH_P"]), 
            "BIN"
        ] = i + 1
    
    prediction_df['PRCSNG_DT'] = datetime.strptime(run_dt, '%Y%m%d').strftime('%d-%b-%Y')
    
    return prediction_df


    # Get feature names from the model
    feature_cols = xgb_model.get_booster().feature_names
    
    # Apply the pandas_udf to perform predictions and transformations
    result_df = df.select(*feature_cols).select(
        predict_and_transform(*[col(c) for c in feature_cols]).alias("prediction")
    ).select("prediction.*")


    --------

        # Derive class_id based on BIN value
    df = df.withColumn(
        "CLASS_ID",
        F.when(F.col("BIN").isNotNull(), F.concat(F.lit("C"), F.col("BIN").cast("string")))
    )