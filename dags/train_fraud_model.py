from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import pandas as pd
import mlflow
import os
import sys

# === Add script directory to path for import ===
sys.path.append("/home/ec2-user/HIFP25/scripts")
from module_4_preprocessor_pandas import MedicarePreprocessorPandas

# === Import your MLflow training function ===
from module_5_ml_pipeline import train_and_log_model_with_mlflow  # Save the training code in this file

# === S3 Path and MLflow setup ===
S3_PATH = "s3://medicare-fraud-data-25-05-2025/merged_ready/train/*.csv"
MLFLOW_TRACKING_URI = "http://localhost:5000"  # Change if using remote MLflow

# === DAG Definition ===
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="train_medicare_fraud_model",
    default_args=default_args,
    description="Train and log Medicare fraud detection model",
    start_date=datetime(2025, 6, 24),
    
    catchup=False
) as dag:

    def preprocess_data(**kwargs):
        # Read raw data from S3
        df_raw = pd.read_csv(S3_PATH, storage_options={"anon": False})
        
        # Apply custom preprocessor
        preprocessor = MedicarePreprocessorPandas()
        df_processed = preprocessor.preprocess(df_raw)
        
        # Save for next step (optionally save to disk, or use XComs or global tmp location)
        df_processed.to_pickle("/tmp/train_processed.pkl")
        print("Preprocessing complete. Data saved to /tmp/train_processed.pkl")

    def train_model(**kwargs):
        df_processed = pd.read_pickle("/tmp/train_processed.pkl")
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        train_and_log_model_with_mlflow(df_processed, threshold=0.05)

    preprocess_task = PythonOperator(
        task_id="preprocess_data",
        python_callable=preprocess_data
    )

    train_task = PythonOperator(
        task_id="train_model",
        python_callable=train_model
    )

    preprocess_task >> train_task
