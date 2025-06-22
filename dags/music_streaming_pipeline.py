# ======================================================================================
# music_streaming_pipeline.py
# This file defines an Airflow DAG for an ETL pipeline for music streaming data.
# ======================================================================================
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import urllib.parse

from src.config import get_config
from src.logging_config import setup_logger, add_s3_handler
from src.cleaning import clean_users_data, clean_songs_data
from src.extract_batch_streams import extract_batch_streams
from src.validate_data import validate_csv_users_data, validate_csv_songs_data, validate_csv_streaming_data
from src.load import create_dwh_tables, load_users_to_dwh, load_songs_to_dwh, load_streaming_to_dwh
from src.kpi_computation import compute_and_save_kpis

# Set up logger for the Airflow DAG file
log = setup_logger('music_streaming_pipeline', mode='a')

# Load configuration and add S3 handler for logging
try:
    config = get_config()
    add_s3_handler(log, config)
except Exception as e:
    log.error("Failed to load configuration or set up S3 logging. DAG will not be operational.", exc_info=True)
    raise

# Retrieve configuration values
DATA_RAW_DIR = config.get('DATA_RAW_DIR')
DATA_PROCESSED_DIR = config.get('DATA_PROCESSED_DIR')
RDS_SOURCE_DIR = config.get('RDS_SOURCE_DIR')
BATCH_SOURCE_DIR = config.get('BATCH_SOURCE_DIR')
RDS_PROCESSED_DIR = config.get('RDS_PROCESSED_DIR')
BATCH_PROCESSED_DIR = config.get('BATCH_PROCESSED_DIR')
DATA_KPI_OUTPUTS_DIR = config.get('DATA_KPI_OUTPUTS_DIR')
DWH_CONN_STRING = config.get('DWH_CONN_STRING')
REDSHIFT_IAM_ROLE = config.get('REDSHIFT_IAM_ROLE')

# Extract bucket name from a given S3 URI
def get_bucket_from_s3_uri(s3_uri):
    """
    Extracts the bucket name from an S3 URI.
    Args:
        s3_uri (str): The S3 URI (e.g., "s3://my-bucket/path/").
    Returns:
        str: The bucket name.
    Raises:
        ValueError: If the S3 URI format is invalid.
    """
    parsed = urllib.parse.urlparse(s3_uri)
    if not parsed.netloc:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    return parsed.netloc

# Determine bucket names for source and processed data
raw_bucket_name = get_bucket_from_s3_uri(DATA_RAW_DIR)
processed_bucket_name = get_bucket_from_s3_uri(DATA_PROCESSED_DIR)

# Define full paths for raw and processed data files/directories
RAW_USERS_CSV_PATH = f"{RDS_SOURCE_DIR.split('//')[1].split('/', 1)[1]}/users.csv"
RAW_SONGS_CSV_PATH = f"{RDS_SOURCE_DIR.split('//')[1].split('/', 1)[1]}/songs.csv"
RAW_STREAMING_DIR_PATH = f"{BATCH_SOURCE_DIR.split('//')[1].split('/', 1)[1]}/"

PROCESSED_USERS_CSV_PATH = f"{RDS_PROCESSED_DIR.split('//')[1].split('/', 1)[1]}/users.csv"
PROCESSED_USERS_PARQUET_PATH = PROCESSED_USERS_CSV_PATH.replace('.csv', '.parquet')

PROCESSED_SONGS_CSV_PATH = f"{RDS_PROCESSED_DIR.split('//')[1].split('/', 1)[1]}/songs.csv"
PROCESSED_SONGS_PARQUET_PATH = PROCESSED_SONGS_CSV_PATH.replace('.csv', '.parquet')

PROCESSED_STREAMING_PARQUET_PATH = f"{BATCH_PROCESSED_DIR.split('//')[1].split('/', 1)[1]}/streaming_combined_{{{{ ds_nodash }}}}.parquet"

# Default arguments for the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

# Define the Airflow DAG
with DAG(
    dag_id='music_streaming_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for music streaming data',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['music', 'etl', 'redshift', 's3', 'kpi'],
) as dag:

    # Task: Extract and Clean Users Data
    extract_and_clean_users = PythonOperator(
        task_id='extract_and_clean_users',
        python_callable=clean_users_data,
        op_kwargs={
            'input_path': f"s3://{raw_bucket_name}/{RAW_USERS_CSV_PATH}",
            'output_path': f"s3://{processed_bucket_name}/{PROCESSED_USERS_CSV_PATH}"
        },
    )

    # Task: Validate Processed Users Data
    validate_processed_users = PythonOperator(
        task_id='validate_processed_users',
        python_callable=validate_csv_users_data,
        op_kwargs={
            'bucket': processed_bucket_name,
            'key': PROCESSED_USERS_CSV_PATH
        },
    )

    # Task: Extract and Clean Songs Data
    extract_and_clean_songs = PythonOperator(
        task_id='extract_and_clean_songs',
        python_callable=clean_songs_data,
        op_kwargs={
            'input_path': f"s3://{raw_bucket_name}/{RAW_SONGS_CSV_PATH}",
            'output_path': f"s3://{processed_bucket_name}/{PROCESSED_SONGS_CSV_PATH}"
        },
    )

    # Task: Validate Processed Songs Data
    validate_processed_songs = PythonOperator(
        task_id='validate_processed_songs',
        python_callable=validate_csv_songs_data,
        op_kwargs={
            'bucket': processed_bucket_name,
            'key': PROCESSED_SONGS_CSV_PATH
        },
    )

    # Task: Extract and Clean Streaming Data (Batch)
    extract_streams = PythonOperator(
        task_id='extract_streams',
        python_callable=extract_batch_streams,
        op_kwargs={
            'input_path': f"s3://{raw_bucket_name}/{RAW_STREAMING_DIR_PATH}",
            'output_path': f"s3://{processed_bucket_name}/{PROCESSED_STREAMING_PARQUET_PATH}"
        },
    )

    # Task: Validate Processed Streaming Data
    validate_processed_streams = PythonOperator(
        task_id='validate_processed_streams',
        python_callable=validate_csv_streaming_data,
        op_kwargs={
            'bucket': processed_bucket_name,
            'key': PROCESSED_STREAMING_PARQUET_PATH
        },
    )

    # Task: Create Data Warehouse Tables
    create_tables = PythonOperator(
        task_id='create_dwh_tables',
        python_callable=create_dwh_tables,
        op_kwargs={'conn_string': DWH_CONN_STRING},
    )

    # Task: Load Users Data to DWH
    load_users = PythonOperator(
        task_id='load_users_to_dwh',
        python_callable=load_users_to_dwh,
        op_kwargs={
            'input_path': f"s3://{processed_bucket_name}/{PROCESSED_USERS_PARQUET_PATH}",
            'conn_string': DWH_CONN_STRING,
            'iam_role': REDSHIFT_IAM_ROLE
        },
    )

    # Task: Load Songs Data to DWH
    load_songs = PythonOperator(
        task_id='load_songs_to_dwh',
        python_callable=load_songs_to_dwh,
        op_kwargs={
            'input_path': f"s3://{processed_bucket_name}/{PROCESSED_SONGS_PARQUET_PATH}",
            'conn_string': DWH_CONN_STRING,
            'iam_role': REDSHIFT_IAM_ROLE
        },
    )

    # Task: Load Streaming Data to DWH
    load_streams = PythonOperator(
        task_id='load_streaming_to_dwh',
        python_callable=load_streaming_to_dwh,
        op_kwargs={
            'input_path': f"s3://{processed_bucket_name}/{PROCESSED_STREAMING_PARQUET_PATH}",
            'conn_string': DWH_CONN_STRING,
            'iam_role': REDSHIFT_IAM_ROLE
        },
    )

    # Task: Compute KPIs and save to S3
    compute_kpis = PythonOperator(
        task_id='compute_kpis',
        python_callable=compute_and_save_kpis,
        op_kwargs={
            'conn_string': DWH_CONN_STRING,
            'output_dir': DATA_KPI_OUTPUTS_DIR,
            'songs_path': f"s3://{processed_bucket_name}/{PROCESSED_SONGS_PARQUET_PATH}",
            'streaming_path': f"s3://{processed_bucket_name}/{PROCESSED_STREAMING_PARQUET_PATH}"
        },
    )

    # Define task dependencies using set_downstream()
    extract_and_clean_users.set_downstream(validate_processed_users)
    extract_and_clean_songs.set_downstream(validate_processed_songs)
    extract_streams.set_downstream(validate_processed_streams)

    validate_processed_users.set_downstream(create_tables)
    validate_processed_songs.set_downstream(create_tables)
    validate_processed_streams.set_downstream(create_tables)

    create_tables.set_downstream([load_users, load_songs, load_streams])

    load_users.set_downstream(compute_kpis)
    load_songs.set_downstream(compute_kpis)
    load_streams.set_downstream(compute_kpis)