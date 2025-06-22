# ======================================================================================
# validate_data.py
# This file provides functions for validating the schema and basic integrity of CSV data
# stored in S3, specifically for users, songs, and streaming data.
# ======================================================================================
import boto3
from src.config import get_config
from src.logging_config import setup_logger, add_s3_handler
import pandas as pd
import io
import urllib.parse

# Set up logger for the validate_data module
logger = setup_logger('validate_data', mode='a')
add_s3_handler(logger, get_config())

# Initialize S3 client
s3_client = boto3.client('s3')

def validate_csv_users_data(bucket, key, **kwargs):
    """
    Validates the schema of users data from a specified S3 CSV file.
    Args:
        bucket (str): The S3 bucket name where the users data is located.
        key (str): The S3 key (path) to the users data CSV file.
        **kwargs: Arbitrary keyword arguments, primarily for Airflow's 'ti' object.
    Raises:
        ValueError: If required columns are missing in the data.
        Exception: For any other errors during S3 access or data reading.
    """
    ti = kwargs.get('ti')
    logger.info(f"Validating users data at s3://{bucket}/{key}")
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), nrows=5)

        required_columns = ['user_id', 'user_name', 'user_age', 'user_country', 'created_at']
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            logger.error(f"Missing columns in users data: {missing}")
            if ti:
                ti.xcom_push(key='data_processed', value=False)
            raise ValueError(f"Missing required columns: {missing}")

        logger.info("Users data validation passed")
        if ti:
            ti.xcom_push(key='data_processed', value=True)
    except Exception as e:
        logger.error(f"Validation failed for users data: {e}")
        if ti:
            ti.xcom_push(key='data_processed', value=False)
        raise

def validate_csv_songs_data(bucket, key, **kwargs):
    """
    Validates the schema of songs data from a specified S3 CSV file.
    Args:
        bucket (str): The S3 bucket name where the songs data is located.
        key (str): The S3 key (path) to the songs data CSV file.
        **kwargs: Arbitrary keyword arguments, primarily for Airflow's 'ti' object.
    Raises:
        ValueError: If required columns are missing in the data.
        Exception: For any other errors during S3 access or data reading.
    """
    ti = kwargs.get('ti')
    logger.info(f"Validating songs data at s3://{bucket}/{key}")
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), nrows=5)

        required_columns = [
            'track_id', 'artists', 'album_name', 'track_name', 'popularity',
            'duration_ms', 'explicit', 'danceability', 'energy', 'key',
            'loudness', 'mode', 'speechiness', 'acousticness',
            'instrumentalness', 'liveness', 'valence', 'tempo',
            'time_signature', 'track_genre'
        ]
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            logger.error(f"Missing columns in songs data: {missing}")
            if ti:
                ti.xcom_push(key='data_processed', value=False)
            raise ValueError(f"Missing required columns: {missing}")

        logger.info("Songs data validation passed")
        if ti:
            ti.xcom_push(key='data_processed', value=True)
    except Exception as e:
        logger.error(f"Validation failed for songs data: {e}")
        if ti:
            ti.xcom_push(key='data_processed', value=False)
        raise

def validate_csv_streaming_data(bucket, key, **kwargs):
    """
    Validates the schema of streaming data from a specified S3 prefix.
    Args:
        bucket (str): The S3 bucket name where the streaming data is located.
        key (str): The S3 key (prefix) to the streaming data files.
        **kwargs: Arbitrary keyword arguments, primarily for Airflow's 'ti' object.
    Raises:
        ValueError: If no files are found or if required columns are missing.
        Exception: For any other errors during S3 access or data reading.
    """
    ti = kwargs.get('ti')
    logger.info(f"Validating streaming data at s3://{bucket}/{key}")
    try:
        if key.endswith('.parquet'):
            logger.info(f"Sampling file for validation: {key}")
            df = pd.read_parquet(f"s3://{bucket}/{key}", engine='pyarrow')
            
            required_columns = ['user_id', 'song_id', 'listen_time']
            missing = [col for col in required_columns if col not in df.columns]
            if missing:
                logger.error(f"Missing columns in {key}: {missing}")
                if ti:
                    ti.xcom_push(key='data_processed', value=False)
                raise ValueError(f"Missing required columns in {key}: {missing}")
        else:
            raise ValueError(f"Validator expected a .parquet file, but got: {key}")

        logger.info("Streaming data validation passed")
        if ti:
            ti.xcom_push(key='data_processed', value=True)
    except Exception as e:
        logger.error(f"Validation failed for streaming data: {e}")
        if ti:
            ti.xcom_push(key='data_processed', value=False)
        raise