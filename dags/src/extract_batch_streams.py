# ======================================================================================
# extract_batch_streams.py
# This file is responsible for extracting and processing batch streaming data.
# ======================================================================================
import boto3
import re
import json
from src.config import get_config
from src.logging_config import setup_logger, add_s3_handler
import pandas as pd
import io
from datetime import datetime

# Set up logger for the extract_batch_streams module
logger = setup_logger('extract_batch_streams', mode='a')
add_s3_handler(logger, get_config())

# Initialize S3 client
s3_client = boto3.client('s3')

# Parse S3 URI into bucket and key
def parse_s3_uri(s3_path):
    """
    Parses an S3 URI string into its constituent bucket and key components.
    Args:
        s3_path (str): The full S3 URI (e.g., "s3://my-bucket/path/to/object").
    Returns:
        tuple: A tuple containing the bucket name (str) and the object key (str).
    Raises:
        ValueError: If the provided S3 URI is invalid.
    """
    parsed = s3_path.split('//')
    if len(parsed) < 2:
        raise ValueError(f"Invalid S3 URI: {s3_path}")
    bucket = parsed[1].split('/')[0]
    key = '/'.join(parsed[1].split('/')[1:])
    return bucket, key

# Get a set of already processed files from S3 state directory
def get_processed_files(config):
    """
    Retrieves a set of already processed streaming file keys from a state file stored in S3.
    Args:
        config (dict): The configuration dictionary containing 'STATE_DIR'.
    Returns:
        set: A set of processed streaming file keys.
    Returns an empty set if the state file does not exist.
    Raises:
        Exception: If there's an error reading the state file from S3.
    """
    try:
        bucket = config['STATE_DIR'].split('//')[1].split('/')[0]
        prefix = '/'.join(config['STATE_DIR'].split('//')[1].split('/')[1:])
        state_key = f"{prefix}/processed_streams.json" if prefix else "processed_streams.json"
        obj = s3_client.get_object(Bucket=bucket, Key=state_key)
        state = json.loads(obj['Body'].read().decode('utf-8'))
        logger.debug(f"Loaded state file {state_key}: {state}")
        return set(state.get('processed', []))
    except s3_client.exceptions.NoSuchKey:
        logger.debug("No state file found, starting fresh")
        return set()
    except Exception as e:
        logger.error(f"Error reading processed files: {e}")
        raise

# Save the list of processed files to S3 state directory
def save_processed_files(processed_files, config):
    """
    Saves the set of processed streaming file keys to a state file in S3.
    Args:
        processed_files (set): A set of file keys that have been processed.
        config (dict): The configuration dictionary containing 'STATE_DIR'.
    Raises:
        Exception: If there's an error saving the state file to S3.
    """
    try:
        bucket = config['STATE_DIR'].split('//')[1].split('/')[0]
        prefix = '/'.join(config['STATE_DIR'].split('//')[1].split('/')[1:])
        state_key = f"{prefix}/processed_streams.json" if prefix else "processed_streams.json"
        state_data = {'processed': list(processed_files), 'last_updated': datetime.utcnow().isoformat()}
        s3_client.put_object(Bucket=bucket, Key=state_key, Body=json.dumps(state_data))
    except Exception as e:
        logger.error(f"Error saving processed files: {e}")
        raise

# Extract batch streams
def extract_batch_streams(input_path, output_path, **kwargs):
    """
    Extracts, combines, and processes batch streaming data from multiple CSV files
    in a specified S3 input path (prefix).
    Args:
        input_path (str): The S3 URI prefix of the input streaming data CSV files.
        output_path (str): The S3 URI for the output processed streaming data (Parquet).
        **kwargs: Arbitrary keyword arguments, primarily for Airflow's 'ti' object.
    Raises:
        ValueError: If the input_path or output_path S3 URI is invalid.
        Exception: For any other errors during data extraction and processing.
    """
    config = get_config()
    ti = kwargs.get('ti')
    bucket, prefix = parse_s3_uri(input_path)
    processed_files = get_processed_files(config)
    all_streams = []
    new_files = set()

    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' not in response:
            logger.info(f"No new files in {input_path} to process.")
            if ti:
                ti.xcom_push(key='data_processed', value=False)
            return

        for obj in response['Contents']:
            file_key = obj['Key']
            if file_key.endswith('.csv') and file_key not in processed_files:
                logger.info(f"Processing new streaming file: {file_key}")
                try:
                    csv_obj = s3_client.get_object(Bucket=bucket, Key=file_key)
                    df = pd.read_csv(io.BytesIO(csv_obj['Body'].read()), encoding='utf-8', on_bad_lines='skip')
                    all_streams.append(df)
                    new_files.add(file_key)
                except Exception as e:
                    logger.error(f"Error reading {file_key}: {e}")
                    continue

        if not all_streams:
            logger.info("No valid new streaming data to process.")
            if ti:
                ti.xcom_push(key='data_processed', value=False)
            return

        combined_df = pd.concat(all_streams, ignore_index=True)
        if combined_df.empty:
            logger.info("No data in new streaming files.")
            if ti:
                ti.xcom_push(key='data_processed', value=False)
            return

        combined_df['user_id'] = combined_df['user_id'].astype(str)
        combined_df['track_id'] = combined_df['track_id'].astype(str)
        combined_df.rename(columns={'track_id': 'song_id'}, inplace=True)
        combined_df['listen_time'] = pd.to_datetime(combined_df['listen_time'], errors='coerce')
        combined_df.dropna(subset=['user_id', 'song_id', 'listen_time'], inplace=True)
        combined_df.drop_duplicates(subset=['user_id', 'song_id', 'listen_time'], inplace=True)

        output_bucket, output_key = parse_s3_uri(output_path)
        with io.BytesIO() as parquet_buffer:
            combined_df.to_parquet(parquet_buffer, index=False)
            s3_client.put_object(Bucket=output_bucket, Key=output_key, Body=parquet_buffer.getvalue())

        processed_files.update(new_files)
        save_processed_files(processed_files, config)
        logger.info(f"Processed {len(combined_df)} streaming records to s3://{output_bucket}/{output_key}")
        if ti:
            ti.xcom_push(key='data_processed', value=True)

    except Exception as e:
        logger.error(f"Error during streaming data extraction: {e}")
        if ti:
            ti.xcom_push(key='data_processed', value=False)
        raise