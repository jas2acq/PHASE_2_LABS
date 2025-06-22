# ======================================================================================
# cleaning.py
# This file contains functions for cleaning and processing data for users, songs, and streaming.
# ======================================================================================
import pandas as pd
import io
import boto3
import json
import re
from datetime import datetime
from src.config import get_config
from src.logging_config import setup_logger, add_s3_handler

# Set up logger for the cleaning module
logger = setup_logger('cleaning', mode='a')
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
def get_processed_files(config, file_type):
    """
    Retrieves a set of already processed file keys from a state file stored in S3.
    Args:
        config (dict): The configuration dictionary containing 'STATE_DIR'.
        file_type (str): The type of file (e.g., 'users', 'songs', 'streams') to retrieve processed state for.
    Returns:
        set: A set of processed file keys.
    Returns an empty set if the state file does not exist.
    Raises:
        Exception: If there's an error reading the state file from S3.
    """
    try:
        bucket = config['STATE_DIR'].split('//')[1].split('/')[0]
        prefix = '/'.join(config['STATE_DIR'].split('//')[1].split('/')[1:])
        state_key = f"{prefix}/processed_{file_type}.json" if prefix else f"processed_{file_type}.json"
        obj = s3_client.get_object(Bucket=bucket, Key=state_key)
        return set(json.loads(obj['Body'].read().decode('utf-8')).get('processed', []))
    except s3_client.exceptions.NoSuchKey:
        return set()
    except Exception as e:
        logger.error(f"Error reading processed files for {file_type}: {e}")
        raise

# Save the list of processed files to S3 state directory
def save_processed_files(processed_files, config, file_type):
    """
    Saves the set of processed file keys to a state file in S3.
    Args:
        processed_files (set): A set of file keys that have been processed.
        config (dict): The configuration dictionary containing 'STATE_DIR'.
        file_type (str): The type of file (e.g., 'users', 'songs', 'streams') to save processed state for.
    Raises:
        Exception: If there's an error saving the state file to S3.
    """
    try:
        bucket = config['STATE_DIR'].split('//')[1].split('/')[0]
        prefix = '/'.join(config['STATE_DIR'].split('//')[1].split('/')[1:])
        state_key = f"{prefix}/processed_{file_type}.json" if prefix else f"processed_{file_type}.json"
        state_data = {'processed': list(processed_files), 'last_updated': datetime.utcnow().isoformat()}
        s3_client.put_object(Bucket=bucket, Key=state_key, Body=json.dumps(state_data))
    except Exception as e:
        logger.error(f"Error saving processed files for {file_type}: {e}")
        raise

# Clean a DataFrame by dropping NaNs and duplicates based on specified columns
def clean_dataframe(df, columns_to_check):
    """
    Cleans a Pandas DataFrame by dropping rows with NaN values in specified columns
    and removing duplicate rows based on a subset of columns.
    Args:
        df (pd.DataFrame): The DataFrame to clean.
        columns_to_check (list): A list of column names to check for NaN values and duplicates.
    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    if df.empty:
        logger.warning("Empty DataFrame provided for cleaning")
        return df
    df = df.dropna(subset=columns_to_check)
    df = df.drop_duplicates(subset=columns_to_check)
    return df

# Clean users data
def clean_users_data(input_path, output_path, **kwargs):
    """
    Cleans user data from a specified S3 input path and saves the cleaned data
    in CSV and Parquet formats to an S3 output path.
    Args:
        input_path (str): The S3 URI of the input users data CSV file.
        output_path (str): The S3 URI for the output cleaned users data.
        **kwargs: Arbitrary keyword arguments, primarily for Airflow's 'ti' object.
    """
    config = get_config()
    ti = kwargs.get('ti')
    bucket, key = parse_s3_uri(input_path)
    processed_files = get_processed_files(config, 'users')
    if key in processed_files:
        logger.info(f"Users file {key} already processed, skipping")
        if ti:
            ti.xcom_push(key='data_processed', value=False)
        return
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
    except s3_client.exceptions.NoSuchKey:
        logger.error(f"File not found at {input_path}, skipping processing")
        if ti:
            ti.xcom_push(key='data_processed', value=False)
        raise
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf-8', on_bad_lines='skip')
    if df.empty:
        logger.warning(f"Users.csv is empty at {input_path}, skipping processing")
        if ti:
            ti.xcom_push(key='data_processed', value=False)
        return
    df['user_id'] = df['user_id'].astype(str)
    df = df[df['user_age'] >= 0]
    df = df.dropna(subset=['user_id', 'user_name', 'user_country', 'created_at'])
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df = clean_dataframe(df, ['user_id'])
    output_bucket, output_key = parse_s3_uri(output_path)
    rds_processed_dir = config.get('RDS_PROCESSED_DIR', 'rds-processed')
    if not output_key.startswith('processed/'):
        output_key = f"processed/{rds_processed_dir}/{output_key}"
    with io.BytesIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=output_bucket, Key=output_key, Body=csv_buffer.getvalue())
    with io.BytesIO() as parquet_buffer:
        df.to_parquet(parquet_buffer, index=False)
        parquet_key = output_key.rsplit('.', 1)[0] + '.parquet'
        s3_client.put_object(Bucket=output_bucket, Key=parquet_key, Body=parquet_buffer.getvalue())
    processed_files.add(key)
    save_processed_files(processed_files, config, 'users')
    logger.info(f"Processed {len(df)} user records to s3://{output_bucket}/{output_key} and .parquet")
    if ti:
        ti.xcom_push(key='data_processed', value=True)

# Clean songs data
def clean_songs_data(input_path, output_path, **kwargs):
    """
    Cleans songs data from a specified S3 input path and saves the cleaned data
    in CSV and Parquet formats to an S3 output path.
    Args:
        input_path (str): The S3 URI of the input songs data CSV file.
        output_path (str): The S3 URI for the output cleaned songs data.
        **kwargs: Arbitrary keyword arguments, primarily for Airflow's 'ti' object.
    """
    config = get_config()
    ti = kwargs.get('ti')
    bucket, key = parse_s3_uri(input_path)
    processed_files = get_processed_files(config, 'songs')
    if key in processed_files:
        logger.info(f"Songs file {key} already processed, skipping")
        if ti:
            ti.xcom_push(key='data_processed', value=False)
        return
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
    except s3_client.exceptions.NoSuchKey:
        logger.error(f"File not found at {input_path}, skipping processing")
        if ti:
            ti.xcom_push(key='data_processed', value=False)
        raise
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf-8', on_bad_lines='skip')
    if df.empty:
        logger.warning(f"Songs.csv is empty at {input_path}, skipping processing")
        if ti:
            ti.xcom_push(key='data_processed', value=False)
        return
    df['track_id'] = df['track_id'].astype(str)
    df['artists'] = df['artists'].fillna('Unknown').astype(str)
    df['album_name'] = df['album_name'].fillna('Unknown').astype(str)
    df['track_name'] = df['track_name'].fillna('Unknown').astype(str)
    df = df[df['duration_ms'] >= 0]
    df = clean_dataframe(df, ['track_id'])
    output_bucket, output_key = parse_s3_uri(output_path)
    rds_processed_dir = config.get('RDS_PROCESSED_DIR', 'rds-processed')
    if not output_key.startswith('processed/'):
        output_key = f"processed/{rds_processed_dir}/{output_key}"
    with io.BytesIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=output_bucket, Key=output_key, Body=csv_buffer.getvalue())
    with io.BytesIO() as parquet_buffer:
        df.to_parquet(parquet_buffer, index=False)
        parquet_key = output_key.rsplit('.', 1)[0] + '.parquet'
        s3_client.put_object(Bucket=output_bucket, Key=parquet_key, Body=parquet_buffer.getvalue())
    processed_files.add(key)
    save_processed_files(processed_files, config, 'songs')
    logger.info(f"Processed {len(df)} song records to s3://{output_bucket}/{output_key} and .parquet")
    if ti:
        ti.xcom_push(key='data_processed', value=True)

# Clean streaming data
def clean_streaming_data(input_path, output_path, **kwargs):
    """
    Cleans streaming data from a specified S3 input path (prefix) and saves the
    cleaned and combined data in CSV and Parquet formats to an S3 output path.
    Args:
        input_path (str): The S3 URI prefix of the input streaming data CSV files.
        output_path (str): The S3 URI for the output cleaned streaming data.
        **kwargs: Arbitrary keyword arguments, primarily for Airflow's 'ti' object.
    """
    config = get_config()
    ti = kwargs.get('ti')
    bucket, prefix = parse_s3_uri(input_path)
    processed_files = get_processed_files(config, 'streams')
    all_dfs = []
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
                    all_dfs.append(df)
                    new_files.add(file_key)
                except Exception as e:
                    logger.error(f"Error reading {file_key}: {e}")
                    continue

        if not all_dfs:
            logger.info("No valid new streaming data to process.")
            if ti:
                ti.xcom_push(key='data_processed', value=False)
            return

        combined_df = pd.concat(all_dfs, ignore_index=True)
        combined_df['user_id'] = combined_df['user_id'].astype(str)
        combined_df['track_id'] = combined_df['track_id'].astype(str)  
        combined_df.rename(columns={'track_id': 'song_id'}, inplace=True)  
        combined_df['listen_time'] = pd.to_datetime(combined_df['listen_time'], errors='coerce')
        combined_df = combined_df.dropna(subset=['user_id', 'song_id', 'listen_time'])
        combined_df = clean_dataframe(combined_df, ['user_id', 'song_id', 'listen_time'])
        output_bucket, output_key = parse_s3_uri(output_path)
        batch_processed_dir = config.get('BATCH_PROCESSED_DIR', 'batch-processed')
        if not output_key.startswith('processed/'):
            output_key = f"processed/{batch_processed_dir}/{output_key}"
        with io.BytesIO() as csv_buffer:
            combined_df.to_csv(csv_buffer, index=False)
            s3_client.put_object(Bucket=output_bucket, Key=output_key, Body=csv_buffer.getvalue())
        with io.BytesIO() as parquet_buffer:
            combined_df.to_parquet(parquet_buffer, index=False)
            parquet_key = output_key.rsplit('.', 1)[0] + '.parquet'
            s3_client.put_object(Bucket=output_bucket, Key=parquet_key, Body=parquet_buffer.getvalue())
        processed_files.update(new_files)
        save_processed_files(processed_files, config, 'streams')
        logger.info(f"Processed {len(combined_df)} streaming records to s3://{output_bucket}/{output_key} and .parquet")
        if ti:
            ti.xcom_push(key='data_processed', value=True)

    except Exception as e:
        logger.error(f"Error during streaming data cleaning: {e}")
        if ti:
            ti.xcom_push(key='data_processed', value=False)
        raise