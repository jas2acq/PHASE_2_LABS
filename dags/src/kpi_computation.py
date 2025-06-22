# ======================================================================================
# kpi_computation.py
# This file contains functions for processing data and computing Key Performance Indicators (KPIs).
# ======================================================================================
import boto3
import pandas as pd
import io
from datetime import datetime
from src.config import get_config
from src.logging_config import setup_logger, add_s3_handler
import pyarrow.parquet as pq
import urllib.parse
import psutil
import os  

# Set up logger for the kpi_computation module
logger = setup_logger('kpi_computation', mode='a')
add_s3_handler(logger, get_config())

# Initialize S3 client
s3_client = boto3.client('s3')

def process_data(streaming_path, songs_path, chunk_size=100000):
    """
    Processes streaming and songs data to compute various KPIs such as unique listeners,
    top artists, track diversity, and genre-based statistics.
    Args:
        streaming_path (str): S3 URI to the processed streaming data (Parquet format).
        songs_path (str): S3 URI to the processed songs data (Parquet format).
        chunk_size (int): The number of rows to process at a time for streaming data.
    Returns:
        tuple: A tuple containing genre-based KPIs and hourly-based KPIs.
    Raises:
        ValueError: If S3 URI formats are invalid.
        Exception: For any other errors during data processing or S3 operations.
    """
    config = get_config()
    streaming_url = urllib.parse.urlparse(streaming_path)
    if not streaming_url.netloc or not streaming_url.scheme:
        raise ValueError(f"Invalid streaming_path format: {streaming_path}")
    streaming_bucket = streaming_url.netloc
    streaming_key = streaming_url.path.lstrip('/')
    logger.debug(f"Streaming path parsed: bucket={streaming_bucket}, key={streaming_key}")

    songs_url = urllib.parse.urlparse(songs_path)
    if not songs_url.netloc or not songs_url.scheme:
        raise ValueError(f"Invalid songs_path format: {songs_path}")
    songs_bucket = songs_url.netloc
    songs_key = songs_url.path.lstrip('/')
    logger.debug(f"Songs path parsed: bucket={songs_bucket}, key={songs_key}")

    try:
        songs_obj = s3_client.get_object(Bucket=songs_bucket, Key=songs_key)
        songs_df = pd.read_parquet(io.BytesIO(songs_obj['Body'].read()))
        songs_df['track_id'] = songs_df['track_id'].astype(str)
        songs_df['track_genre'] = songs_df['track_genre'].astype(str)
        songs_df['artists'] = songs_df['artists'].astype(str)
        logger.info(f"Loaded {len(songs_df)} records from songs data.")

        genre_data = {}
        hourly_data = {f'{h:02d}': {'unique_users': set(), 'artist_counts': {}, 'track_counts': {}} for h in range(24)}

        streaming_obj = s3_client.get_object(Bucket=streaming_bucket, Key=streaming_key)
        streaming_bytes = io.BytesIO(streaming_obj['Body'].read())
        parquet_file = pq.ParquetFile(streaming_bytes)

        num_row_groups = parquet_file.num_row_groups
        total_rows_processed = 0

        for i in range(num_row_groups):
            table = parquet_file.read_row_group(i)
            streaming_chunk_df = table.to_pandas()
            total_rows_processed += len(streaming_chunk_df)

            streaming_chunk_df['user_id'] = streaming_chunk_df['user_id'].astype(str)
            streaming_chunk_df['song_id'] = streaming_chunk_df['song_id'].astype(str)
            streaming_chunk_df['listen_time'] = pd.to_datetime(streaming_chunk_df['listen_time'], errors='coerce')
            streaming_chunk_df.dropna(subset=['listen_time'], inplace=True)

            merged_df = pd.merge(
                streaming_chunk_df,
                songs_df[['track_id', 'track_genre', 'artists']],
                left_on='song_id',
                right_on='track_id',
                how='inner'
            )
            merged_df.drop(columns=['track_id'], inplace=True)

            genre_listens = merged_df.groupby('track_genre').size().to_dict()
            for genre, listens in genre_listens.items():
                genre_data[genre] = genre_data.get(genre, 0) + listens

            merged_df['listen_hour'] = merged_df['listen_time'].dt.hour.astype(str).str.zfill(2)
            for hour in range(24):
                h_str = f'{hour:02d}'
                hourly_chunk = merged_df[merged_df['listen_hour'] == h_str]
                hourly_data[h_str]['unique_users'].update(hourly_chunk['user_id'].unique())
                artist_listens = hourly_chunk.explode('artists').groupby('artists').size()
                for artist_name, count in artist_listens.items():
                    hourly_data[h_str]['artist_counts'][artist_name] = hourly_data[h_str]['artist_counts'].get(artist_name, 0) + count
                track_listens = hourly_chunk.groupby('song_id').size()
                for track_id, count in track_listens.items():
                    hourly_data[h_str]['track_counts'][track_id] = hourly_data[h_str]['track_counts'].get(track_id, 0) + count

            if i % 10 == 0:
                process = psutil.Process(os.getpid())
                mem_info = process.memory_info()
                logger.debug(f"Memory usage after chunk {i}: RSS={mem_info.rss / (1024 ** 2):.2f} MB, VMS={mem_info.vms / (1024 ** 2):.2f} MB")

        logger.info(f"Finished processing all streaming data. Total rows processed: {total_rows_processed}")
        return genre_data, hourly_data

    except Exception as e:
        logger.error(f"Error processing data for KPI computation: {e}")
        raise

def compute_and_save_kpis(conn_string, output_dir, songs_path, streaming_path, **kwargs):
    """
    Computes and saves Key Performance Indicators (KPIs) to S3.
    Args:
        conn_string (str): Redshift connection string.
        output_dir (str): S3 URI for the directory where KPI outputs will be saved.
        songs_path (str): S3 URI to the processed songs data (Parquet format).
        streaming_path (str): S3 URI to the processed streaming data (Parquet format).
        **kwargs: Arbitrary keyword arguments, primarily for Airflow's 'ti' object.
    """
    ti = kwargs.get('ti')
    logger.info("Starting KPI computation")
    try:
        genre_kpis, hourly_kpis = process_data(streaming_path, songs_path)

        genre_kpi_list = []
        for genre, listens in genre_kpis.items():
            genre_kpi_list.append({'genre': genre, 'total_listens': listens})

        hourly_kpi_list = []
        for hour, data in hourly_kpis.items():
            unique_listeners = len(data['unique_users'])
            top_artists = sorted(data['artist_counts'].items(), key=lambda x: x[1], reverse=True)[:5]
            total_listens = sum(data['track_counts'].values())  # Fixed: Correct denominator
            track_diversity_index = len(data['track_counts']) / max(1, total_listens)

            hourly_kpi_list.append({
                'hour': hour,
                'unique_listeners': unique_listeners,
                'top_artists': top_artists,
                'track_diversity_index': track_diversity_index
            })

        output_url = urllib.parse.urlparse(output_dir)
        if not output_url.netloc or not output_url.scheme:
            raise ValueError(f"Invalid output_dir format: {output_dir}")
        kpi_bucket = output_url.netloc
        kpi_prefix = output_url.path.lstrip('/')

        genre_key = f"{kpi_prefix}/genres/genre_{datetime.now().strftime('%Y%m%d')}.parquet"
        genre_df = pd.DataFrame(genre_kpi_list)
        with io.BytesIO() as buffer:
            genre_df.to_parquet(buffer, index=False)
            s3_client.put_object(Bucket=kpi_bucket, Key=genre_key, Body=buffer.getvalue())
        logger.info(f"Saved genre KPIs to s3://{kpi_bucket}/{genre_key}")

        for kpi in hourly_kpi_list:
            hour_key = f"{kpi_prefix}/hours/kpis_{kpi['hour']}_{datetime.now().strftime('%Y%m%d')}.parquet"
            hourly_df = pd.DataFrame([kpi])
            with io.BytesIO() as buffer:
                hourly_df.to_parquet(buffer, index=False)
                s3_client.put_object(Bucket=kpi_bucket, Key=hour_key, Body=buffer.getvalue())
            logger.info(f"Saved hourly KPIs for hour {kpi['hour']} to s3://{kpi_bucket}/{hour_key}")

        logger.info("KPI computation and saving completed successfully.")
        if ti:
            ti.xcom_push(key='data_processed', value=True)

    except Exception as e:
        logger.error(f"Error during KPI computation and saving: {e}")
        if ti:
            ti.xcom_push(key='data_processed', value=False)
        raise