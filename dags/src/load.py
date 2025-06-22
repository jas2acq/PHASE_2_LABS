# ======================================================================================
# load.py
# This file contains functions for loading data into Amazon Redshift Data Warehouse.
# ======================================================================================
import boto3
from botocore.exceptions import ClientError
from src.config import get_config
from src.logging_config import setup_logger, add_s3_handler
import urllib.parse
import time

# Set up logger for the load module
logger = setup_logger('load', mode='a')
add_s3_handler(logger, get_config())

# Initialize S3 and Redshift Data API clients
s3_client = boto3.client('s3')
redshift_data_client = boto3.client('redshift-data')

def wait_for_query_completion(query_id, dbname, cluster_identifier, db_user):
    """
    Waits for a Redshift Data API query to complete.
    Args:
        query_id (str): The ID of the Redshift Data API query to wait for.
        dbname (str): The database name.
        cluster_identifier (str): The Redshift cluster identifier.
        db_user (str): The database user.
    Raises:
        Exception: If the query fails or an unexpected status is encountered.
    """
    while True:
        desc_response = redshift_data_client.describe_statement(Id=query_id)
        status = desc_response['Status']
        if status == 'FINISHED':
            logger.debug(f"Query {query_id} finished successfully.")
            break
        elif status == 'FAILED':
            logger.error(f"Query {query_id} failed: {desc_response.get('Error')}")
            raise Exception(f"Redshift query failed: {desc_response.get('Error')}")
        elif status in ['ABORTED', 'ALL_CANCELLED', 'CANCELLING']:
            logger.error(f"Query {query_id} was {status}.")
            raise Exception(f"Redshift query was {status}.")
        logger.debug(f"Query {query_id} status: {status}. Waiting...")
        time.sleep(5)

def create_dwh_tables(conn_string, schema='public', **kwargs):
    """
    Creates necessary tables in the Redshift Data Warehouse if they do not already exist.
    Args:
        conn_string (str): The Redshift connection string.
        schema (str): The schema name where tables will be created. Defaults to 'public'.
        **kwargs: Arbitrary keyword arguments, primarily for Airflow's 'ti' object.
    Raises:
        Exception: If there's an error during table creation in Redshift.
    """
    ti = kwargs.get('ti')
    try:
        logger.info(f"Creating Redshift tables in schema {schema}")
        cluster_id = conn_string.split('//')[1].split('.')[0]
        db_user = "awsuser"
        dbname = conn_string.split('/')[-1]

        create_table_sql = f"""
        CREATE SCHEMA IF NOT EXISTS {schema};
        CREATE TABLE IF NOT EXISTS {schema}.users (
            user_id VARCHAR(255) PRIMARY KEY,
            user_name VARCHAR(255),
            user_age INTEGER,
            user_country VARCHAR(255),
            created_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS {schema}.songs (
            track_id VARCHAR(255) PRIMARY KEY,
            artists VARCHAR(255),
            album_name VARCHAR(255),
            track_name VARCHAR(255),
            popularity INTEGER,
            duration_ms INTEGER,
            explicit INTEGER,
            danceability FLOAT,
            energy FLOAT,
            key INTEGER,
            loudness FLOAT,
            mode INTEGER,
            speechiness FLOAT,
            acousticness FLOAT,
            instrumentalness FLOAT,
            liveness FLOAT,
            valence FLOAT,
            tempo FLOAT,
            time_signature INTEGER,
            track_genre VARCHAR(255)
        );
        CREATE TABLE IF NOT EXISTS {schema}.streaming (
            user_id VARCHAR(255),
            song_id VARCHAR(255),
            listen_time TIMESTAMP,
            PRIMARY KEY (user_id, song_id, listen_time)
        );
        CREATE TABLE IF NOT EXISTS {schema}.staging_users (LIKE {schema}.users);
        CREATE TABLE IF NOT EXISTS {schema}.staging_songs (LIKE {schema}.songs);
        CREATE TABLE IF NOT EXISTS {schema}.staging_streaming (LIKE {schema}.streaming);
        """

        response = redshift_data_client.execute_statement(
            Database=dbname,
            Sql=create_table_sql,
            ClusterIdentifier=cluster_id,
            DbUser=db_user
        )
        query_id = response['Id']
        logger.info(f"Executed table creation statement with Query ID: {query_id}")
        wait_for_query_completion(query_id, dbname, cluster_id, db_user)
        logger.info("Redshift tables created successfully.")
        if ti:
            ti.xcom_push(key='data_processed', value=True)

    except Exception as e:
        logger.error(f"Error creating Redshift tables: {e}")
        if ti:
            ti.xcom_push(key='data_processed', value=False)
        raise

def load_to_redshift(table_name, staging_table_name, s3_input_path, columns, pkey_columns, conn_string, iam_role, **kwargs):
    """
    Loads data from S3 into a Redshift table using a staging table and UPSERT logic.
    Args:
        table_name (str): The name of the target Redshift table.
        staging_table_name (str): The name of the staging table.
        s3_input_path (str): The S3 URI of the input data (Parquet file).
        columns (list): A list of column names in the order they appear in the S3 data.
        pkey_columns (list): A list of primary key column names for UPSERT logic.
        conn_string (str): The Redshift connection string.
        iam_role (str): The IAM role ARN for Redshift to access S3.
        **kwargs: Arbitrary keyword arguments, primarily for Airflow's 'ti' object.
    Raises:
        Exception: If there's an error during the data loading process.
    """
    ti = kwargs.get('ti')
    try:
        logger.info(f"Loading data from {s3_input_path} to Redshift table {table_name} via staging table {staging_table_name}")
        cluster_id = conn_string.split('//')[1].split('.')[0]
        db_user = "awsuser"
        dbname = conn_string.split('/')[-1]

        clear_staging_sql = f"TRUNCATE TABLE public.{staging_table_name};"
        response_clear = redshift_data_client.execute_statement(
            Database=dbname, Sql=clear_staging_sql, ClusterIdentifier=cluster_id, DbUser=db_user
        )
        logger.debug(f"Cleared staging table {staging_table_name} with Query ID: {response_clear['Id']}")
        wait_for_query_completion(response_clear['Id'], dbname, cluster_id, db_user)

        copy_sql = f"""
        COPY public.{staging_table_name} ({', '.join(columns)})
        FROM '{s3_input_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET;
        """
        response_copy = redshift_data_client.execute_statement(
            Database=dbname, Sql=copy_sql, ClusterIdentifier=cluster_id, DbUser=db_user
        )
        query_id = response_copy['Id']
        logger.info(f"Executed COPY statement for {table_name} with Query ID: {query_id}")
        wait_for_query_completion(query_id, dbname, cluster_id, db_user)

        pkey_condition = " AND ".join([f"target.{col} = source.{col}" for col in pkey_columns])
        insert_columns = ", ".join(columns)
        insert_values = ", ".join([f"source.{col}" for col in columns])

        upsert_sql = f"""
        BEGIN;
        DELETE FROM public.{table_name} target USING public.{staging_table_name} source WHERE {pkey_condition};
        INSERT INTO public.{table_name} ({insert_columns}) SELECT {insert_values} FROM public.{staging_table_name} source;
        COMMIT;
        """
        response_upsert = redshift_data_client.execute_statement(
            Database=dbname, Sql=upsert_sql, ClusterIdentifier=cluster_id, DbUser=db_user
        )
        upsert_query_id = response_upsert['Id']
        logger.info(f"Executed UPSERT statement for {table_name} with Query ID: {upsert_query_id}")
        wait_for_query_completion(upsert_query_id, dbname, cluster_id, db_user)

        logger.info(f"Successfully loaded data to Redshift table {table_name}.")
        if ti:
            ti.xcom_push(key='data_processed', value=True)

    except Exception as e:
        logger.error(f"Error loading data to Redshift table {table_name}: {e}")
        if ti:
            ti.xcom_push(key='data_processed', value=False)
        raise

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

def load_users_to_dwh(input_path, conn_string, iam_role, **kwargs):
    """
    Orchestrates loading of user data from S3 to the Redshift data warehouse.
    Args:
        input_path (str): S3 URI of the processed user data (Parquet file).
        conn_string (str): Redshift connection string.
        iam_role (str): IAM role ARN for Redshift to access S3.
        **kwargs: Arbitrary keyword arguments.
    """
    logger.info("Loading users to Redshift")
    columns = ['user_id', 'user_name', 'user_age', 'user_country', 'created_at']
    load_to_redshift('users', 'staging_users', input_path, columns, ['user_id'], conn_string, iam_role, **kwargs)

def load_songs_to_dwh(input_path, conn_string, iam_role, **kwargs):
    """
    Orchestrates loading of songs data from S3 to the Redshift data warehouse.
    Args:
        input_path (str): S3 URI of the processed songs data (Parquet file).
        conn_string (str): Redshift connection string.
        iam_role (str): IAM role ARN for Redshift to access S3.
        **kwargs: Arbitrary keyword arguments.
    """
    logger.info("Loading songs to Redshift")
    columns = [
        'track_id', 'artists', 'album_name', 'track_name', 'popularity', 'duration_ms',
        'explicit', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness',
        'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature', 'track_genre'
    ]
    load_to_redshift('songs', 'staging_songs', input_path, columns, ['track_id'], conn_string, iam_role, **kwargs)

def load_streaming_to_dwh(input_path, conn_string, iam_role, **kwargs):
    """
    Orchestrates loading of streaming data from S3 to the Redshift data warehouse.
    Args:
        input_path (str): S3 URI of the processed streaming data (Parquet file).
        conn_string (str): Redshift connection string.
        iam_role (str): IAM role ARN for Redshift to access S3.
        **kwargs: Arbitrary keyword arguments.
    """
    logger.info("Loading streaming to Redshift")
    columns = ['user_id', 'song_id', 'listen_time']
    load_to_redshift('streaming', 'staging_streaming', input_path, columns, ['user_id', 'song_id', 'listen_time'], conn_string, iam_role, **kwargs)
