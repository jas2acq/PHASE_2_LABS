# ======================================================================================
# config.py
# This file handles configuration loading from AWS SSM Parameter Store.
# ======================================================================================
import boto3
import re
from src.logging_config import setup_logger
from botocore.exceptions import ClientError

# Set up logger for the config module
logger = setup_logger('config', mode='a')

# Function to retrieve a parameter from AWS SSM Parameter Store
def get_ssm_parameter(parameter_name):
    """
    Retrieves a parameter value from AWS SSM Parameter Store.
    Args:
        parameter_name (str): The name of the parameter to retrieve.
    Returns:
        str: The value of the parameter.
    Raises:
        ValueError: If the parameter is not found or has an invalid S3 path format.
        ClientError: If an AWS client-related error occurs during retrieval.
    """
    ssm_client = boto3.client('ssm')  # Fixed: Renamed from s3_client to ssm_client
    try:
        response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
        value = response['Parameter']['Value']
        if parameter_name in ['/ms-config/dev/data/raw', '/ms-config/dev/data/processed',
                             '/ms-config/dev/data/kpi', '/ms-config/dev/log/dir', '/ms-config/dev/state/dir']:
            if not re.match(r'^s3://[\w\-./]+$', value):
                raise ValueError(f"Invalid S3 path format for {parameter_name}: {value}")
        return value
    except ClientError as e:
        if e.response['Error']['Code'] == 'ParameterNotFound':
            logger.error(f"SSM parameter not found: {parameter_name}")
            raise ValueError(f"Required configuration parameter not found in SSM: {parameter_name}")
        logger.error(f"AWS ClientError retrieving parameter {parameter_name}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error retrieving parameter {parameter_name}: {e}")
        raise

# Function to get all required configuration parameters
def get_config():
    """
    Retrieves all required configuration parameters from AWS SSM Parameter Store
    and constructs a comprehensive configuration dictionary.
    Returns:
        dict: A dictionary containing all resolved configuration parameters.
    Raises:
        ValueError: If any required configuration parameter cannot be retrieved or is invalid.
    """
    config_map = {
        'DATA_RAW_DIR': '/ms-config/dev/data/raw',
        'DATA_PROCESSED_DIR': '/ms-config/dev/data/processed',
        'RDS_SOURCE_DIR': '/ms-config/dev/data/rds_source',
        'BATCH_SOURCE_DIR': '/ms-config/dev/data/batch_source',
        'RDS_PROCESSED_DIR': '/ms-config/dev/data/rds_processed',
        'BATCH_PROCESSED_DIR': '/ms-config/dev/data/batch_processed',
        'DATA_KPI_OUTPUTS_DIR': '/ms-config/dev/data/kpi',
        'STATE_DIR': '/ms-config/dev/state/dir',
        'LOG_DIR': '/ms-config/dev/log/dir',
        'DWH_CONN_STRING': '/ms-config/dev/redshift/conn',
        'REDSHIFT_IAM_ROLE': '/ms-config/dev/redshift/role',
    }
    config = {key: get_ssm_parameter(param_name) for key, param_name in config_map.items()}
    config['RDS_SOURCE_DIR'] = f"{config['DATA_RAW_DIR']}/{config['RDS_SOURCE_DIR'].split('/')[-1].rstrip('/').replace('_', '-')}"
    config['BATCH_SOURCE_DIR'] = f"{config['DATA_RAW_DIR']}/{config['BATCH_SOURCE_DIR'].split('/')[-1].rstrip('/').replace('_', '-')}"
    config['RDS_PROCESSED_DIR'] = f"{config['DATA_PROCESSED_DIR']}/{config['RDS_PROCESSED_DIR'].split('/')[-1].rstrip('/').replace('_', '-')}"
    config['BATCH_PROCESSED_DIR'] = f"{config['DATA_PROCESSED_DIR']}/{config['BATCH_PROCESSED_DIR'].split('/')[-1].rstrip('/').replace('_', '-')}"
    return config