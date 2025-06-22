# ======================================================================================
# logging_config.py
# This file provides utilities for setting up and configuring logging,
# including custom S3 handler for writing logs to S3.
# ======================================================================================
import logging
import boto3
from botocore.exceptions import ClientError
import time
import urllib.parse

def setup_logger(name, mode='a'):
    """
    Sets up a basic logger with a stream handler to output messages to the console.
    Args:
        name (str): The name of the logger.
        mode (str): The mode for file logging ('a' for append, 'w' for overwrite).
    Returns:
        logging.Logger: The configured logger instance.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

def add_s3_handler(logger, config):
    """
    Adds a custom S3 logging handler to an existing logger.
    Args:
        logger (logging.Logger): The logger instance to add the handler to.
        config (dict): A dictionary containing configuration, specifically 'LOG_DIR'.
    Raises:
        ValueError: If 'LOG_DIR' in config is not a valid S3 path.
        Exception: If there's an error setting up the S3 logging handler.
    """
    try:
        log_dir = config.get('LOG_DIR')
        if not log_dir or not log_dir.startswith('s3://'):
            raise ValueError("LOG_DIR must be a valid S3 path")

        parsed_log_dir = urllib.parse.urlparse(log_dir)
        bucket = parsed_log_dir.netloc
        prefix = parsed_log_dir.path.lstrip('/')
        if prefix and not prefix.endswith('/'):
            prefix += '/'

        timestamp = time.strftime('%Y%m%dT%H%M', time.gmtime())
        s3_handler = S3Handler(bucket, prefix, logger.name, timestamp, 'a')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        s3_handler.setFormatter(formatter)
        logger.addHandler(s3_handler)
    except Exception as e:
        logger.error(f"Failed to set up S3 logging: {e}")

class S3Handler(logging.Handler):
    """
    A custom logging handler that sends log records to an S3 bucket.
    """
    def __init__(self, bucket, prefix, logger_name, timestamp, mode='a'):
        """
        Initializes the S3Handler.
        Args:
            bucket (str): The S3 bucket name where logs will be stored.
            prefix (str): The S3 prefix (folder path) within the bucket.
            logger_name (str): The name of the logger associated with this handler.
            timestamp (str): A timestamp to be included in the log file name.
            mode (str): 'a' for append (default), 'w' for overwrite.
        """
        super().__init__()
        self.s3_client = boto3.client('s3')
        self.bucket = bucket
        self.prefix = prefix
        self.logger_name = logger_name
        self.timestamp = timestamp
        self.mode = mode

    def emit(self, record):
        """
        Emits a log record by formatting and uploading it to S3.
        Args:
            record (logging.LogRecord): The log record to be emitted.
        """
        try:
            log_msg = self.format(record)
            key = f"{self.prefix}{self.logger_name}_{self.timestamp}.log"

            if self.mode == 'a':
                try:
                    obj = self.s3_client.get_object(Bucket=self.bucket, Key=key)
                    existing_log = obj['Body'].read().decode('utf-8')
                    log_msg = existing_log + '\n' + log_msg
                except ClientError as e:
                    if e.response['Error']['Code'] != 'NoSuchKey':
                        raise

            self.s3_client.put_object(Bucket=self.bucket, Key=key, Body=log_msg.encode('utf-8'))
        except Exception as e:
            print(f"Error sending log to S3: {e}")
            self.handleError(record)
