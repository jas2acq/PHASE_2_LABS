import os
import csv
import time
import sys
from datetime import datetime
from dotenv import load_dotenv
import boto3
import logging

# Add the parent directory to the system path to allow imports from src
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Configure logging to a local file and console for immediate feedback
# This setup is for local execution visibility. For MWAA, the add_s3_handler
# function (from logging_config.py) will direct logs to S3.
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
log_file = os.path.join(log_dir, 'batch_simulate.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
log = logging.getLogger('batch_simulate')

# Load environment variables from .env file
load_dotenv()

def validate_env_vars():
    """
    Validates the presence and type of required environment variables.
    Exits the program if any variable is missing or has an invalid type.
    """
    required_vars = {
        'BATCH_SOURCE_DIRECTORY': str,
        'S3_BUCKET_NAME': str,
        'STREAM_TIME_LIMIT_SECONDS': str,
        'CSV_BATCH_INTERVAL_SECONDS': int,
        'RECORDS_PER_BATCH': int
    }
    config = {}
    for var, var_type in required_vars.items():
        value = os.getenv(var)
        if value is None:
            log.error(f"Environment variable {var} is not set. Please set it and retry.")
            sys.exit(1)
        try:
            config[var] = var_type(value)
        except ValueError:
            log.error(f"Invalid value for {var}: '{value}'. Expected type: {var_type.__name__}.")
            sys.exit(1)
    return config

# Validate and load environment variables into configuration
config = validate_env_vars()
BATCH_SOURCE_DIRECTORY = config['BATCH_SOURCE_DIRECTORY']
S3_BUCKET_NAME = config['S3_BUCKET_NAME']
STREAM_TIME_LIMIT = config['STREAM_TIME_LIMIT_SECONDS']
CSV_BATCH_INTERVAL_SECONDS = config['CSV_BATCH_INTERVAL_SECONDS']
RECORDS_PER_BATCH = config['RECORDS_PER_BATCH']

# Define the path for the state file
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'state_files', 'last_processed_state_fresh.txt')

# Initialize S3 client
s3_client = boto3.client('s3')

def read_last_processed_state():
    """
    Reads the last processed state from a local file, if it exists.
    This allows the simulation to resume from where it left off.
    Returns:
        tuple: (filename, line_number, batch_count, state_was_valid)
    """
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    parts = content.split(',')
                    if len(parts) == 3:
                        filename, line_number, batch_count = parts
                        log.info(f"Resuming from file: '{filename}', line: {line_number}, batch count: {batch_count}.")
                        return filename, int(line_number), int(batch_count), True
                    else:
                        log.warning(f"Malformed state file content '{content}'. Expected format 'filename,line_number,batch_count'. Starting from scratch.")
                else:
                    log.info("State file is empty. Starting simulation from scratch.")
        except (ValueError, IndexError) as e:
            log.warning(f"Error reading state file content '{content}'. Error: {e}. Starting from scratch.")
        except IOError as e:
            log.error(f"IO error reading state file {STATE_FILE}: {e}. Starting from scratch.")
    else:
        log.info("No state file found. Starting simulation from scratch.")
    return None, 0, 0, False

def write_last_processed_state(filename, line_number, batch_count):
    """
    Writes the current processing state to a local file.
    Args:
        filename (str): The name of the file being processed.
        line_number (int): The last line number processed in the file.
        batch_count (int): The current batch count.
    """
    state_dir = os.path.dirname(STATE_FILE)
    if not os.path.exists(state_dir):
        try:
            os.makedirs(state_dir)
        except OSError as e:
            log.error(f"Error creating state directory {state_dir}: {e}")
            return # Exit function if directory creation fails
    try:
        with open(STATE_FILE, 'w', encoding='utf-8') as f:
            f.write(f"{filename},{line_number},{batch_count}")
        log.debug(f"Updated state: file '{filename}', line {line_number}, batch {batch_count}.")
    except IOError as e:
        log.error(f"IO error writing to state file {STATE_FILE}: {e}")
    except Exception as e:
        log.error(f"Unexpected error writing to state file {STATE_FILE}: {e}")

def delete_state_file():
    """Deletes the local state file."""
    if os.path.exists(STATE_FILE):
        try:
            os.remove(STATE_FILE)
            log.info(f"State file '{STATE_FILE}' deleted.")
        except OSError as e:
            log.error(f"Error deleting state file {STATE_FILE}: {e}. Please remove manually if necessary.")
        except Exception as e:
            log.error(f"Unexpected error deleting state file {STATE_FILE}: {e}")

def generate_stream_records(batch_dir_path):
    """
    Generates and uploads streaming records in batches to S3.
    It reads CSV files from a source directory, processes them in batches,
    and uploads each batch as a separate CSV file to S3.
    Args:
        batch_dir_path (str): The path to the directory containing source CSV files.
    """
    if not os.path.isdir(batch_dir_path):
        log.error(f"Batch source directory not found at '{batch_dir_path}'. Please check the path.")
        sys.exit(1) # Exit if source directory is not found

    try:
        all_files_in_source = os.listdir(batch_dir_path)
        streams_csv_files = sorted([f for f in all_files_in_source if f.startswith('streams') and f.endswith('.csv')])
        if not streams_csv_files:
            log.warning(f"No 'streams*.csv' files found in source directory: {batch_dir_path}. Exiting.")
            return # Exit if no relevant files are found
        log.info(f"Found {len(streams_csv_files)} streams files: {streams_csv_files}.")
    except OSError as e:
        log.error(f"Error listing files in batch source directory {batch_dir_path}: {e}")
        sys.exit(1)

    input_columns = ["user_id", "track_id", "listen_time"]
    last_file, last_line, last_batch_count, state_was_valid = read_last_processed_state()
    total_records_generated = 0
    start_time = time.time()

    for filename in streams_csv_files:
        file_path = os.path.join(batch_dir_path, filename)
        
        # Skip files already processed or until the last processed file is reached
        if last_file and filename < last_file and state_was_valid:
            log.info(f"Skipping already processed file: {filename}.")
            continue
        elif last_file and filename == last_file and state_was_valid:
            log.info(f"Resuming processing for file: {filename} from line {last_line}.")
            # Reset last_line for the next file if this one is fully processed from state
            file_start_line = last_line
            current_batch_count = last_batch_count
            last_file = None # Clear last_file so subsequent files don't trigger this condition
        else:
            file_start_line = 0
            current_batch_count = 0
            
        log.info(f"Processing file: {filename}.")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                csv_reader = csv.reader(f)
                header = next(csv_reader)  # Read header
                if header != input_columns:
                    log.error(f"Header mismatch in {filename}. Expected {input_columns}, got {header}. Skipping file.")
                    continue

                batch_records = []
                local_line_number = 0

                for i, row in enumerate(csv_reader):
                    local_line_number = i + 1 # +1 for 0-indexed to 1-indexed for logging
                    if local_line_number <= file_start_line:
                        continue # Skip lines already processed

                    if time.time() - start_time > int(STREAM_TIME_LIMIT):
                        log.info(f"Stream time limit of {STREAM_TIME_LIMIT} seconds reached. Stopping simulation.")
                        # Write the current state before exiting due to time limit
                        write_last_processed_state(filename, local_line_number, current_batch_count)
                        return

                    batch_records.append(row)
                    if len(batch_records) >= RECORDS_PER_BATCH:
                        output_key = f"raw/batch_streams/batch_{filename.replace('.csv', '')}_{current_batch_count}.csv"
                        local_temp_file = f"/tmp/batch_{current_batch_count}.csv"
                        
                        try:
                            with open(local_temp_file, 'w', newline='', encoding='utf-8') as temp_f:
                                csv_writer = csv.writer(temp_f)
                                csv_writer.writerow(header) # Write header to each batch file
                                csv_writer.writerows(batch_records)
                            
                            s3_client.upload_file(local_temp_file, S3_BUCKET_NAME, output_key)
                            log.info(f"Uploaded batch {current_batch_count} ({len(batch_records)} records) from '{filename}' to s3://{S3_BUCKET_NAME}/{output_key}")
                            total_records_generated += len(batch_records)
                            write_last_processed_state(filename, local_line_number, current_batch_count)
                            batch_records = []
                            current_batch_count += 1
                            time.sleep(CSV_BATCH_INTERVAL_SECONDS) # Pause to simulate streaming interval
                        except IOError as e:
                            log.error(f"IO error processing batch {current_batch_count} for file {filename}: {e}")
                        except boto3.exceptions.S3UploadFailedError as e:
                            log.error(f"S3 upload failed for batch {current_batch_count} to {output_key}: {e}")
                        except Exception as e:
                            log.error(f"Unexpected error during batch upload for batch {current_batch_count}: {e}")
                        finally:
                            if os.path.exists(local_temp_file):
                                try:
                                    os.remove(local_temp_file) # Clean up local temp file
                                except OSError as e:
                                    log.warning(f"Failed to remove temp file {local_temp_file}: {e}")
                                except Exception as e:
                                    log.warning(f"Unexpected error removing temp file {local_temp_file}: {e}")

                # Upload any remaining records in the last batch
                if batch_records:
                    output_key = f"raw/batch_streams/batch_{filename.replace('.csv', '')}_{current_batch_count}.csv"
                    local_temp_file = f"/tmp/batch_{current_batch_count}.csv"
                    try:
                        with open(local_temp_file, 'w', newline='', encoding='utf-8') as temp_f:
                            csv_writer = csv.writer(temp_f)
                            csv_writer.writerow(header)
                            csv_writer.writerows(batch_records)
                        
                        s3_client.upload_file(local_temp_file, S3_BUCKET_NAME, output_key)
                        log.info(f"Uploaded final batch {current_batch_count} ({len(batch_records)} records) from '{filename}' to s3://{S3_BUCKET_NAME}/{output_key}")
                        total_records_generated += len(batch_records)
                        write_last_processed_state(filename, local_line_number, current_batch_count) # Update state after final batch
                    except IOError as e:
                        log.error(f"IO error writing final batch for file {filename}: {e}")
                    except boto3.exceptions.S3UploadFailedError as e:
                        log.error(f"S3 upload failed for final batch to {output_key}: {e}")
                    except Exception as e:
                        log.error(f"Error uploading final batch to S3 {output_key}: {e}")
                    finally:
                        if os.path.exists(local_temp_file):
                            try:
                                os.remove(local_temp_file)
                            except OSError as e:
                                log.warning(f"Failed to remove temp file {local_temp_file}: {e}")
                            except Exception as e:
                                log.warning(f"Unexpected error removing temp file {local_temp_file}: {e}")

            # After successfully processing a file, delete the state file to indicate completion for that file
            if filename == streams_csv_files[-1] and not (time.time() - start_time > int(STREAM_TIME_LIMIT)):
                delete_state_file()
            
        except FileNotFoundError:
            log.error(f"File not found: {file_path}. Skipping.")
        except IOError as e:
            log.error(f"Error reading file {file_path}: {e}")
        except Exception as e:
            log.error(f"Error during stream simulation for file {filename}: {e}")
            
    log.info(f"Stream simulation finished. Total records generated: {total_records_generated}.")

if __name__ == "__main__":
    log.info(f"Starting batch simulation.")
    log.info(f"BATCH_SOURCE_DIRECTORY: {BATCH_SOURCE_DIRECTORY}")
    log.info(f"S3_BUCKET_NAME: {S3_BUCKET_NAME}")
    log.info(f"STREAM_TIME_LIMIT_SECONDS: {STREAM_TIME_LIMIT}")
    log.info(f"CSV_BATCH_INTERVAL_SECONDS: {CSV_BATCH_INTERVAL_SECONDS}")
    log.info(f"RECORDS_PER_BATCH: {RECORDS_PER_BATCH}")
    
    try:
        generate_stream_records(BATCH_SOURCE_DIRECTORY)
    except Exception as e:
        log.critical(f"An unhandled error occurred during batch simulation: {e}", exc_info=True)
        sys.exit(1)