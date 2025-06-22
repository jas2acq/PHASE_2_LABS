# Music Streaming ETL Pipeline Documentation (AWS)

This document provides step-by-step instructions for setting up and running the music streaming ETL pipeline defined in the Airflow DAG `music_streaming_etl_pipeline` within an AWS environment. The pipeline leverages AWS services for data storage, processing, and orchestration.

## Overview

The pipeline processes music streaming data, including user profiles, song metadata, and streaming events, using the following AWS services:
- **Amazon S3**: Stores raw data, processed data, KPIs, logs, and state files.
- **Amazon Redshift**: Serves as the data warehouse for analytics.
- **AWS SSM Parameter Store**: Manages configuration parameters securely.
- **AWS Managed Workflows for Apache Airflow (MWAA)**: Orchestrates the ETL pipeline (optional, can use local Airflow).
- **AWS IAM**: Secures access to AWS resources.

The pipeline:
- Extracts and cleans raw data from S3.
- Validates the cleaned data.
- Loads processed data into Redshift.
- Computes Key Performance Indicators (KPIs) and saves them to S3.

## Prerequisites

- **AWS Account** with permissions to create and manage:
  - S3 buckets.
  - Redshift clusters.
  - SSM Parameter Store parameters.
  - IAM roles and policies.
  - MWAA environments (if using managed Airflow).
- **AWS CLI** configured with appropriate credentials (`aws configure`).
- **Python 3.8+** for local development or testing.
- **Git** for version control (optional).
- **IAM User/Role** with permissions for MWAA, S3, Redshift, and SSM.

## Setup Instructions

### 1. Configure AWS Resources

1. **Create S3 Buckets**:
   - Create buckets with unique names for:
     - Raw data: `s3://<your-bucket>-raw/`
     - Processed data: `s3://<your-bucket>-processed/`
     - KPI outputs: `s3://<your-bucket>-kpi/`
     - Logs: `s3://<your-bucket>-logs/`
     - State files: `s3://<your-bucket>-state/`
   - Enable **versioning** and **server-side encryption** (SSE-S3 or SSE-KMS) for security.
   - Restrict public access via bucket policies.
   - Example bucket policy for Airflow and Redshift access:
     ```json
     {
       "Version": "2012-10-17",
       "Statement": [
         {
           "Effect": "Allow",
           "Principal": {
             "AWS": [
               "arn:aws:iam::<account-id>:role/<airflow-role>",
               "arn:aws:iam::<account-id>:role/<redshift-role>"
             ]
           },
           "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
           "Resource": [
             "arn:aws:s3:::<your-bucket>-*",
             "arn:aws:s3:::<your-bucket>-*/*"
           ]
         }
       ]
     }
     ```

2. **Set Up Redshift Cluster**:
   - Launch a Redshift cluster in a VPC with private subnets.
     - Choose an appropriate node type (e.g., `dc2.large`) based on data volume.
     - Enable **encryption** and **enhanced VPC routing**.
   - Note the cluster endpoint, database name, and master username/password.
   - Create an IAM role (`<redshift-role>`) with:
     - Policy: `AmazonS3ReadOnlyAccess` (or a custom policy for specific buckets).
     - Trust relationship: Allow `redshift.amazonaws.com`.
   - Attach the IAM role to the Redshift cluster.
   - Ensure the Redshift security group allows inbound traffic from the Airflow environment (e.g., MWAA VPC).

3. **Configure SSM Parameter Store**:
   - Store parameters under `/ms-config/dev/` with secure strings for sensitive data (e.g., Redshift credentials).
   - Required parameters:
     - `/ms-config/dev/data/raw`: `s3://<your-bucket>-raw/`
     - `/ms-config/dev/data/processed`: `s3://<your-bucket>-processed/`
     - `/ms-config/dev/data/rds_source`: `s3://<your-bucket>-raw/rds-source/`
     - `/ms-config/dev/data/batch_source`: `s3://<your-bucket>-raw/batch-source/`
     - `/ms-config/dev/data/rds_processed`: `s3://<your-bucket>-processed/rds-processed/`
     - `/ms-config/dev/data/batch_processed`: `s3://<your-bucket>-processed/batch-processed/`
     - `/ms-config/dev/data/kpi`: `s3://<your-bucket>-kpi/`
     - `/ms-config/dev/state/dir`: `s3://<your-bucket>-state/`
     - `/ms-config/dev/log/dir`: `s3://<your-bucket>-logs/`
     - `/ms-config/dev/redshift/conn`: `redshift+psycopg2://awsuser:<password>@<cluster-endpoint>:5439/<dbname>`
     - `/ms-config/dev/redshift/role`: `arn:aws:iam::<account-id>:role/<redshift-role>`
   - Grant read access to the Airflow execution role:
     ```json
     {
       "Version": "2012-10-17",
       "Statement": [
         {
           "Effect": "Allow",
           "Action": ["ssm:GetParameter", "ssm:GetParameters"],
           "Resource": "arn:aws:ssm:<region>:<account-id>:parameter/ms-config/dev/*"
         }
       ]
     }
     ```

4. **Upload Sample Data**:
   - Upload sample CSV files to:
     - `s3://<your-bucket>-raw/rds-source/users.csv`
     - `s3://<your-bucket>-raw/rds-source/songs.csv`
     - `s3://<your-bucket>-raw/batch-source/streams*.csv`
   - Ensure files match the schema defined in `validate_data.py`:
     - `users.csv`: `user_id`, `user_name`, `user_age`, `user_country`, `created_at`
     - `songs.csv`: `track_id`, `artists`, `album_name`, `track_name`, `popularity`, `duration_ms`, etc.
     - `streams*.csv`: `user_id`, `track_id`, `listen_time`

### 2. Set Up Airflow (AWS MWAA or Local)

#### Option 1: AWS Managed Workflows for Apache Airflow (MWAA)
1. **Create an MWAA Environment**:
   - In the AWS Console, create an MWAA environment:
     - Name: e.g., `music-streaming-airflow`
     - Airflow version: 2.6+ (compatible with the DAG).
     - S3 bucket for DAGs: `s3://<your-bucket>-dags/`
     - Execution role with permissions for S3, SSM, Redshift, and CloudWatch.
   - Configure networking:
     - Place MWAA in the same VPC as Redshift.
     - Ensure security groups allow communication between MWAA and Redshift (port 5439).
   - Enable CloudWatch logging for scheduler, task, and webserver logs.

2. **Upload Code to S3**:
   - Organize the code in the DAGs bucket:
     ```
     s3://<your-bucket>-dags/
     ├── dags/
     │   ├── music_streaming_pipeline.py
     │   └── src/
     │       ├── batch_simulate.py
     │       ├── cleaning.py
     │       ├── config.py
     │       ├── extract_batch_streams.py
     │       ├── kpi_computation.py
     │       ├── load.py
     │       ├── logging_config.py
     │       └── validate_data.py
     ```
   - Upload using AWS CLI:
     ```bash
     aws s3 sync ./dags s3://<your-bucket>-dags/dags/
     ```

3. **Install Dependencies**:
   - Create a `requirements.txt`:
     ```
     apache-airflow==2.6.3
     pandas==1.5.3
     boto3==1.26.0
     pyarrow==10.0.1
     python-dotenv==0.21.0
     psutil==5.9.4
     ```
   - Upload to the MWAA S3 bucket:
     ```bash
     aws s3 cp requirements.txt s3://<your-bucket>-dags/requirements.txt
     ```
   - Update the MWAA environment to use the requirements file.

4. **Access MWAA**:
   - Once the environment is active, access the Airflow UI via the provided URL.
   - Log in using AWS SSO or IAM credentials.

#### Option 2: Local Airflow
1. **Install Airflow**:
   ```bash
   pip install apache-airflow==2.6.3
   airflow db init
   ```

2. **Place Code**:
   - Copy the Python files to the Airflow DAGs folder (e.g., `~/airflow/dags/`).
   - Maintain the structure:
     ```
     dags/
     ├── music_streaming_pipeline.py
     └── src/
         ├── batch_simulate.py
         ├── cleaning.py
         ├── config.py
         ├── extract_batch_streams.py
         ├── kpi_computation.py
         ├── load.py
         ├── logging_config.py
         └── validate_data.py
     ```

3. **Configure AWS Credentials**:
   - Ensure the AWS CLI is configured or set environment variables:
     ```bash
     export AWS_ACCESS_KEY_ID=<your-key>
     export AWS_SECRET_ACCESS_KEY=<your-secret>
     export AWS_DEFAULT_REGION=<your-region>
     ```

4. **Start Airflow**:
   ```bash
   airflow scheduler &
   airflow webserver -p 8080
   ```
   - Access the UI at `http://localhost:8080`.

### 3. Configure Batch Simulation (Optional)

The `batch_simulate.py` script generates synthetic streaming data for testing.

1. **Create `.env` File**:
   - In the script’s directory:
     ```
     BATCH_SOURCE_DIRECTORY=/local/path/to/csv/files
     S3_BUCKET_NAME=<your-bucket>-raw
     STREAM_TIME_LIMIT_SECONDS=3600
     CSV_BATCH_INTERVAL_SECONDS=10
     RECORDS_PER_BATCH=1000
     ```

2. **Run the Script**:
   ```bash
   python batch_simulate.py
   ```
   - Output: Batched CSV files in `s3://<your-bucket>-raw/YYYY/MM/DD/batch_streams_*.csv`.

### 4. Run the Airflow DAG

1. **Enable the DAG**:
   - In the Airflow UI, toggle `music_streaming_etl_pipeline` to "On".

2. **Trigger the DAG**:
   - UI: Click "Trigger DAG".
   - CLI:
     ```bash
     airflow dags trigger -d music_streaming_etl_pipeline
     ```

3. **Monitor Execution**:
   - **MWAA**: Check CloudWatch logs under `/aws/mwaa/<environment-name>`.
   - **Local**: View logs in `~/airflow/logs/`.
   - Pipeline logs are also saved to `s3://<your-bucket>-logs/`.

### 5. Pipeline Workflow

The DAG performs:
1. **Extract and Clean Users Data**:
   - Source: `s3://<your-bucket>-raw/rds-source/users.csv`
   - Output: `s3://<your-bucket>-processed/rds-processed/users.{csv,parquet}`
2. **Validate Users Data**:
   - Ensures required columns are present.
3. **Extract and Clean Songs Data**:
   - Source: `s3://<your-bucket>-raw/rds-source/songs.csv`
   - Output: `s3://<your-bucket>-processed/rds-processed/songs.{csv,parquet}`
4. **Validate Songs Data**:
   - Checks schema.
5. **Extract and Clean Streaming Data**:
   - Source: `s3://<your-bucket>-raw/batch-source/streams*.csv`
   - Output: `s3://<your-bucket>-processed/batch-processed/streaming_combined_<date>.parquet`
6. **Validate Streaming Data**:
   - Verifies schema.
7. **Create Redshift Tables**:
   - Tables: `users`, `songs`, `streaming`, `staging_*`.
8. **Load Data to Redshift**:
   - Uses COPY and UPSERT via staging tables.
9. **Compute KPIs**:
   - Outputs: `s3://<your-bucket>-kpi/genres/` and `s3://<your-bucket>-kpi/hours/`.

## AWS Best Practices

- **Security**:
  - Use least privilege IAM policies.
  - Rotate Redshift credentials regularly via AWS Secrets Manager.
  - Enable S3 bucket encryption and block public access.
- **Scalability**:
  - Scale Redshift nodes based on data volume.
  - Use MWAA’s auto-scaling for high task concurrency.
  - Partition S3 data by date for efficient access.
- **Monitoring**:
  - Set up CloudWatch alarms for Redshift CPU/memory usage.
  - Monitor MWAA task failures via CloudWatch Logs Insights.
  - Use S3 inventory reports to track data growth.
- **Cost Optimization**:
  - Use Redshift’s pause/resume feature for non-production environments.
  - Enable S3 lifecycle policies to archive old logs/state files.
  - Choose cost-effective MWAA environment sizes.

## Troubleshooting

- **DAG Import Errors**:
  - Verify `requirements.txt` dependencies in MWAA.
  - Check S3 DAG folder sync status.
- **S3 Permission Issues**:
  - Validate IAM role policies for Airflow and Redshift.
  - Ensure bucket names match SSM parameters.
- **Redshift Connection Failures**:
  - Check VPC security group rules.
  - Confirm Redshift IAM role is attached.
- **Data Validation Failures**:
  - Inspect input CSVs for schema mismatches.
  - Review S3 logs in `s3://<your-bucket>-logs/`.
- **KPI Task OOM Errors**:
  - Increase MWAA worker size or adjust `chunk_size` in `kpi_computation.py`.

## Maintenance

- **Log Retention**: Configure S3 lifecycle rules to archive logs after 30 days.
- **State Management**: Clear `s3://<your-bucket>-state/` to reset processing if needed.
- **Redshift Maintenance**: Run `ANALYZE` and `VACUUM` periodically.
- **Pipeline Updates**: Sync DAG changes to S3 and update MWAA.

