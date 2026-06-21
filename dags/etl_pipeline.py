import datetime
import os
import boto3
import pandas as pd
import pyarrow.parquet as pq
import s3fs
from botocore.client import Config

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=2),
}

def get_minio_client():
    """Helper to get MinIO details from Airflow connection"""
    conn = BaseHook.get_connection("minio_conn")
    endpoint_url = conn.extra_dejson.get("endpoint_url", "http://minio:9000")
    return {
        "endpoint_url": endpoint_url,
        "access_key": conn.login,
        "secret_key": conn.password
    }

def run_data_quality_check(logical_date, **context):
    """
    Data Quality Check:
    1. Verify Bronze raw events file exists and contains > 0 rows.
    2. Verify Silver Parquet dataset exists and contains > 0 rows.
    3. Verify Silver row count is at least 95% of Bronze row count (allowing minor drops for invalid records).
    """
    dt = datetime.datetime.strptime(logical_date, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")
    
    minio_info = get_minio_client()
    
    # 1. Check Bronze raw event file row count
    s3_client = boto3.client(
        "s3",
        endpoint_url=minio_info["endpoint_url"],
        aws_access_key_id=minio_info["access_key"],
        aws_secret_access_key=minio_info["secret_key"],
        config=Config(signature_version="s3v4")
    )
    
    bronze_key = f"year={year}/month={month}/day={day}/events.json"
    print(f"Checking Bronze layer: bucket=bronze, key={bronze_key}")
    
    try:
        response = s3_client.get_object(Bucket="bronze", Key=bronze_key)
        # Read lines to count records
        bronze_count = sum(1 for _ in response["Body"].iter_lines())
    except Exception as e:
        raise ValueError(f"Bronze raw file not found or unreadable for date {logical_date}. Error: {e}")
        
    print(f"Bronze Row Count: {bronze_count}")
    
    # 2. Check Silver Parquet dataset row count using metadata (extremely fast)
    fs = s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": minio_info["endpoint_url"]},
        key=minio_info["access_key"],
        secret=minio_info["secret_key"]
    )
    
    silver_prefix = f"silver/year={year}/month={month}/day={day}/"
    print(f"Checking Silver layer: {silver_prefix}")
    
    try:
        dataset = pq.ParquetDataset(silver_prefix, filesystem=fs)
        silver_count = sum(fragment.metadata.num_rows for fragment in dataset.fragments)
    except Exception as e:
        raise ValueError(f"Silver Parquet files not found or empty for date {logical_date}. Error: {e}")
        
    print(f"Silver Row Count: {silver_count}")
    
    # 3. Validation Logic
    if bronze_count == 0:
        raise ValueError("Bronze dataset is empty.")
    if silver_count == 0:
        raise ValueError("Silver dataset is empty.")
        
    match_pct = (silver_count / bronze_count) * 100
    print(f"Silver to Bronze Record Match: {match_pct:.2f}%")
    
    if match_pct < 95.0:
        raise ValueError(
            f"Data Quality Check FAILED: Silver row count ({silver_count}) "
            f"is below 95% of Bronze row count ({bronze_count})."
        )
        
    print("Data Quality Check PASSED successfully!")


def load_gold_to_postgres(logical_date, **context):
    """
    Loads Gold layer aggregations from MinIO to Postgres database.
    Ensures idempotency by deleting any pre-existing rows for the logical_date.
    """
    dt = datetime.datetime.strptime(logical_date, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")
    
    minio_info = get_minio_client()
    storage_options = {
        "key": minio_info["access_key"],
        "secret": minio_info["secret_key"],
        "client_kwargs": {"endpoint_url": minio_info["endpoint_url"]}
    }
    
    gold_tables = {
        "daily_active_users": f"s3://gold/daily_active_users/year={year}/month={month}/day={day}/",
        "product_sales": f"s3://gold/product_sales/year={year}/month={month}/day={day}/",
        "funnel_metrics": f"s3://gold/funnel_metrics/year={year}/month={month}/day={day}/"
    }
    
    pg_hook = PostgresHook(postgres_conn_id="postgres_warehouse")
    engine = pg_hook.get_sqlalchemy_engine()
    
    for table_name, s3_path in gold_tables.items():
        print(f"Loading table '{table_name}' from {s3_path}")
        
        # Read from MinIO
        try:
            df = pd.read_parquet(s3_path, storage_options=storage_options)
        except Exception as e:
            print(f"No Gold files found for table '{table_name}' at {s3_path}. Skipping. Error: {e}")
            continue
            
        if len(df) == 0:
            print(f"Table '{table_name}' has 0 rows. Skipping Postgres load.")
            continue
            
        # Standardize columns to match Postgres table definitions
        # (Remove Partition columns year, month, day if present)
        cols_to_drop = [c for c in ["year", "month", "day"] if c in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
            
        print(f"Rows to load into PostgreSQL '{table_name}': {len(df)}")
        
        # Ensure date column is formatted as YYYY-MM-DD
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.date
            
        # Execute load inside a transaction with deletion to guarantee idempotency
        with engine.begin() as conn:
            # Delete existing records for execution date
            delete_sql = f"DELETE FROM {table_name} WHERE date = '{logical_date}'"
            print(f"Running: {delete_sql}")
            conn.execute(delete_sql)
            
            # Write new data
            df.to_sql(table_name, con=conn, if_exists="append", index=False)
            print(f"Table '{table_name}' loaded successfully.")


with DAG(
    "batch_etl_pipeline",
    default_args=default_args,
    description="Batch ETL Pipeline with Spark, Airflow, and MinIO",
    schedule_interval="@daily",
    start_date=datetime.datetime(2026, 6, 15),
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1. Ingestion: Generate 1M events and upload to MinIO Bronze layer
    generate_raw_data = BashOperator(
        task_id="generate_raw_data",
        bash_command=(
            "python /opt/airflow/scripts/data_generator.py "
            "--date {{ ds }} --count 1000000"
        ),
    )

    # 2. Spark Job: Process Bronze -> Silver -> Gold
    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4 "
            "/opt/airflow/jobs/spark_transform.py --date {{ ds }}"
        ),
    )

    # 3. Data Quality Check: Run checks comparing Bronze and Silver row counts
    data_quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=run_data_quality_check,
        op_kwargs={"logical_date": "{{ ds }}"},
    )

    # 4. Data Warehouse Load: Insert Gold data to Postgres
    load_to_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_gold_to_postgres,
        op_kwargs={"logical_date": "{{ ds }}"},
    )

    # DAG Dependency Flow
    generate_raw_data >> spark_transform >> data_quality_check >> load_to_postgres
