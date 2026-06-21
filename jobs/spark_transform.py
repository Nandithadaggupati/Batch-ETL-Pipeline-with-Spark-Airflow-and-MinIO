import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum, when, hour, year, month, dayofmonth, 
    to_date, sha2, concat_ws, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

def create_spark_session():
    # Fetch MinIO config from environment or default to docker compose hostname
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    
    spark = SparkSession.builder \
        .appName("Medallion-ETL-Spark-Job") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()
        
    return spark

def process_bronze_to_silver(spark, date_str):
    print(f"--- Processing Bronze to Silver for date: {date_str} ---")
    
    # Parse date components
    # date_str format: YYYY-MM-DD
    parts = date_str.split("-")
    yr, mn, dy = parts[0], parts[1], parts[2]
    
    bronze_path = f"s3a://bronze/year={yr}/month={mn}/day={dy}/events.json"
    silver_path = "s3a://silver/"
    
    print(f"Reading from: {bronze_path}")
    
    # Define raw schema to enforce types on read
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("device", StringType(), True),
        StructField("ip_address", StringType(), True)
    ])
    
    # Read raw JSON
    df_raw = spark.read.schema(schema).json(bronze_path)
    
    # Clean and transform
    # 1. Filter out records missing critical identifiers
    df_cleaned = df_raw.filter(
        col("event_id").isNotNull() & col("user_id").isNotNull()
    )
    
    # 2. Cast timestamp string to proper TimestampType
    df_transformed = df_cleaned.withColumn("timestamp", col("timestamp").cast(TimestampType()))
    
    # 3. Fill nulls for metadata
    df_transformed = df_transformed.na.fill({
        "product_id": "unknown",
        "category": "unknown",
        "price": 0.0
    })
    
    # 4. Enrich with derived columns
    df_enriched = df_transformed \
        .withColumn("hour", hour(col("timestamp"))) \
        .withColumn("session_id", sha2(concat_ws("-", col("user_id"), col("ip_address"), to_date(col("timestamp"))), 256)) \
        .withColumn("year", lit(yr)) \
        .withColumn("month", lit(mn)) \
        .withColumn("day", lit(dy))
        
    # Write to Silver layer in Parquet, partitioned by date (year/month/day)
    print(f"Writing Silver Parquet to: {silver_path}")
    df_enriched.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(silver_path)
        
    print("Bronze to Silver processing completed.")

def process_silver_to_gold(spark, date_str):
    print(f"--- Processing Silver to Gold for date: {date_str} ---")
    
    parts = date_str.split("-")
    yr, mn, dy = parts[0], parts[1], parts[2]
    
    silver_path = f"s3a://silver/year={yr}/month={mn}/day={dy}/"
    
    print(f"Reading from Silver path: {silver_path}")
    df_silver = spark.read.parquet(silver_path)
    
    # Check if empty
    if df_silver.count() == 0:
        print("No records found in Silver layer for this date.")
        return
        
    # Add date column for aggregates
    df_silver = df_silver.withColumn("date", to_date(col("timestamp")))
    
    # 1. Daily Active Users aggregate
    print("Computing Daily Active Users...")
    df_dau = df_silver.groupBy("date").agg(
        countDistinct("user_id").alias("unique_users"),
        count("event_id").alias("total_events")
    ).withColumn("year", lit(yr)) \
     .withColumn("month", lit(mn)) \
     .withColumn("day", lit(dy))
     
    # 2. Product Sales aggregate (only purchases)
    print("Computing Product Sales...")
    df_purchases = df_silver.filter(col("event_type") == "purchase")
    df_sales = df_purchases.groupBy("date", "product_id", "category").agg(
        count("event_id").alias("units_sold"),
        sum("price").alias("revenue")
    ).withColumn("year", lit(yr)) \
     .withColumn("month", lit(mn)) \
     .withColumn("day", lit(dy))
     
    # 3. Funnel & Device Metrics
    print("Computing Funnel Metrics...")
    df_funnel = df_silver.groupBy("date", "device").agg(
        count(when(col("event_type") == "page_view", 1)).alias("page_views"),
        count(when(col("event_type") == "click", 1)).alias("clicks"),
        count(when(col("event_type") == "add_to_cart", 1)).alias("adds_to_cart"),
        count(when(col("event_type") == "purchase", 1)).alias("purchases")
    )
    
    # Calculate conversion rate: purchases / page_views
    df_funnel = df_funnel.withColumn(
        "conversion_rate",
        when(col("page_views") > 0, col("purchases").cast(DoubleType()) / col("page_views").cast(DoubleType()))
        .otherwise(0.0)
    ).withColumn("year", lit(yr)) \
     .withColumn("month", lit(mn)) \
     .withColumn("day", lit(dy))
     
    # Write to Gold Layer
    gold_base = "s3a://gold"
    
    print(f"Writing Gold: Daily Active Users to {gold_base}/daily_active_users/")
    df_dau.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(f"{gold_base}/daily_active_users/")
        
    print(f"Writing Gold: Product Sales to {gold_base}/product_sales/")
    df_sales.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(f"{gold_base}/product_sales/")
        
    print(f"Writing Gold: Funnel Metrics to {gold_base}/funnel_metrics/")
    df_funnel.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(f"{gold_base}/funnel_metrics/")
        
    print("Silver to Gold processing completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD format")
    args = parser.parse_args()
    
    # Initialize Spark
    spark_session = create_spark_session()
    
    try:
        # Run Medallion stages
        process_bronze_to_silver(spark_session, args.date)
        process_silver_to_gold(spark_session, args.date)
    finally:
        # Stop Spark Session
        spark_session.stop()
        print("Spark Session stopped successfully.")
