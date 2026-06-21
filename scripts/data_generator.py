import argparse
import datetime
import json
import os
import random
import uuid
import boto3
from botocore.client import Config

def generate_events(date_str, count):
    # Parse date
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    
    # Pre-defined categories and products
    categories = {
        "electronics": ["prod_laptop", "prod_phone", "prod_headphones", "prod_monitor", "prod_keyboard"],
        "apparel": ["prod_shirt", "prod_jeans", "prod_jacket", "prod_sneakers", "prod_socks"],
        "home": ["prod_blender", "prod_coffee_maker", "prod_lamp", "prod_cushion", "prod_vacuum"],
        "books": ["prod_novel", "prod_biography", "prod_textbook", "prod_comic", "prod_cookbook"],
        "beauty": ["prod_lipstick", "prod_perfume", "prod_shampoo", "prod_moisturizer", "prod_mascara"]
    }
    
    product_prices = {
        "prod_laptop": 999.99, "prod_phone": 699.99, "prod_headphones": 149.99, "prod_monitor": 249.99, "prod_keyboard": 79.99,
        "prod_shirt": 24.99, "prod_jeans": 49.99, "prod_jacket": 89.99, "prod_sneakers": 79.99, "prod_socks": 9.99,
        "prod_blender": 39.99, "prod_coffee_maker": 59.99, "prod_lamp": 29.99, "prod_cushion": 19.99, "prod_vacuum": 129.99,
        "prod_novel": 14.99, "prod_biography": 19.99, "prod_textbook": 99.99, "prod_comic": 9.99, "prod_cookbook": 24.99,
        "prod_lipstick": 19.99, "prod_perfume": 59.99, "prod_shampoo": 12.99, "prod_moisturizer": 24.99, "prod_mascara": 14.99
    }
    
    event_types = ["page_view", "click", "add_to_cart", "purchase"]
    # Funnel weights: page_view (60%), click (25%), add_to_cart (12%), purchase (3%)
    event_weights = [0.60, 0.25, 0.12, 0.03]
    
    devices = ["desktop", "mobile", "tablet"]
    device_weights = [0.45, 0.45, 0.10]
    
    # Write to a temporary local file
    local_filename = f"temp_events_{date_str}.json"
    print(f"Generating {count} events for {date_str} to {local_filename}...")
    
    # Generate and write in chunks to be memory efficient
    chunk_size = 50000
    with open(local_filename, "w") as f:
        for chunk_idx in range(0, count, chunk_size):
            current_chunk_size = min(chunk_size, count - chunk_idx)
            lines = []
            for _ in range(current_chunk_size):
                # Random timestamp inside the specified date
                seconds_offset = random.randint(0, 86399)
                event_time = dt + datetime.timedelta(seconds=seconds_offset)
                
                # Simulate ~80,000 distinct users to ensure aggregate activity
                user_id = f"user_{random.randint(1, 80000)}"
                event_type = random.choices(event_types, weights=event_weights, k=1)[0]
                device = random.choices(devices, weights=device_weights, k=1)[0]
                
                # Product details
                category = random.choice(list(categories.keys()))
                product = random.choice(categories[category])
                price = product_prices[product]
                
                # IP address
                ip = f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
                
                event = {
                    "event_id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "event_type": event_type,
                    "timestamp": event_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                    "product_id": product,
                    "category": category,
                    "price": price,
                    "device": device,
                    "ip_address": ip
                }
                lines.append(json.dumps(event) + "\n")
            f.writelines(lines)
            
    print(f"Generation complete. Uploading to MinIO...")
    
    # MinIO configuration
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    
    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        config=Config(signature_version="s3v4")
    )
    
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")
    s3_key = f"year={year}/month={month}/day={day}/events.json"
    
    s3.upload_file(local_filename, "bronze", s3_key)
    print(f"Uploaded {local_filename} to s3://bronze/{s3_key}")
    
    # Remove local temp file
    os.remove(local_filename)
    print("Cleaned up local temporary file.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD format")
    parser.add_argument("--count", type=int, default=1000000, help="Number of events to generate")
    args = parser.parse_args()
    
    generate_events(args.date, args.count)
