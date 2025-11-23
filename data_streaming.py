import json
import pandas as pd
from confluent_kafka import Producer
import time
from pathlib import Path

KAFKA_BROKER = "localhost:9092"
LIDAR_TOPIC = "lidar-stream"
METADATA_TOPIC = "lidar-metadata"

producer = Producer({"bootstrap.servers": KAFKA_BROKER})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered: {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

def stream_parquet_to_kafka(parquet_path, topic, delay=0.001):
    """Stream parquet file to Kafka topic"""
    try:
        df = pd.read_parquet(parquet_path)
        print(f"Streaming {len(df)} records from {Path(parquet_path).name} to topic '{topic}'...")
        
        for idx, row in df.iterrows():
            msg = row.to_dict()
            producer.produce(
                topic,
                value=json.dumps(msg).encode("utf-8"),
                callback=delivery_report
            )
            producer.poll(0)
            
            if (idx + 1) % 1000 == 0:
                print(f"  Streamed {idx + 1} records...")
            
            time.sleep(delay)  # simulate streaming sensor data
        
        producer.flush()
        print(f"✓ Successfully streamed {len(df)} records from {Path(parquet_path).name}\n")
    except Exception as e:
        print(f"✗ Error streaming {parquet_path}: {str(e)}\n")

def stream_all_parquet_files(parquet_dir):
    """Stream all parquet files from directory to Kafka"""
    parquet_path = Path(parquet_dir)
    
    # Find all parquet files
    parquet_files = sorted(list(parquet_path.glob("*.parquet")))
    
    if not parquet_files:
        print(f"No parquet files found in {parquet_dir}")
        return
    
    print(f"Found {len(parquet_files)} parquet file(s)\n")
    
    # Stream LiDAR point cloud data
    lidar_files = [f for f in parquet_files if not any(x in f.name for x in ['metadata', 'colorscale'])]
    if lidar_files:
        print(f"--- Streaming LiDAR Data ({len(lidar_files)} files) ---")
        for pf in lidar_files:
            stream_parquet_to_kafka(str(pf), LIDAR_TOPIC, delay=0.001)
    
    # Stream metadata files
    metadata_files = [f for f in parquet_files if 'metadata' in f.name or 'colorscale' in f.name]
    if metadata_files:
        print(f"--- Streaming Metadata ({len(metadata_files)} files) ---")
        for pf in metadata_files:
            stream_parquet_to_kafka(str(pf), METADATA_TOPIC, delay=0.01)
    
    print("✓ All files streamed successfully!")

if __name__ == "__main__":
    stream_all_parquet_files("parquet_output")
