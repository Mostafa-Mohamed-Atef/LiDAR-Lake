import json
import os
import time
import sys
from pathlib import Path
from collections import defaultdict

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ---------------------------------------------------------------------
# Kafka Configuration
# ---------------------------------------------------------------------
# Inside Docker network → kafka:9092 (set via KAFKA_BROKER env var)
# Outside Docker (host machine) → localhost:29092
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
if not KAFKA_BROKER:
    # Default to localhost for local development
    KAFKA_BROKER = "localhost:29092"
    print("[WARN] KAFKA_BROKER not set, using default: localhost:29092")

LIDAR_TOPIC = "lidar-stream"
METADATA_TOPIC = "lidar-metadata"
MAX_LIDAR_ROWS = int(os.getenv("MAX_LIDAR_ROWS", "500000"))

# Global counters for tracking
delivery_stats = {
    "delivered": 0,
    "failed": 0,
    "errors": []
}


# ---------------------------------------------------------------------
# Initialize Kafka Producer
# ---------------------------------------------------------------------
def create_producer():
    """Create and return a Kafka producer with proper configuration."""
    producer_config = {
        "bootstrap.servers": KAFKA_BROKER,
        "socket.timeout.ms": 10000,
        "connections.max.idle.ms": 540000,
        "retry.backoff.ms": 1000,
        "max.in.flight.requests.per.connection": 1,
        "queue.buffering.max.messages": 100000,  # Limit queue size
        "queue.buffering.max.kbytes": 1048576,   # 1GB max queue
    }
    return Producer(producer_config)


# ---------------------------------------------------------------------
# Verify Kafka Connection
# ---------------------------------------------------------------------
def verify_kafka_connection():
    """Verify that we can connect to Kafka broker."""
    try:
        admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKER})
        # Try to get metadata - this will fail if we can't connect
        metadata = admin_client.list_topics(timeout=10)
        print(f"[INFO] Successfully connected to Kafka broker: {KAFKA_BROKER}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to connect to Kafka broker {KAFKA_BROKER}: {e}")
        print("[ERROR] Make sure Kafka is running and accessible.")
        return False


# ---------------------------------------------------------------------
# Kafka Delivery Callback
# ---------------------------------------------------------------------
def delivery_report(err, msg):
    """Confirm whether Kafka successfully received the message."""
    if err:
        delivery_stats["failed"] += 1
        error_msg = f"Delivery failed: {err}"
        delivery_stats["errors"].append(error_msg)
        # Only print first few errors to avoid spam
        if delivery_stats["failed"] <= 5:
            print(f"[ERROR] {error_msg}")
        elif delivery_stats["failed"] == 6:
            print("[WARN] Suppressing further delivery error messages...")
    else:
        delivery_stats["delivered"] += 1


# ---------------------------------------------------------------------
# Stream a Parquet File to Kafka
# ---------------------------------------------------------------------
def stream_parquet_to_kafka(parquet_path, topic, delay=0.001, producer=None):
    """
    Stream a parquet file to Kafka topic.
    
    Args:
        parquet_path: Path to the parquet file
        topic: Kafka topic name
        delay: Delay between messages in seconds
        producer: Kafka producer instance (creates new if None)
    """
    if producer is None:
        producer = create_producer()
    
    try:
        file_name = Path(parquet_path).name
        print(f"[INFO] Loading parquet file: {file_name}")
        
        # Load parquet file
        df = pd.read_parquet(parquet_path)
        original_rows = len(df)
        if topic == LIDAR_TOPIC and MAX_LIDAR_ROWS > 0 and original_rows > MAX_LIDAR_ROWS:
            df = df.head(MAX_LIDAR_ROWS)
            print(f"[INFO] Limiting LiDAR rows from {original_rows:,} to first {len(df):,} rows")
        total_rows = len(df)
        
        print(f"[INFO] Streaming {total_rows:,} rows from '{file_name}' → topic '{topic}'")
        
        # Reset stats for this file
        file_failed_rows = 0
        start_delivered = delivery_stats["delivered"]
        start_failed = delivery_stats["failed"]
        
        # Poll interval - poll every N messages instead of every message
        POLL_INTERVAL = 100
        
        for idx, row in df.iterrows():
            try:
                # Convert row to JSON
                payload = json.dumps(row.to_dict()).encode("utf-8")
                
                # Handle backpressure - wait if queue is full
                while True:
                    try:
                        producer.produce(
                            topic, 
                            value=payload, 
                            callback=delivery_report
                        )
                        break
                    except BufferError:
                        # Queue is full, poll to flush some messages
                        producer.poll(0.1)
                        time.sleep(0.01)
                
                # Poll periodically instead of every message
                if (idx + 1) % POLL_INTERVAL == 0:
                    producer.poll(0)
                
                # Progress updates
                if (idx + 1) % 10000 == 0:
                    delivered = delivery_stats["delivered"]
                    failed = delivery_stats["failed"]
                    print(f"  [INFO] Progress: {idx + 1:,}/{total_rows:,} rows | "
                          f"Delivered: {delivered:,} | Failed: {failed:,}")
                
                # Small delay to avoid overwhelming Kafka
                if delay > 0:
                    time.sleep(delay)
                    
            except Exception as e:
                file_failed_rows += 1
                print(f"[ERROR] Failed to send row {idx}: {e}")
                if file_failed_rows > 10:
                    print(f"[ERROR] Too many errors for file {file_name}, stopping...")
                    break
        
        # Flush remaining messages
        print(f"[INFO] Flushing remaining messages for '{file_name}'...")
        remaining = producer.flush(timeout=30)
        if remaining > 0:
            print(f"[WARN] {remaining} messages were not delivered after flush timeout")
        
        file_delivered = delivery_stats["delivered"] - start_delivered
        file_failed_callbacks = delivery_stats["failed"] - start_failed
        file_success = (file_failed_rows == 0 
                        and file_failed_callbacks == 0 
                        and remaining == 0)
        status = "[SUCCESS]" if file_success else "[WARN]"
        print(f"{status} Finished streaming '{file_name}'")
        print(f"  Total rows: {total_rows:,} | Delivered: {file_delivered:,} "
              f"| Callback failures: {file_failed_callbacks:,} "
              f"| Row errors: {file_failed_rows:,}\n")
        
        return file_success
        
    except Exception as e:
        print(f"[ERROR] Failed streaming file '{parquet_path}': {e}\n")
        import traceback
        traceback.print_exc()
        return False


# ---------------------------------------------------------------------
# Stream All Parquet Files in Directory
# ---------------------------------------------------------------------
def stream_all_parquet_files(parquet_dir):
    """Stream all parquet files from directory to Kafka."""
    parquet_path = Path(parquet_dir)
    
    if not parquet_path.exists():
        print(f"[ERROR] Directory not found: {parquet_dir}")
        return False
    
    files = sorted(parquet_path.glob("*.parquet"))
    
    if not files:
        print(f"[WARN] No parquet files found in {parquet_dir}")
        return False
    
    print(f"[INFO] Found {len(files)} parquet file(s)\n")
    
    # Verify Kafka connection before starting
    if not verify_kafka_connection():
        return False
    
    # Create producer
    producer = create_producer()
    
    # Reset global stats
    delivery_stats["delivered"] = 0
    delivery_stats["failed"] = 0
    delivery_stats["errors"] = []
    
    # Categorize files
    lidar_files = [f for f in files if "metadata" not in f.name and "colorscale" not in f.name]
    metadata_files = [f for f in files if "metadata" in f.name or "colorscale" in f.name]
    
    success = True
    
    # -- LiDAR Point Cloud Data --
    if lidar_files:
        print(f"--- Streaming LiDAR Data ({len(lidar_files)} files) ---")
        for f in lidar_files:
            if not stream_parquet_to_kafka(str(f), LIDAR_TOPIC, delay=0.001, producer=producer):
                success = False
    
    # -- Metadata --
    if metadata_files:
        print(f"--- Streaming Metadata ({len(metadata_files)} files) ---")
        for f in metadata_files:
            if not stream_parquet_to_kafka(str(f), METADATA_TOPIC, delay=0.01, producer=producer):
                success = False
    
    # Final summary
    total_delivered = delivery_stats["delivered"]
    total_failed = delivery_stats["failed"]
    
    print("=" * 60)
    print("[SUMMARY] Streaming Complete")
    print(f"  Total Delivered: {total_delivered:,} messages")
    print(f"  Total Failed: {total_failed:,} messages")
    
    if total_failed > 0:
        print(f"\n[WARN] {total_failed} messages failed to deliver.")
        if len(delivery_stats["errors"]) > 0:
            print("First few errors:")
            for err in delivery_stats["errors"][:5]:
                print(f"  - {err}")
    
    if success and total_failed == 0:
        print("[SUCCESS] All parquet files streamed successfully!")
    else:
        print("[WARN] Some files had errors during streaming.")
    
    return success


# ---------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------
if __name__ == "__main__":
    try:
        success = stream_all_parquet_files("parquet_output")
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n[INFO] Streaming interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
