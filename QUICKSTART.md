# LiDAR Lake - Complete Setup & Operation Guide

## 📋 Project Overview

LiDAR Lake is a big data pipeline for processing LiDAR point cloud data from autonomous vehicles. The pipeline:
1. Converts raw PLY files to Parquet format
2. Streams data through Kafka
3. Cleans and validates points using Flink
4. Stores results in Kafka topics and HDFS

---

## 🚀 Quick Start (All-in-One Commands)

### Terminal 1: Start Kafka Infrastructure
```powershell
cd E:\Dev\Big-Data-Project\LiDAR-Lake
docker compose up -d
```
This starts:
- ZooKeeper (coordination service)
- Kafka (message broker)
- Namenode (HDFS master)
- Datanode (HDFS worker)
- Jobmanager & Taskmanager (Flink processing)

### Terminal 2: Activate Python Environment
```powershell
cd E:\Dev\Big-Data-Project\LiDAR-Lake
.\myenv\Scripts\Activate.ps1
```

### Terminal 3: Convert PLY to Parquet
```powershell
python .\to_parquet.py
```
**What it does:**
- Reads Toronto_3D LiDAR data (.ply files)
- Converts to Parquet format (compressed, efficient)
- Exports metadata (classifications, color scale)
- Outputs to `parquet_output/` folder

**Output:**
```
Found 4 PLY file(s). Starting conversion...
✓ Converted: Toronto_3D/Toronto_3D/L001.ply -> parquet_output/L001.parquet
✓ Converted: Toronto_3D/Toronto_3D/L002.ply -> parquet_output/L002.parquet
...
Successfully converted 4/4 file(s)
```

### Terminal 4: Stream Parquet Data to Kafka
```powershell
python .\data_streaming.py
```
**What it does:**
- Reads all Parquet files from `parquet_output/`
- Streams LiDAR points to Kafka topic: `lidar-stream` (21+ million records)
- Streams metadata to Kafka topic: `lidar-metadata`
- Shows progress every 1000 records

**Expected output:**
```
Found 6 parquet file(s)
--- Streaming LiDAR Data (4 files) ---
Streaming 21567172 records from L001.parquet to topic 'lidar-stream'...
  Streamed 1000 records...
  Streamed 2000 records...
```

**Note:** This takes 30-60 minutes depending on your system. Keep running while processing.

### Terminal 5: Run Flink Cleaning Job
```powershell
docker exec -it jobmanager flink run -py /opt/flink/usrlib/Lidar_cleaning/__main__.py
```
**What it does:**
- Subscribes to Kafka `lidar-stream` topic
- Validates each LiDAR point (coordinates, intensity, classification)
- Routes valid points → `lidar-clean` topic & HDFS `/lidar/clean/`
- Routes invalid points → `lidar-dirty` topic & HDFS `/lidar/dirty/`
- Writes to HDFS every batch (fault tolerance)

**Expected output:**
```
Starting LiDAR Cleaning Job
Kafka Broker: kafka:9092
Raw Topic: lidar-stream
Clean Topic: lidar-clean
Dirty Topic: lidar-dirty
HDFS Clean Output: hdfs://namenode:8020/lidar/clean/
HDFS Dirty Output: hdfs://namenode:8020/lidar/dirty/
```

---

## 📊 Monitoring & Verification

### Monitor Flink Job UI
```
http://localhost:8081
```
Shows job status, throughput, and error logs in real-time.

### View HDFS NameNode UI
```
http://localhost:9870
```
Check file storage and block distribution.

### Check Kafka Topics
```powershell
# List all topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# View messages from clean topic (first 10)
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic lidar-clean --from-beginning --max-messages 10

# View messages from dirty topic
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic lidar-dirty --from-beginning --max-messages 10
```

### Check HDFS Storage
```powershell
# List clean records in HDFS
docker exec namenode hdfs dfs -ls /lidar/clean/

# Sample clean records
docker exec namenode hdfs dfs -cat /lidar/clean/clean-lidar-*.json | head -5

# Get file count
docker exec namenode hdfs dfs -count /lidar/clean/
```

### Check Flink Checkpoints
```powershell
# View checkpoint directory
docker exec jobmanager ls -la /tmp/flink-checkpoints/
```

---

## 🛑 Stopping & Cleanup

### Stop All Services
```powershell
# Gracefully stop containers
docker compose stop

# Or force stop
docker compose kill

# Remove containers and volumes
docker compose down

# Remove volumes (WARNING: Deletes all data!)
docker compose down -v
```

### View Docker Logs
```powershell
# All services
docker compose logs

# Specific service
docker compose logs jobmanager

# Follow logs in real-time
docker compose logs -f kafka

# Last 100 lines
docker compose logs --tail=100
```

---

## 🔧 Configuration & Environment Variables

### Kafka Configuration
- **Bootstrap Server:** `kafka:9092` (internal Docker network)
- **Host Access:** `localhost:29092` (from Windows)
- **Topics:** Auto-created (`auto.create.topics.enable=true`)

### Flink Configuration
- **Parallelism:** 2 (set in docker-compose.yaml)
- **Checkpoint Interval:** 60 seconds
- **State Backend:** Built-in (local filesystem)

### HDFS Configuration
- **Namenode:** `hdfs://namenode:8020`
- **Clean Output:** `/lidar/clean/`
- **Dirty Output:** `/lidar/dirty/`

### Python Scripts (Customizable)
```powershell
# Modify Kafka broker (for local setup)
# In data_streaming.py or __main__.py, change:
KAFKA_BROKER = "127.0.0.1:9092"  # Local without Docker

# Modify HDFS paths
# In __main__.py, set environment variable:
$env:HDFS_OUTPUT_CLEAN = "hdfs://namenode:8020/my-path/clean/"
```

---

## 📈 Data Pipeline Flow

```
Toronto_3D/
  ├── L001.ply
  ├── L002.ply
  └── Mavericks_classes_9.txt
      ↓
   [to_parquet.py]
      ↓
  parquet_output/
  ├── L001.parquet
  ├── L002.parquet
  └── metadata
      ↓
   [data_streaming.py]
      ↓
   Kafka Topics:
  ├── lidar-stream (raw data)
  └── lidar-metadata
      ↓
   [Flink Cleaning Job]
      ↓
   Kafka Topics (output):
  ├── lidar-clean (valid points)
  ├── lidar-dirty (invalid points)
   ↓
   HDFS Storage:
  ├── /lidar/clean/ (valid points)
  └── /lidar/dirty/ (invalid points)
```

---

## ⚠️ Common Issues & Solutions

### Issue 1: "Cannot connect to Kafka"
**Cause:** Kafka broker not running or wrong address
```powershell
# Check if Kafka is running
docker ps | grep kafka

# Verify Kafka started successfully
docker compose logs kafka | tail -20

# Restart Kafka
docker compose restart kafka
```

### Issue 2: "ModuleNotFoundError: No module named 'noise_filters'"
**Cause:** PyFlink trying to pickle external imports
**Solution:** Already fixed in `__main__.py` - noise filters are inlined

### Issue 3: "HDFS permission denied" or "Connection refused"
**Cause:** HDFS not ready or path doesn't exist
```powershell
# Wait for HDFS to be ready
docker compose logs namenode | grep "SafeMode"

# Create output directories manually
docker exec namenode hdfs dfs -mkdir -p /lidar/clean /lidar/dirty
docker exec namenode hdfs dfs -chmod 777 /lidar
```

### Issue 4: "No data flowing through pipeline"
**Cause:** Kafka topics empty or Flink not subscribed
```powershell
# Verify data in Kafka
docker exec kafka kafka-topics --describe --bootstrap-server localhost:9092 --topic lidar-stream

# Check topic record count
docker exec kafka kafka-run-class kafka.tools.JmxTool --object-name kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec
```

---

## 🎯 Performance Tuning

### Increase Throughput
```yaml
# In docker-compose.yaml
taskmanager:
  environment:
    FLINK_PROPERTIES: |
      taskmanager.numberOfTaskSlots: 4  # Increase from 2
      parallelism.default: 4
```

### Reduce Checkpoint Interval (More Frequent Backups)
```python
# In __main__.py
env.enable_checkpointing(
    interval=30_000,  # 30 seconds instead of 60
    mode=CheckpointingMode.EXACTLY_ONCE,
)
```

### Increase Batch Size (Faster Streaming)
```python
# In data_streaming.py
if batch_count % 500 == 0:  # Print progress every 500 instead of 1000
    print(f"Processed {batch_count} records...")
```

---

## 📝 Validation Criteria (Noise Filters)

Valid LiDAR points must pass all checks:
- **Coordinates:** X ∈ [-2000, 2000], Y ∈ [-2000, 2000], Z ∈ [-200, 500]
- **Intensity:** [0, 65535]
- **Classification:** {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

Failures include:
- `coord_out_of_bounds` - Coordinates outside limits
- `intensity_out_of_range` - Invalid intensity value
- `invalid_class` - Unknown classification
- `parse_error` - JSON/data format error

---

## 🔗 Useful Links

- **Kafka Documentation:** https://kafka.apache.org/documentation/
- **Flink Documentation:** https://flink.apache.org/
- **HDFS WebUI:** http://localhost:9870
- **Flink WebUI:** http://localhost:8081

---

## ✅ Checklist for Full Run

- [ ] Docker Desktop running
- [ ] `docker compose up -d` started all services
- [ ] `python .\to_parquet.py` completed successfully
- [ ] `python .\data_streaming.py` streaming data (25+ million records)
- [ ] `docker exec -it jobmanager flink run...` cleaning job running
- [ ] Monitor topics with `kafka-console-consumer`
- [ ] Verify HDFS output with `hdfs dfs -ls /lidar/clean/`
- [ ] Check Flink UI at http://localhost:8081

---

**Last Updated:** November 24, 2025

