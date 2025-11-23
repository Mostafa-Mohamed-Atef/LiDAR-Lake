# 🚗 LiDAR Lake – Graduation Project (SIC G13)

A scalable, automated, and high-performance platform for processing LiDAR data used in autonomous vehicles.
This project enables reliable ingestion, cleaning, transformation, and analysis of massive 3D point-cloud datasets.

![WhatsApp Image 2025-11-21 at 19 48 55_b60ab510](https://github.com/user-attachments/assets/52a74ace-5db7-4626-bb44-b2549371190b)


---

## 👥 Team Members

* **Mostafa Atef**
* **Seif Alaa**
* **Salma Ashraf**

---

## 📌 1. The Business Problem

Autonomous vehicles generate **millions of 3D LiDAR points per second**.
This data is:

* Massive
* Noisy
* Expensive to store
* Hard to process efficiently

Without a well-designed data pipeline, companies suffer from:

* Reduced safety
* Poor perception accuracy
* Slow development cycles
* High operational costs

**LiDAR Lake** solves these issues by providing an automated, scalable data platform that organizes, cleans, and prepares LiDAR data for downstream use.

---

## ⭐ 2. Why This Project Matters

LiDAR Lake gives companies and research teams the tools needed to extract meaningful insights from raw LiDAR data. Benefits include:

* 🚀 Faster development of self-driving features
* 🧹 Better quality control by removing sensor noise
* 💾 Cheaper long-term storage + easier data management
* 🤖 Smoother training pipelines for AI perception models
* 📊 Real-time & historical analytics across vehicle fleets

Build once → reuse for every dataset, vehicle, and experiment.
This saves **time**, **money**, and **engineering effort**.

---

## 🏗️ 3. Technical Architecture Overview

LiDAR Lake follows a **simulated streaming, big-data architecture** optimized for terabytes of 3D point-cloud data.

### **1. Kafka – Ingestion Layer**

Collects raw LiDAR frames + metadata.
Designed for high-throughput, loss-free ingestion from vehicles or upload services.

### **2. Flink – Real-Time Cleaning Layer**

Cleans sensor metadata streams by:

* Removing corrupted frames
* Filtering invalid points
* Ensuring accurate timestamps

Prevents bad data from ever entering storage.

### **3. HDFS – Raw Storage Layer**

Stores cleaned LiDAR frames in distributed format (PCD/Parquet).
Handles massive LiDAR datasets reliably.

### **4. Spark – Batch Processing & Feature Extraction**

Spark performs heavy computations:

* Spatial filtering
* Downsampling / voxelization
* Geometry / object feature extraction
* ML-ready feature vector generation

Outputs analytics-ready Parquet datasets.

### **5. Iceberg – Lakehouse Layer**

Spark writes output into Iceberg tables, enabling:

* ACID transactions
* Time travel & versioning
* Schema evolution
* Auto-partitioning
* Fast metadata operations

Iceberg becomes the **source of truth** for AI & analytics.

### **6. Airflow – Orchestration Layer**

Automates:

* Ingestion
* Cleaning
* Spark feature extraction
* Table updates

Ensures reliable end-to-end data operations.

### **7. AI & Visualization – Insights Layer**

Teams use Iceberg to:

* Build dashboards (Grafana)
* Train ML models (PyTorch)
* Visualize 3D point clouds
* Track feature quality and patterns

---

## 🧭 4. Why Not Cloud?

LiDAR data is:

* Extremely large (terabytes per week)
* Expensive to store/compute in the cloud
* Bandwidth-heavy to upload
* Sensitive for commercial & R&D use

An **on-premise, open-source** architecture provides:

* Full data control
* No vendor lock-in
* Predictable costs
* High performance at scale

Ideal for universities, research labs, and companies with limited budgets or strict privacy needs.
