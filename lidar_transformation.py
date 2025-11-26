#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sqrt, when, lit
import datetime

spark = SparkSession.builder.appName("LiDARProcessing").getOrCreate()





# In[2]:


df = spark.read.json("hdfs:///tmp/transformed_points.jsonl")

print(f"✓ Data loaded: {df.count()} rows")
print("\nColumns:")
df.printSchema()


# In[4]:



# Read your data
df = spark.read.json("hdfs:///tmp/transformed_points.jsonl")

print("Total rows:", df.count())
df.printSchema()



# In[5]:


# ============================================
# CELL 2: Check distance statistics
# ============================================
# See what distance values you actually have
from pyspark.sql.functions import min, max, mean, count

distance_stats = df.select(
    min("distance_from_origin"),
    max("distance_from_origin"),
    mean("distance_from_origin"),
    count("distance_from_origin")
)

distance_stats.show()

# Show all unique distance values (if small dataset)
print("\nAll distance_from_origin values:")
df.show(20)



# In[6]:


min_dist = df.agg(min("distance_from_origin")).collect()[0][0]
max_dist = df.agg(max("distance_from_origin")).collect()[0][0]

print(f"Your data range: {min_dist} to {max_dist}")

# Option B: Filter with correct range
filtered = df.filter(
    (col("distance_from_origin") >= min_dist) & 
    (col("distance_from_origin") <= max_dist)
)

print(f"Filtered rows: {filtered.count()}")
filtered.show(5)


# In[7]:


df.limit(1).toPandas()


# In[8]:


df.groupBy("brightness_class").count().show()


# In[9]:


intensity_stats = df.select(
    min("intensity").alias("min_intensity"),
    max("intensity").alias("max_intensity"),
    mean("intensity").alias("avg_intensity")
)

intensity_stats.show()


# In[10]:


processed = df.select(
    "x", "y", "z",
    "intensity",
    "red", "green", "blue",
    "distance_from_origin",
    "distance_2d_horizontal",
    "brightness_class",
    "red_normalized",
    "green_normalized",
    "blue_normalized"
).withColumn(
    "brightness_avg", 
    (col("red_normalized") + col("green_normalized") + col("blue_normalized")) / 3
)

print(f"Processed rows: {processed.count()}")
processed.show(5, False)


# In[11]:


from pyspark.sql.functions import round, concat_ws

voxelized = processed.withColumn(
    "voxel_x", round(col("x") / 0.1).cast("int")
).withColumn(
    "voxel_y", round(col("y") / 0.1).cast("int")
).withColumn(
    "voxel_z", round(col("z") / 0.1).cast("int")
).withColumn(
    "voxel_id", concat_ws("_", col("voxel_x"), col("voxel_y"), col("voxel_z"))
)

unique_voxels = voxelized.select("voxel_id").distinct().count()
print(f"Unique voxels: {unique_voxels}")


# In[12]:


objects = voxelized.groupBy("voxel_id").agg({
    "x": "mean",
    "y": "mean",
    "z": "mean",
    "intensity": "mean",
    "red_normalized": "mean",
    "green_normalized": "mean",
    "blue_normalized": "mean",
    "brightness_avg": "mean"
}).withColumnRenamed("avg(x)", "mean_x") \
 .withColumnRenamed("avg(y)", "mean_y") \
 .withColumnRenamed("avg(z)", "mean_z") \
 .withColumnRenamed("avg(intensity)", "mean_intensity") \
 .withColumnRenamed("avg(red_normalized)", "mean_red_norm") \
 .withColumnRenamed("avg(green_normalized)", "mean_green_norm") \
 .withColumnRenamed("avg(blue_normalized)", "mean_blue_norm") \
 .withColumnRenamed("avg(brightness_avg)", "mean_brightness")

print(f"Final objects: {objects.count()}")
objects.show(10, False)


# In[13]:


output_path = "hdfs:///tmp/lidar_processed_v1.0.parquet"

objects.write     .mode("overwrite")     .parquet(output_path)

print(f"✓ Saved to: {output_path}")


# In[14]:


import subprocess

result = subprocess.run(
    ["hdfs", "dfs", "-ls", "-h", "/tmp/"],
    capture_output=True,
    text=True
)

print("Files in /staging_lidar_kafka_flink/:")
print(result.stdout)


# In[15]:


df = spark.read.parquet("hdfs:///tmp/lidar_processed_v1.0.parquet")
print("Total rows:", df.count())
df.printSchema()


# In[ ]:




