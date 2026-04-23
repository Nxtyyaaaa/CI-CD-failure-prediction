from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, avg, to_timestamp,
    hour, dayofweek, input_file_name
)
from pyspark.sql.window import Window

# -------------------------------
# 1. Start Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("CICD Failure Prediction - Advanced Pipeline") \
    .getOrCreate()

# -------------------------------
# 2. Load Dataset
# -------------------------------
df = spark.read.option("header", "true").csv("dataset/data.csv")

# -------------------------------
# 3. Clean Column Names
# -------------------------------
for c in df.columns:
    df = df.withColumnRenamed(c, c.strip().replace(" ", "_"))

print("\n=== RAW DATA ===")
df.show(5, truncate=False)

# -------------------------------
# 4. Data Type Fixes
# -------------------------------

# Convert timestamp
df = df.withColumn(
    "timestamp",
    to_timestamp("timestamp")
)

# -------------------------------
# 5. Feature Engineering
# -------------------------------

# Target variable (failure)
df = df.withColumn(
    "is_failure",
    when(col("status") == "failed", 1).otherwise(0)
)

# Error detection from message (case-insensitive)
df = df.withColumn(
    "error_flag",
    when(col("message").rlike("(?i)error|failed|exception"), 1).otherwise(0)
)

# -------------------------------
# 6. Time-Based Features
# -------------------------------

df = df.withColumn("hour", hour("timestamp"))
df = df.withColumn("day_of_week", dayofweek("timestamp"))

# -------------------------------
# 7. Pipeline-Level Aggregation
# -------------------------------

window_pipeline = Window.partitionBy("pipeline_id")

df = df.withColumn(
    "failure_rate",
    avg("is_failure").over(window_pipeline)
)

# -------------------------------
# 8. Rolling Failure Trend (KEY)
# -------------------------------

window_time = Window.partitionBy("pipeline_id") \
    .orderBy("timestamp") \
    .rowsBetween(-3, 0)

df = df.withColumn(
    "recent_failure_rate",
    avg("is_failure").over(window_time)
)

# -------------------------------
# 9. Failure Streak (ADVANCED)
# -------------------------------

window_streak = Window.partitionBy("pipeline_id") \
    .orderBy("timestamp") \
    .rowsBetween(Window.unboundedPreceding, 0)

from pyspark.sql.functions import sum as spark_sum

df = df.withColumn(
    "failure_streak",
    spark_sum("is_failure").over(window_streak)
)

# -------------------------------
# 10. Final Output
# -------------------------------

print("\n=== FINAL FEATURE DATASET ===")
df.select(
    "pipeline_id",
    "status",
    "is_failure",
    "error_flag",
    "failure_rate",
    "recent_failure_rate",
    "failure_streak",
    "hour",
    "day_of_week"
).show(10, truncate=False)

# -------------------------------
# 11. Save for ML
# -------------------------------

df.coalesce(1).write.mode("overwrite").parquet("processed_data")

print("\n✅ FINAL DATASET SAVED → processed_data/")

# -------------------------------
# 12. Stop Spark
# -------------------------------
spark.stop()