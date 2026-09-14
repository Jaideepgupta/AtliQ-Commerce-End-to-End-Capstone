# Databricks notebook source

# MAGIC %md
# MAGIC # AtliQ Phase 2 — Kafka → Delta with Structured Streaming
# MAGIC
# MAGIC Build Bronze → Silver → Gold using Structured Streaming.
# MAGIC
# MAGIC **Databricks Free Edition:**
# MAGIC - Checkpoints use Unity Catalog Volumes
# MAGIC - Streams write to managed Delta tables
# MAGIC - Each stream has its own checkpoint folder
# MAGIC - `availableNow=True` is used because ProcessingTime is not supported


# COMMAND ----------

# ============================================================
# KAFKA CONNECTION
# ============================================================

# Demo configuration
# IMPORTANT:
# Do not commit real Kafka credentials to GitHub.

KAFKA_BOOTSTRAP = "<YOUR_KAFKA_BOOTSTRAP_SERVER>"
KAFKA_API_KEY = "<YOUR_KAFKA_API_KEY>"
KAFKA_API_SECRET = "<YOUR_KAFKA_API_SECRET>"
KAFKA_TOPIC = "atliq.orders.events"


# ============================================================
# UNITY CATALOG OBJECTS
# ============================================================

CATALOG = "atliq"
SCHEMA = "streaming"


# ============================================================
# CHECKPOINT ROOT
# ============================================================

CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"


# COMMAND ----------

# ============================================================
# ONE-TIME SETUP
# ============================================================

spark.sql(
    f"CREATE CATALOG IF NOT EXISTS {CATALOG}"
)

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}"
)

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## TASK 1 — BRONZE
# MAGIC
# MAGIC Read raw Kafka events without parsing the JSON.
# MAGIC
# MAGIC Preserve:
# MAGIC - Kafka key
# MAGIC - Kafka value
# MAGIC - topic
# MAGIC - partition
# MAGIC - offset
# MAGIC - timestamp


# COMMAND ----------

from pyspark.sql import functions as F


# ============================================================
# READ EVENTS FROM KAFKA
# ============================================================

bronze_df = (
    spark.readStream
    .format("kafka")

    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP
    )

    .option(
        "subscribe",
        KAFKA_TOPIC
    )

    # Read existing events as well as new events
    .option(
        "startingOffsets",
        "earliest"
    )

    # Confluent Cloud authentication
    .option(
        "kafka.security.protocol",
        "SASL_SSL"
    )

    .option(
        "kafka.sasl.mechanism",
        "PLAIN"
    )

    .option(
        "kafka.sasl.jaas.config",
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{KAFKA_API_KEY}" '
        f'password="{KAFKA_API_SECRET}";'
    )

    .load()
)


# COMMAND ----------

# ============================================================
# KEEP KAFKA METADATA
# ============================================================

# Convert binary key/value into strings
# Preserve Kafka metadata

bronze_df = bronze_df.select(

    F.col("key")
        .cast("string")
        .alias("key"),

    F.col("value")
        .cast("string")
        .alias("value"),

    F.col("topic"),

    F.col("partition"),

    F.col("offset"),

    F.col("timestamp")
)


# COMMAND ----------

# ============================================================
# WRITE BRONZE STREAM TO DELTA
# ============================================================

bronze_query = (
    bronze_df.writeStream

    .format("delta")

    .outputMode("append")

    # Required for Databricks Free Edition
    .trigger(
        availableNow=True
    )

    # Unique checkpoint for Bronze
    .option(
        "checkpointLocation",
        f"{CKPT}/bronze"
    )

    .toTable(
        f"{CATALOG}.{SCHEMA}.bronze_order_events"
    )
)


# Wait until the bounded Bronze run finishes
bronze_query.awaitTermination()


# COMMAND ----------

# MAGIC %md
# MAGIC ## TASK 2 — SILVER
# MAGIC
# MAGIC Read Bronze as a stream.
# MAGIC
# MAGIC Transformations:
# MAGIC 1. Parse JSON using an explicit schema
# MAGIC 2. Convert `event_ts` to timestamp
# MAGIC 3. Apply a 10-minute watermark
# MAGIC 4. Deduplicate using `event_id`
# MAGIC 5. Write to Silver


# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)


# ============================================================
# EXPLICIT EVENT SCHEMA
# ============================================================

event_schema = StructType([

    StructField(
        "event_id",
        StringType(),
        True
    ),

    StructField(
        "event_type",
        StringType(),
        True
    ),

    StructField(
        "event_ts",
        StringType(),
        True
    ),

    StructField(
        "order_id",
        IntegerType(),
        True
    ),

    StructField(
        "customer_id",
        IntegerType(),
        True
    ),

    StructField(
        "city",
        StringType(),
        True
    ),

    StructField(
        "product_id",
        IntegerType(),
        True
    ),

    StructField(
        "quantity",
        IntegerType(),
        True
    ),

    StructField(
        "order_amount",
        DoubleType(),
        True
    ),

    StructField(
        "payment_method",
        StringType(),
        True
    )
])


# COMMAND ----------

# ============================================================
# READ BRONZE AS STREAMING TABLE
# ============================================================

bronze_stream = (
    spark.readStream
    .table(
        f"{CATALOG}.{SCHEMA}.bronze_order_events"
    )
)


# COMMAND ----------

# ============================================================
# PARSE JSON + SILVER TRANSFORMATIONS
# ============================================================

silver_df = (
    bronze_stream

    # Parse JSON
    .withColumn(
        "event_json",
        F.from_json(
            F.col("value"),
            event_schema
        )
    )

    # Extract parsed JSON fields
    .select(
        "event_json.*"
    )

    # Convert event timestamp
    .withColumn(
        "event_ts",
        F.to_timestamp("event_ts")
    )

    # Handle late-arriving events
    .withWatermark(
        "event_ts",
        "10 minutes"
    )

    # Remove duplicate events
    .dropDuplicates(
        ["event_id"]
    )
)


# COMMAND ----------

# ============================================================
# WRITE SILVER STREAM TO DELTA
# ============================================================

silver_query = (
    silver_df.writeStream

    .format("delta")

    .outputMode("append")

    # Required for Databricks Free Edition
    .trigger(
        availableNow=True
    )

    # Unique checkpoint for Silver
    .option(
        "checkpointLocation",
        f"{CKPT}/silver"
    )

    .toTable(
        f"{CATALOG}.{SCHEMA}.silver_order_events"
    )
)


# Wait until the bounded Silver run finishes
silver_query.awaitTermination()


# COMMAND ----------

# MAGIC %md
# MAGIC ## TASK 3 — GOLD
# MAGIC
# MAGIC Keep only `payment_received` events.
# MAGIC
# MAGIC Aggregate into 5-minute tumbling windows:
# MAGIC
# MAGIC - `orders_paid` = number of payment events
# MAGIC - `revenue` = total order amount
# MAGIC
# MAGIC Closed windows are written to Gold.


# COMMAND ----------

# ============================================================
# READ SILVER AS STREAMING TABLE
# ============================================================

silver_stream = (
    spark.readStream
    .table(
        f"{CATALOG}.{SCHEMA}.silver_order_events"
    )
)


# COMMAND ----------

# ============================================================
# FILTER PAYMENT EVENTS + 5-MINUTE WINDOW
# ============================================================

gold_df = (
    silver_stream

    # Keep only successful payment events
    .filter(
        F.col("event_type") == "payment_received"
    )

    # Event-time watermark
    .withWatermark(
        "event_ts",
        "10 minutes"
    )

    # 5-minute tumbling windows
    .groupBy(
        F.window(
            F.col("event_ts"),
            "5 minutes"
        )
    )

    # Calculate metrics
    .agg(

        F.count("*")
            .alias("orders_paid"),

        F.sum("order_amount")
            .alias("revenue")
    )

    # Flatten window structure
    .select(

        F.col("window.start")
            .alias("window_start"),

        F.col("window.end")
            .alias("window_end"),

        F.col("orders_paid"),

        F.col("revenue")
    )
)


# COMMAND ----------

# ============================================================
# WRITE GOLD STREAM TO DELTA
# ============================================================

gold_query = (
    gold_df.writeStream

    .format("delta")

    .outputMode("append")

    # Required for Databricks Free Edition
    .trigger(
        availableNow=True
    )

    # Unique checkpoint for Gold
    .option(
        "checkpointLocation",
        f"{CKPT}/gold"
    )

    .toTable(
        f"{CATALOG}.{SCHEMA}.gold_revenue_5min"
    )
)


# Wait until the bounded Gold run finishes
gold_query.awaitTermination()


# COMMAND ----------

# MAGIC %md
# MAGIC ## VERIFY — SILVER
# MAGIC
# MAGIC Check event counts for each event type.


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     event_type,
# MAGIC     COUNT(*) AS events
# MAGIC FROM atliq.streaming.silver_order_events
# MAGIC GROUP BY event_type
# MAGIC ORDER BY event_type;


# COMMAND ----------

# MAGIC %md
# MAGIC ## VERIFY — GOLD
# MAGIC
# MAGIC Check the 5-minute revenue windows.


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     window_start,
# MAGIC     window_end,
# MAGIC     orders_paid,
# MAGIC     revenue
# MAGIC FROM atliq.streaming.gold_revenue_5min
# MAGIC ORDER BY window_start DESC
# MAGIC LIMIT 12;


# COMMAND ----------

# MAGIC %md
# MAGIC ## VERIFY — BRONZE
# MAGIC
# MAGIC Check the total number of raw Kafka events.


# COMMAND ----------

spark.sql("""
SELECT
    COUNT(*) AS bronze_events
FROM atliq.streaming.bronze_order_events
""").show()