from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType
import os

# ========================
# ENV LOADER (SAFE)
# ========================
def get_env(key):
    value = os.getenv(key)
    if not value:
        raise Exception(f"❌ Missing ENV: {key}")
    return value

KAFKA_BOOTSTRAP = get_env("KAFKA_BOOTSTRAP")
KAFKA_TOPIC = get_env("KAFKA_TOPIC")

POSTGRES_URL = get_env("POSTGRES_URL")
POSTGRES_USER = get_env("POSTGRES_USER")
POSTGRES_PASSWORD = get_env("POSTGRES_PASSWORD")
POSTGRES_TABLE = get_env("POSTGRES_TABLE")

print("\n🔧 CONFIG")
print("Kafka:", KAFKA_BOOTSTRAP)
print("Topic:", KAFKA_TOPIC)
print("Postgres:", POSTGRES_URL)
print("Table:", POSTGRES_TABLE)

# ========================
# SPARK SESSION
# ========================
spark = SparkSession.builder \
    .appName("FinStreamFinal") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ========================
# SCHEMA
# ========================
schema = StructType() \
    .add("id", StringType()) \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("market_cap", DoubleType()) \
    .add("timestamp", DoubleType())

# ========================
# READ FROM KAFKA
# ========================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# ========================
# PARSE JSON
# ========================
df_json = df.selectExpr("CAST(value AS STRING)")

df_parsed = df_json.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# ========================
# PROCESSING
# ========================
df_clean = df_parsed.filter(col("price").isNotNull() & (col("price") > 0))

df_final = df_clean.withColumn(
    "status",
    (col("price") > 50000).cast("string")
)

# ========================
# WRITE TO POSTGRES
# ========================
def write_to_postgres(batch_df, batch_id):

    print(f"\n🔥 Batch: {batch_id}")

    if batch_df.isEmpty():
        print("⚠️ Empty batch")
        return

    print("📊 Count:", batch_df.count())

    # ========================
    # 1. WRITE TO DOCKER DB
    # ========================
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/finstream") \
            .option("dbtable", "transactions") \
            .option("user", "postgres") \
            .option("password", "admin") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print("✅ Docker DB write success")

    except Exception as e:
        print("❌ Docker DB error:", e)

    # ========================
    # 2. WRITE TO LOCAL DB
    # ========================
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://host.docker.internal:5432/finstream") \
            .option("dbtable", "transactions") \
            .option("user", "postgres") \
            .option("password", "admin") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print("✅ Local DB write success")

    except Exception as e:
        print("❌ Local DB error:", e)
# ========================
# START STREAM
# ========================
query = df_final.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "/tmp/checkpoints/finstream") \
    .start()

print("🚀 STREAM STARTED...")

query.awaitTermination()
#