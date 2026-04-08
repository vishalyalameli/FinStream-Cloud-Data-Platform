from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

POSTGRES_URL = os.getenv("POSTGRES_URL")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE")

# ========================
# SPARK SESSION
# ========================
spark = SparkSession.builder \
    .appName("FinStreamDynamic") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3"
    ) \
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
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP.replace("localhost", "kafka")) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

df_json = df.selectExpr("CAST(value AS STRING)")

df_parsed = df_json.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# ========================
# PROCESSING
# ========================
df_clean = df_parsed.filter(col("price") > 0)

df_final = df_clean.withColumn(
    "status",
    (col("price") > 50000).cast("string")
)

# ========================
# WRITE TO POSTGRES
# ========================
def write_to_postgres(batch_df, batch_id):

    print(f"\n🔥 Processing batch: {batch_id}")

    count = batch_df.count()
    print(f"📊 Rows in batch: {count}")

    # 👉 SHOW DATA (IMPORTANT)
    batch_df.show(truncate=False)

    # ❗ If no data → stop here
    if count == 0:
        print("⚠️ No data in this batch, skipping write")
        return

    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", POSTGRES_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print("✅ Data written to PostgreSQL")

    except Exception as e:
        print("❌ Error writing to DB:", e)

query = df_final.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .start()