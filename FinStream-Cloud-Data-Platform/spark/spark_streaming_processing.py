# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName("FinStreamLive") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
#     .getOrCreate()

# df = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:29092") \
#     .option("subscribe", "transactions") \
#     .load()

# df.selectExpr("CAST(value AS STRING)").writeStream \
#     .format("console") \
#     .start() \
#     .awaitTermination()

# 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType

# ================================
# 1. CREATE SPARK SESSION
# ================================
spark = SparkSession.builder \
    .appName("FinStreamLive") \
    .config(
    "spark.jars.packages",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ================================
# 2. DEFINE SCHEMA (IMPORTANT)
# ================================
schema = StructType() \
    .add("id", StringType()) \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("market_cap", DoubleType()) \
    .add("timestamp", DoubleType())

# ================================
# 3. READ STREAM FROM KAFKA
# ================================
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# ================================
# 4. CONVERT DATA (BINARY → JSON)
# ================================
df_json = df.selectExpr("CAST(value AS STRING)")

df_parsed = df_json.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# ================================
# 5. DATA CLEANING
# ================================
df_clean = df_parsed.filter(
    (col("price").isNotNull()) & (col("price") > 0)
)

# ================================
# 6. FRAUD DETECTION LOGIC 🔥
# ================================
df_final = df_clean.withColumn(
    "status",
    (col("price") > 50000).cast("string")  # simple fraud rule
)

# ================================
# 7. OUTPUT (CONSOLE)
# ================================
query = df_final.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query.awaitTermination()