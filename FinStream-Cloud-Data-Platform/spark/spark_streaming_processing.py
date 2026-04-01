from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("FinStreamLive") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "transactions") \
    .load()

df.selectExpr("CAST(value AS STRING)").writeStream \
    .format("console") \
    .start() \
    .awaitTermination()