from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TwitterStreamAnalysis") \
    .getOrCreate()

# Define schema for incoming data
schema = StructType([
    StructField("tweet_id", StringType()),
    StructField("airline", StringType()),
    StructField("text", StringType()),
    StructField("sentiment", StringType()),
    StructField("user", StringType())
])

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tweets_topic") \
    .load()

# Parse JSON data
tweets_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Perform analysis - count tweets by airline and sentiment
analysis_df = tweets_df.groupBy("airline", "sentiment") \
    .count() \
    .orderBy("airline", "count", ascending=False)

# Add this after the analysis_df definition

def write_to_postgres(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost/twitter_data") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "tweet_analysis") \
        .option("user", "spark_user") \
        .option("password", "spark_pass") \
        .mode("append") \
        .save()

# Change the output to use foreachBatch
query = analysis_df \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_postgres) \
    .start()

query.awaitTermination()
