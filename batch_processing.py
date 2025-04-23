from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("TwitterBatchAnalysis") \
    .config("spark.jars", "postgresql-42.7.3.jar") \
    .getOrCreate()

# Read from correct table - note consistent indentation
batch_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost/twitter_data") \
    .option("dbtable", "tweet_analysis") \
    .option("user", "spark_user") \
    .option("password", "spark_pass") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Perform batch analysis
batch_analysis = batch_df.groupBy("airline", "sentiment") \
    .agg({"count": "max"}) \
    .withColumnRenamed("max(count)", "tweet_count_batch") \
    .orderBy(col("tweet_count_batch").desc())

print("📊 Batch Analysis Results:")
batch_analysis.show(truncate=False)

# Read streaming results
streaming_results = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost/twitter_data") \
    .option("dbtable", "tweet_analysis") \
    .option("user", "spark_user") \
    .option("password", "spark_pass") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Compare results
print("📈 Batch vs Streaming Comparison:")
comparison = batch_analysis.join(
    streaming_results.withColumnRenamed("count", "tweet_count_streaming"), 
    ["airline", "sentiment"],
    "outer"
).select(
    "airline", 
    "sentiment", 
    "tweet_count_batch", 
    "tweet_count_streaming", 
    "analysis_time"
).orderBy(col("tweet_count_streaming").desc())

comparison.show(truncate=False)

spark.stop()
