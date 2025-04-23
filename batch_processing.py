from pyspark.sql import SparkSession

# ✅ Initialize Spark with JDBC driver registered
spark = SparkSession.builder \
    .appName("TwitterBatchAnalysis") \
    .config("spark.jars", "postgresql-42.7.3.jar") \
    .getOrCreate()

# ✅ Read batch data from 'tweets' table in PostgreSQL
batch_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost/twitter_data") \
    .option("dbtable", "tweets") \
    .option("user", "spark_user") \
    .option("password", "spark_pass") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# ✅ Perform batch sentiment analysis
batch_analysis = batch_df.groupBy("airline", "sentiment") \
    .count() \
    .orderBy("airline", "count", ascending=False)

# ✅ Display batch results
print("📊 Batch Analysis Results:")
batch_analysis.show(truncate=False)

# ✅ Read previously processed streaming results
streaming_results = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost/twitter_data") \
    .option("dbtable", "tweet_analysis") \
    .option("user", "spark_user") \
    .option("password", "spark_pass") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# ✅ Compare batch vs streaming results
print("📈 Batch vs Streaming Comparison:")
batch_analysis.join(streaming_results, ["airline", "sentiment"], "outer") \
    .show(truncate=False)

# ✅ Clean shutdown
spark.stop()
