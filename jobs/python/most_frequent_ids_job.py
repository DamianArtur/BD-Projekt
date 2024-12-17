from sys import argv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

clickstream_data_path = argv[1]

# Create a Spark session
spark = SparkSession.builder.appName("ClickstreamStatistics").getOrCreate()

# Load clickstream data
clickstream_data = spark.read.option("header", "true").csv(clickstream_data_path)

# Calculate statistics for prev_id with titles
prev_id_stats = (
    clickstream_data
    .groupBy("prev_id", "prev_title")
    .agg(count("*").alias("count_prev_id"))
    .orderBy(col("count_prev_id").desc())
)

# Calculate statistics for curr_id
curr_id_stats = (
    clickstream_data
    .groupBy("curr_id", "curr_title")
    .agg(count("*").alias("count_curr_id"))
    .orderBy(col("count_curr_id").desc())
)

# Show results
print("Najczęściej występujące artykuły w prev_id (z tytułami):")
prev_id_stats.select("prev_id", "prev_title", "count_prev_id").show(10)

print("Najczęściej występujące artykuły w curr_id (z tytułami):")
curr_id_stats.select("curr_id", "curr_title", "count_curr_id").show(10)
