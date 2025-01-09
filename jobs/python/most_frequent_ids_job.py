import os
from sys import argv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

import matplotlib.pyplot as plt

# Retrieve command-line arguments
clickstream_data_path = argv[1]

# Create a Spark session
spark = SparkSession.builder.appName("ClickstreamStatistics").getOrCreate()

# Load clickstream data from Parquet
clickstream_data = spark.read.parquet(clickstream_data_path)

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
print("Most frequent articles from prev_id (with titles):")
prev_id_stats.select("prev_id", "prev_title", "count_prev_id").show(10)

print("Most frequent articles from curr_id (with titles):")
curr_id_stats.select("curr_id", "curr_title", "count_curr_id").show(10)

# Convert to Pandas DataFrame for plotting
prev_df = prev_id_stats.limit(10).toPandas()
curr_df = curr_id_stats.limit(10).toPandas()

# Plot the data
plt.figure(figsize=(14, 7))

# Plot for prev_id
plt.bar(prev_df["prev_title"], prev_df["count_prev_id"], alpha=0.7, label="prev_id")

# Plot for curr_id
plt.bar(curr_df["curr_title"], curr_df["count_curr_id"], alpha=0.7, label="curr_id")

# Customize the plot
plt.title("Most frequent articles (prev_id vs curr_id)")
plt.xlabel("Article title")
plt.ylabel("# of occurences")
plt.xticks(rotation=45, ha='right')
plt.legend()

# Save the plot
output_dir = "/opt/data"
os.makedirs(output_dir, exist_ok=True)
plot_path = os.path.join(output_dir, "most_frequent_ids.png")
plt.tight_layout()
plt.savefig(plot_path)
plt.close()
