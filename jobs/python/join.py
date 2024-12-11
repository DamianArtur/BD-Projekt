from sys import argv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

clickstream_data_path = argv[1]
articles_data_path = argv[2]

spark = SparkSession.builder.appName("JsonDataCleaning").getOrCreate()

clickstream_data = spark.read.option("header", "true").csv(clickstream_data_path)
articles_data = spark.read.json(articles_data_path)

# Join for prev_id
prev_join = clickstream_data.alias("clickstream") \
    .join(articles_data.alias("articles"), col("clickstream.prev_id") == col("articles.id"), how="left") \
    .select(
        col("clickstream.prev_id"), 
        col("clickstream.curr_id"), 
        col("clickstream.n"), 
        col("clickstream.prev_title"), 
        col("clickstream.curr_title"), 
        col("clickstream.type"), 
        col("articles.text").alias("prev_text")
    )

# Join for curr_id
final_join = prev_join.alias("prev_data") \
    .join(articles_data.alias("articles"), col("prev_data.curr_id") == col("articles.id"), how="left") \
    .select(
        col("prev_data.prev_id"), 
        col("prev_data.curr_id"), 
        col("prev_data.n"), 
        col("prev_data.prev_title"), 
        col("prev_data.curr_title"), 
        col("prev_data.type"), 
        col("prev_data.prev_text"),
        col("articles.text").alias("curr_text")
    )

final_join.show()