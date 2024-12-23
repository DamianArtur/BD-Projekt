from sys import argv

from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, Word2Vec
from pyspark.sql.functions import udf, col, length, avg, min, max, count
from pyspark.sql.types import StringType

from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

clickstream_data_path = argv[1]
articles_data_path = argv[2]

spark = SparkSession.builder.appName("JsonDataCleaning").getOrCreate()

articles_data = spark.read.json(articles_data_path)

# List of topics to identification
topics = [
    "sports",
    "history",
    "science",
    "technology",
    "art",
    "geography",
    "literature",
    "music",
    "philosophy",
    "politics",
    "biology",
    "physics",
    "mathematics",
    "economics",
    "psychology",
    "engineering",
    "medicine",
    "astronomy",
    "education",
    "architecture"
]

# Function to classify topic based on cosine similarity
def classify_topic(vector, topics_vectors, topics):
    if vector is None:
        return "unknown"
    similarities = cosine_similarity([vector], topics_vectors)[0]
    max_index = np.argmax(similarities)
    return topics[max_index]

classify_topic_udf = udf(lambda vec: classify_topic(vec, topics_vectors, topics), StringType())

# Tokenize article texts
tokenizer = Tokenizer(inputCol="text", outputCol="tokens")

# Apply tokenizer to the original DataFrame
data = tokenizer.transform(articles_data)

# Train Word2Vec Model
word2vec = Word2Vec(vectorSize=100, minCount=1, inputCol="tokens", outputCol="vector")
model = word2vec.fit(data)
data = model.transform(data)

# Get Topic Embeddings
topics_df = spark.createDataFrame([(topic,) for topic in topics], ["topic"])

tokenizer.setInputCol("topic")
tokenizer.setOutputCol("tokens")

# Tokenize the topics DataFrame
tokenized_topics_df = tokenizer.transform(topics_df)

# Apply Word2Vec model to get vectors for the topics
topics_df_with_vectors = model.transform(tokenized_topics_df)

# Collect the topic vectors
topics_vectors = [row['vector'] for row in topics_df_with_vectors.collect()]

# Classify Articles
data = data.withColumn("topic", classify_topic_udf(col("vector")))

# Add a column for article length (e.g., length of the "text" field)
data = data.withColumn("length", length(col("text")))

# Group by topic and compute statistics
stats = (
    data.groupBy("topic")
    .agg(
        count("*").alias("article_count"),
        avg("length").alias("avg_length"),
        min("length").alias("min_length"),
        max("length").alias("max_length")
    )
    .orderBy("article_count", ascending=False)
)

# Show Data
data.select("id", "title", "topic", "length").show()

# Show the statistics
stats.show(truncate=False)