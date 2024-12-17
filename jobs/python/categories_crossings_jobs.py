from sys import argv

from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, Word2Vec
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

clickstream_data_path = argv[1]
articles_data_path = argv[2]

spark = SparkSession.builder.appName("JsonDataCleaning").getOrCreate()

clickstream_data = spark.read.option("header", "true").csv(clickstream_data_path)
articles_data = spark.read.json(articles_data_path)

# clickstream_data.show()
# articles_data.show()

clickstream_with_text = clickstream_data.join(
    articles_data, 
    clickstream_data.curr_id == articles_data.id, 
    "inner"
).select(
    "prev_id", "curr_id", "n", "prev_title", "curr_title", "type", "text"
)

# Tokenize article texts
tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
tokenized_articles = tokenizer.transform(clickstream_with_text)

# Train Word2Vec Model
word2vec = Word2Vec(vectorSize=100, minCount=1, inputCol="tokens", outputCol="vector")
word2vec_model = word2vec.fit(tokenized_articles)

# Transform data to include word vectors
articles_with_vectors = word2vec_model.transform(tokenized_articles)

# List of topics
topics = [
    "sports", "history", "science", "technology", "art", 
    "geography", "literature", "music", "philosophy", "politics", 
    "biology", "physics", "mathematics", "economics", "psychology", 
    "engineering", "medicine", "astronomy", "education", "architecture"
]

# Create DataFrame with topics
topics_df = spark.createDataFrame([(topic,) for topic in topics], ["topic"])

# Tokenize topics
tokenizer.setInputCol("topic")
tokenizer.setOutputCol("tokens")
tokenized_topics_df = tokenizer.transform(topics_df)

# Generate topic vectors
topics_with_vectors = word2vec_model.transform(tokenized_topics_df)

# Collect topic vectors
topics_vectors = [row['vector'] for row in topics_with_vectors.collect()]

# Function to classify topic based on cosine similarity
def classify_topic(vector, topics_vectors, topics):
    if vector is None:
        return "unknown"
    similarities = cosine_similarity([vector], topics_vectors)[0]
    max_index = np.argmax(similarities)
    return topics[max_index]

# Register UDF for classification
classify_topic_udf = udf(lambda vec: classify_topic(vec, topics_vectors, topics), StringType())

# Add topic classification to articles
articles_with_topics = articles_with_vectors.withColumn("topic", classify_topic_udf(col("vector")))

# Join for prev_id to get previous article's topic
clickstream_with_topics = articles_with_topics.alias("curr").join(
    articles_with_topics.alias("prev"),
    col("curr.prev_id") == col("prev.curr_id"),
    "left"
).select(
    col("curr.prev_id"), col("curr.curr_id"), col("curr.topic").alias("current_topic"),
    col("prev.topic").alias("previous_topic"), col("curr.n")
)

# Group by transitions and count occurrences
clickstream_with_topics = clickstream_with_topics.withColumn("n", col("n").cast("int"))
transitions = clickstream_with_topics.groupBy("previous_topic", "current_topic").sum("n")

# Show results
transitions.show()