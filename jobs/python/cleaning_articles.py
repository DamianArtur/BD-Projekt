from sys import argv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace

# Retrieve command-line arguments
input_path = argv[1]
output_path = argv[2]
mode = argv[3]

# Initialize a Spark session with an application name
spark = SparkSession.builder.appName("JsonDataCleaning").getOrCreate()

# Read the JSON data from the specified input path
data = spark.read.json(input_path)

print("Data before cleaning:")
data.show()

# Remove rows with null values in any column
data_cleaned = data.dropna()

# Remove all non-digit characters from the `id` field
data_cleaned = data_cleaned.withColumn("id", regexp_replace(col("id"), r"[^\d]", ""))

# Filter out rows where `id` cannot be cast to integers
data_cleaned = data_cleaned.filter(col("id").cast("int").isNotNull())

# Trim whitespace from the `text` and `title` fields
data_cleaned = data_cleaned.select(
    col("id"),  # Keep `id` as it is after cleaning
    trim(col("text")).alias("text"),  # Trim whitespace from `text`
    trim(col("title")).alias("title")  # Trim whitespace from `title`
)

print("Data after cleaning:")
data_cleaned.show()

# Write the cleaned data to the specified output path in the desired mode
data_cleaned.write.mode(mode).json(output_path)

# Stop the Spark session
spark.stop()
