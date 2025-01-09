from sys import argv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace

# Retrieve command-line arguments
input_path = argv[1]
output_path = argv[2]

# Create a Spark session with the specified application name
spark = SparkSession.builder.appName("DataCleaningAndNormalization").getOrCreate()

# Load the input data as a Spark DataFrame from Parquet
data = spark.read.parquet(input_path)

# Print the data before cleaning for reference
print("Data before cleaning:")
data.show()

# Remove rows with null values in any column
data_cleaned = data.dropna()

# Remove all non-digit characters from the 'prev_id' and 'curr_id' columns
data_cleaned = data_cleaned.withColumn("prev_id", regexp_replace(col("prev_id"), r"[^\d]", ""))
data_cleaned = data_cleaned.withColumn("curr_id", regexp_replace(col("curr_id"), r"[^\d]", ""))

# Filter out rows where 'prev_id' or 'curr_id' cannot be cast to integers
data_cleaned = data_cleaned.filter((col("prev_id").cast("int").isNotNull()) & 
                                   (col("curr_id").cast("int").isNotNull()))

# Trim whitespace from all column values
data_cleaned = data_cleaned.select(
    *[trim(col(column)).alias(column) for column in data_cleaned.columns]
)

# Print the cleaned data for verification
print("Data after cleaning:")
data_cleaned.show()

# Write the cleaned data to the specified output path in Parquet format
data_cleaned.write.mode("overwrite").parquet(output_path)

# Stop the Spark session
spark.stop()
