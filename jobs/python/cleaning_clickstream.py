from sys import argv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace

# Retrieve command-line arguments for input and output file paths, header option, and write mode
input_path = argv[1]  # Path to the input CSV file
output_path = argv[2]  # Path to save the cleaned CSV file
is_header = argv[3]  # Whether the input CSV file has a header ("true" or "false")
mode = argv[4]  # Write mode ("overwrite", "append", etc.)

# Create a Spark session with the specified application name
spark = SparkSession.builder.appName("DataCleaningAndNormalization").getOrCreate()

# Load the input data as a Spark DataFrame, setting the header option dynamically
data = spark.read.option("header", is_header).csv(input_path)

# Print the data before cleaning for reference
print("Data before cleaning:")
data.show()

# Step 1: Remove rows with null values in any column
data_cleaned = data.dropna()

# Step 2: Remove all non-digit characters from the 'prev_id' and 'curr_id' columns
data_cleaned = data_cleaned.withColumn("prev_id", regexp_replace(col("prev_id"), r"[^\d]", ""))
data_cleaned = data_cleaned.withColumn("curr_id", regexp_replace(col("curr_id"), r"[^\d]", ""))

# Step 3: Filter out rows where 'prev_id' or 'curr_id' cannot be cast to integers
data_cleaned = data_cleaned.filter((col("prev_id").cast("int").isNotNull()) & 
                                   (col("curr_id").cast("int").isNotNull()))

# Step 4: Trim whitespace from all column values
data_cleaned = data_cleaned.select(
    *[trim(col(column)).alias(column) for column in data_cleaned.columns]
)

# Print the cleaned data for verification
print("Data after cleaning:")
data_cleaned.show()

# Write the cleaned data to the specified output path in the desired mode
data_cleaned.write.mode(mode).option("header", is_header).csv(output_path)

# Stop the Spark session to free up resources
spark.stop()
