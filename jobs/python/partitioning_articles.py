from sys import argv
from pyspark.sql import SparkSession

# Retrieve command-line arguments:
# - `number_of_partitions`: The number of partitions to use for the data
# - `input_path`: Path to the input JSON file(s)
# - `output_path`: Path to save the partitioned output JSON file(s)
number_of_partitions = int(argv[1])
input_path = argv[2]
output_path = argv[3]

# Initialize a Spark session with an application name "Partitioning"
spark = SparkSession.builder.appName("Partitioning").getOrCreate()

# Read the JSON data from the specified input path
data = spark.read.json(input_path)

# Show the data before partitioning for verification
print("Data read (before partitioning):")
data.show()

# Repartition the data into the specified number of partitions
data_partitioned = data.repartition(number_of_partitions)

# Show the data after partitioning to verify partitioning effect
print("Data read (after partitioning):")
data_partitioned.show()
print(f"Number of partitions: {data_partitioned.rdd.getNumPartitions()}")

# Write the partitioned data to the specified output path, overwriting any existing data
data_partitioned.write.mode("overwrite").json(output_path)

# Stop the Spark session to free up resources
spark.stop()
