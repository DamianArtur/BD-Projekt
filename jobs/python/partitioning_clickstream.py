from sys import argv
from pyspark.sql import SparkSession

# Retrieve command-line arguments
number_of_partitions = int(argv[1])
input_path = argv[2]
output_path = argv[3]
delimiter = argv[4]
mode = argv[5]
is_header = argv[6]

# Initialize a Spark session with an application name "Partitioning"
spark = SparkSession.builder.appName("Partitioning").getOrCreate()

# Read the CSV data from the input path, using the specified delimiter and setting `header=True` to infer the schema
data = spark.read.option("delimiter", delimiter).csv(input_path, header=True)

# Show the data before partitioning for verification
print("Data read (before partitioning):")
data.show()

# Repartition the data into the specified number of partitions
data_partitioned = data.repartition(number_of_partitions)

# Show the data after partitioning to verify partitioning effect
print("Data read (after partitioning):")
data_partitioned.show()
print(f"Number of partitions: {data_partitioned.rdd.getNumPartitions()}")

# Write the partitioned data to the output path in Parquet format, using the specified mode
data_partitioned.write.mode(mode).parquet(output_path)

# Stop the Spark session
spark.stop()
