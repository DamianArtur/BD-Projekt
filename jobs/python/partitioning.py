from sys import argv
from pyspark.sql import SparkSession

number_of_partitions = int(argv[1])
input_path = argv[2]
output_path = argv[3]
delimiter = argv[4]
mode = argv[5]
is_header = argv[6]

spark = SparkSession.builder.appName("Partitioning").getOrCreate()

data = spark.read.option("delimiter", delimiter).csv(input_path, header=True, inferSchema=True)

print("Wczytane dane (przed partycjonowaniem):")
data.show()

data_partitioned = data.repartition(number_of_partitions)

print("Wczytane dane (po partycjonowaniu):")
data_partitioned.show()
print(f"Liczba partycji: {data_partitioned.rdd.getNumPartitions()}")

data_partitioned.write.mode(mode).option("header", is_header).csv(output_path)


spark.stop()