import os
from sys import argv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import numpy as np
import matplotlib.pyplot as plt

clickstream_data_path = argv[1]

# Create a Spark session
spark = SparkSession.builder.appName("ClickstreamStatistics").getOrCreate()

# Load clickstream data
clickstream_data = spark.read.option("header", "true").csv(clickstream_data_path)

# Path to plot file
output_dir = "/opt/data"
output_path = os.path.join(output_dir, "long_tail_curve.png")

# Data process
n_values = clickstream_data.select("n").rdd.flatMap(lambda x: x).collect()
n_values = [int(n) for n in n_values]
n_values_sorted = np.sort(n_values)[::-1]  # Descending order

# Plot making
plt.figure(figsize=(10, 6))
plt.plot(n_values_sorted, linewidth=2, color='b')

plt.title("Long Tail for Clickstream Data")
plt.xlabel("Rank")
plt.ylabel("Number of Clicks (n)")
plt.yscale("log")
plt.grid()
plt.show()

plt.savefig(output_path)
plt.close()

print(f"Wykres został zapisany pod ścieżką: {output_path}")