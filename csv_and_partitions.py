from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PrepareshSpark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("/home/interview/orders.csv", header=True, inferSchema=True)

num_partitions = df.rdd.getNumPartitions()
print(f"number of partitions = {num_partitions}")

spark.stop()
