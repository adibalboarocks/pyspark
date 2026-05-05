from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PrepareshSpark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("/home/interview/orders.csv", header=True, inferSchema=True)
a_df=df.rdd.repartition(8)
# Repartition the DataFrame to 8 partitions and print the task count
# in the format: task count = <N>
print("task count =", a_df.getNumPartitions())
spark.stop()
