from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("PrepareshSpark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("/home/interview/orders.csv", header=True, inferSchema=True)

df.cache()

start = time.time()
df.count()
first_run = time.time() - start

start = time.time()
df.count()
second_run = time.time() - start

is_faster = second_run < first_run

print(f"first run = {first_run:.2f}s")
print(f"second run = {second_run:.2f}s")
print(f"faster after caching = {str(is_faster).lower()}")

spark.stop()
