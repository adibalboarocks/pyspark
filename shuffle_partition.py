from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("PrepareshSpark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("/home/interview/orders.csv", header=True, inferSchema=True)

# Find the order status with the highest count and print in the format:
# most common status = <status>, count = <N>
df_grouped=df.groupBy("status").agg(F.count("status").alias("total_count"))
df_grouped_filtered=df_grouped.orderBy("total_count", ascending=False).first()
print(f"most common status = {df_grouped_filtered['status']}, count = {df_grouped_filtered['total_count']}")
spark.stop()
