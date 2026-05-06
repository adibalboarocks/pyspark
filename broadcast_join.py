from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("PrepareshSpark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

orders_df = spark.read.csv("/home/interview/orders.csv", header=True, inferSchema=True)
customers_df = spark.read.csv("/home/interview/customers.csv", header=True, inferSchema=True)

joined_df = orders_df.join(broadcast(customers_df), "customer_id")

city_counts = joined_df.groupBy("city").count()
distinct_cities = city_counts.count()

print(f"cities with orders = {distinct_cities}")

spark.stop()
