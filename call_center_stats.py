from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("PrepareshSpark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load the CSVs, write your solution, and save the result as result_df
calls_df=spark.read.csv("/home/interview/calls.csv", header=True)
customers_df=spark.read.csv("/home/interview/customers.csv", header=True)

joined_df=calls_df.join(customers_df,calls_df.cust_id==customers_df.cust_id,"inner")
result_df=joined_df.groupBy(calls_df.date).agg(F.count_distinct(calls_df.cust_id).alias("num_customers"), F.sum(calls_df.duration).alias("total_duration"))
# --- Do not edit below this line ---
result_df.coalesce(1).write.csv("/home/interview/output", header=True, mode="overwrite")
spark.stop()
