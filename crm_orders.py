from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("PrepareshSpark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load the CSVs, join them, and save the result as result_df
customer=spark.read.csv("/home/interview/customers.csv",header=True)
orders=spark.read.csv("/home/interview/orders.csv",header=True)
customer_order_joined=customer.join(orders,"customer_id")
products=spark.read.csv("/home/interview/products.csv",header=True)
all=customer_order_joined.join(products,"product_id").select(F.col("order_id"),F.concat("first_name",lit(" "),"last_name").alias("customer_name"),F.col("email").alias("customer_email"),F.col("product_name"),F.col("category").alias("product_category"),F.col("order_date"))
print(all.head())
# --- Do not edit below this line ---
result_df=all
result_df.coalesce(1).write.csv("/home/interview/output", header=True, mode="overwrite")
spark.stop()
