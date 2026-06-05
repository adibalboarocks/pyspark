from pyspark.sql import SparkSession
import pyspark.sql.functions as F
spark = SparkSession.builder.appName("PrepareshSpark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

products=spark.read.csv("/home/interview/products.csv",header=True)
sales=spark.read.csv("/home/interview/sales.csv",header=True)
inventory=spark.read.csv("/home/interview/inventory.csv",header=True)


sales_agg = sales.groupBy("product_id").agg(
    F.sum("quantity").alias("total_quantity"),
    F.sum("revenue").alias("total_revenue")
)

stock_agg = inventory.groupBy("product_id").agg(
    F.sum("stock").alias("total_stock")
)

result_df = products.join(sales_agg, on="product_id", how="left") \
    .join(stock_agg, on="product_id", how="left")

result_df = result_df.withColumn("total_quantity", F.coalesce("total_quantity", F.lit(0))) \
    .withColumn("total_revenue", F.coalesce("total_revenue", F.lit(0))) \
    .withColumn("total_stock", F.coalesce("total_stock", F.lit(0)))

# --- Do not edit below this line ---
result_df.coalesce(1).write.csv("/home/interview/output", header=True, mode="overwrite")
spark.stop()
