from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("PrepareshSpark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

portfolio = spark.read.csv("/home/interview/portfolio.csv", header=True, inferSchema=True)
prices = spark.read.csv("/home/interview/prices.csv", header=True, inferSchema=True)

# Write your code here and save it to result_df
joined_df = portfolio.join(prices, on="company", how="inner")

# Step 2: Multiply shares by closing_price to get the value of each individual holding per day
value_df = joined_df.withColumn("holding_value", F.col("shares") * F.col("closing_price"))

# Step 3: Group by PE_firm and date, then sum the holding values to get the total portfolio value
result_df = value_df.groupBy("PE_firm", "date").agg(
    F.sum("holding_value").cast("integer").alias("portfolio_value")
)

result_df = result_df.select(
    "PE_firm", "date", "portfolio_value"
).orderBy("PE_firm", "date")
# --- Do not edit below this line ---
result_df.coalesce(1).write.csv("/home/interview/output", header=True, mode="overwrite")
spark.stop()
