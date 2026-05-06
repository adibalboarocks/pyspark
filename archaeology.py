from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("PrepareshSpark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

artifacts = spark.read.csv("/home/interview/artifacts.csv", header=True, inferSchema=True)

# Step 1: Filter for records where Quantity is strictly greater than 100
filtered_df = artifacts.filter(F.col("Quantity") > 100)

# Step 2: Convert the 'Material' column strings to uppercase
result_df = filtered_df.withColumn("Material", F.upper(F.col("Material")))

# Step 3: Select columns to ensure schema order
result_df = result_df.select("ID", "Item", "Period", "Material", "Quantity")

# --- Do not edit below this line ---
result_df.coalesce(1).write.csv("/home/interview/output", header=True, mode="overwrite")
spark.stop()
