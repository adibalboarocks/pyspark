from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("PrepareshSpark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load the CSV, write your solution, and save the result as result_df
dataset_df=spark.read.csv("/home/interview/videos.csv",header=True)
result_df=dataset_df.filter((col("release_year")>=2019) & (col("view_count")>1000000))

# --- Do not edit below this line ---
result_df.coalesce(1).write.csv("/home/interview/output", header=True, mode="overwrite")
spark.stop()
