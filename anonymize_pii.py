from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("PrepareshSpark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("/home/interview/users.csv", header=True, inferSchema=True)

email_domain = F.regexp_extract(F.col("email"), r"@(.+)", 1)
df = df.withColumn("email_domain", email_domain)

anon_phone = F.regexp_replace(F.col("phone").cast("string"), r"^\d{6}", "******")
df = df.withColumn("anon_phone", anon_phone)

result_df = df.select("user_id", "email_domain", "anon_phone")

# --- Do not edit below this line ---
result_df.coalesce(1).write.csv("/home/interview/output", header=True, mode="overwrite")
spark.stop()
