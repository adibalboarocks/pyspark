from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("PrepareshSpark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

employees = spark.read.csv("/home/interview/employees.csv", header=True, inferSchema=True)
payroll = spark.read.csv("/home/interview/payroll.csv", header=True, inferSchema=True)

# Write your logic here and save it to result_df
joined_df=employees.join(payroll,"employee_id")
calculated_df=joined_df.withColumn("pay", F.when(F.col("hours_worked")>40,40*F.col("hourly_rate")+(F.col("hours_worked")-40)*1.5*F.col("hourly_rate"))\
.otherwise(F.col("hours_worked")*F.col("hourly_rate")))
result_df=calculated_df.select("employee_id","name","position","pay")
print(result_df.show())
# --- Do not edit below this line ---
result_df.coalesce(1).write.csv("/home/interview/output", header=True, mode="overwrite")
spark.stop()
