from email import header
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("employee").getOrCreate()

input_file_path = "data/employees.csv"

df = spark.read.csv(input_file_path, header=True, inferSchema=True)
df.show()

agg_df = df.groupBy("department_id").agg({"salary":"sum"})

agg_df.write.csv("data/aggregated_employee.csv",header=True)

spark.stop()
# /media/manu/sec_storage/project_2024_2u/projects24/pyspark_prac/data/employees.csv