from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("Analytics-Testing").config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.2.0").getOrCreate()

file_format = "avro"

# Objective: Bring all records from Brazil with Artist = Shakira
start = time.time()
df = spark.read.format(file_format).load(f"./chart.{file_format}")
df = df.select("artist", "date", "region", "streams")
df = df.filter((df.artist == 'Shakira') & (df.region == 'Brazil'))
df.show()
print(f"==== Executed in {time.time() - start} seconds ====")

