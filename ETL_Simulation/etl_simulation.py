from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder.appName("ETL_testing").config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.2.0").getOrCreate()

FORMAT = "avro"
# Objective:
# - Create column date - ok
# - Convert Lbs to Kg(T) - ok
# - Create column Reals_Sold(M)
# - Do some aggregation

start = time.time()
df = spark.read.format("orc").load(f"./exportacao_full.orc")

df = df.withColumn("Date",date_format(concat_ws("-",col("Year"),col("Month")), "yyyy-MM"))
df = df.withColumn("Net_Weight", round(df.Net_Weight * 0.000453, 2))
df = df.withColumn("Reals_Sold", round(df.Dollar_Sold * 0.00000525, 2))
df = df.groupBy("Country", "Date", "Description").agg(round(sum("Reals_Sold"), 2).alias("Reals_Sold"), \
                                       round(sum("Net_Weight"), 2).alias("Net_Weight"))

df.show()

df.write.mode('overwrite').format(FORMAT).save(f"ouput.{FORMAT}")
print(f"==== Executed in {time.time() - start} seconds ====")