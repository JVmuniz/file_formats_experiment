from pyspark.sql import SparkSession
import matplotlib, time

spark = SparkSession.builder.appName("Analytics-Testing").getOrCreate()



# Objective: Bring all records from Brazil with Artist = Shakira
start = time.time()
df = spark.read.format("orc").load("./file_formats/chart.orc")
df = df.select("artist", "date", "region", "streams")
df = df.filter((df.artist == 'Shakira') & (df.region == 'Brazil'))
df.show()
print(f"==== Executed in {time.time() - start} seconds ====")
# df_graph = df.toPandas()
# df_graph.plot(x="date", y="streams")