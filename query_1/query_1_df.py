from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

DATA_PARQUET_PATH = "parquet/Crime_Data"

spark = SparkSession.builder \
    .appName("Query 1 - DataFrame API") \
    .getOrCreate()

df = spark.read.parquet(f"hdfs://master:9000/{DATA_PARQUET_PATH}", header=True, inferSchema=True)

print(f"###########################")
print(f"Cols of the df: {df.columns}")

window_spec = Window.partitionBy("year").orderBy(col("crime_total").desc())

(df
 .withColumn("Year", substring("DATE OCC", 7, 4))
 .withColumn("Month", substring("DATE OCC", 1, 2))
 .groupBy("Year", "Month")
 .count()
 .orderBy("Year", "Month")
 .withColumnRenamed("count", "crime_total")
 .withColumn("rank", rank().over(window_spec))
 .filter("rank <= 3")
 .show(1000))

spark.stop()

