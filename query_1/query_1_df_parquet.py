from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

DATA_PARQUET_PATH = "hdfs://master:9000/parquet/Crime_Data"

#############################
# Preparation
#############################
spark = SparkSession.builder \
    .appName("Query 1 - DataFrame API - PARQUET") \
    .getOrCreate()

df = spark.read.parquet(DATA_PARQUET_PATH, header=True, inferSchema=True)


##############################
# Querying
##############################
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

