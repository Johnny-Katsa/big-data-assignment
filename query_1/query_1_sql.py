from pyspark.sql import SparkSession
from config import DATA_PARQUET_PATH
from helper import intro_print, outro_print

#############################
# Preparation
#############################
spark = SparkSession.builder \
    .appName("Query 1 - SQL API") \
    .getOrCreate()

intro_print(spark.sparkContext.appName)

df = spark.read.parquet(DATA_PARQUET_PATH, header=True, inferSchema=True)


#############################
# Querying
#############################
df.createOrReplaceTempView("crime_data")

query = """

WITH rankings AS (
    SELECT year, month, crime_total, RANK() OVER (PARTITION BY year ORDER BY crime_total DESC) as ranking
    FROM (
        SELECT SUBSTRING(`DATE OCC`, 7, 4) as year, SUBSTRING(`DATE OCC`, 1, 2) as month, COUNT(*) as crime_total
        FROM crime_data
        GROUP BY year, month
    )
)
SELECT * FROM rankings WHERE ranking <= 3 ORDER BY year ASC, ranking ASC

"""

result = spark.sql(query)
result.show(1000)

outro_print()
spark.stop()

