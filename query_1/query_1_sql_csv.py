from pyspark.sql import SparkSession

DATA_CSV_PATH = "hdfs://master:9000/csv/Crime_Data"

#############################
# Preparation
#############################
spark = SparkSession.builder \
    .appName("Query 1 - SQL API") \
    .getOrCreate()

df = spark.read.csv(DATA_CSV_PATH, header=True, inferSchema=True)


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


spark.stop()

