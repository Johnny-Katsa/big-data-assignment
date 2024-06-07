from pyspark.sql import SparkSession

DATA_CSV_PATH = "hdfs://master:9000/csv/Crime_Data"

#############################
# Preparation
#############################
spark = SparkSession.builder \
    .appName("Query 2 - SQL API") \
    .getOrCreate()

df = spark.read.csv(DATA_CSV_PATH, header=True)


#############################
# Querying
#############################
df.createOrReplaceTempView("crime_data")

query = """

SELECT count(*) as incidents,
CASE
WHEN `TIME OCC` >=  500 AND `TIME OCC` <= 1159 THEN 'Morning'
WHEN `TIME OCC` >= 1200 AND `TIME OCC` <= 1659 THEN 'Afternoon'
WHEN `TIME OCC` >= 1700 AND `TIME OCC` <= 2059 THEN 'Evening'
WHEN `TIME OCC` >= 2100 OR `TIME OCC`  <=  459 THEN 'Night'
ELSE 'NA'
END AS time_segment
FROM crime_data 
WHERE `Premis Desc` = 'STREET'
GROUP BY time_segment 
ORDER BY incidents DESC

"""

result = spark.sql(query)
result.explain(True)
result.show(1000)


spark.stop()

