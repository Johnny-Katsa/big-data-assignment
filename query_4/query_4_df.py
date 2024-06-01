from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, avg, count, round, desc
from pyspark.sql.types import DoubleType
import math

CRIME_DATA_CSV_PATH = "hdfs://master:9000/csv/Crime_Data"
STATION_LOCATIONS_CSV_PATH = "hdfs://master:9000/data/LAPD_Police_Stations_long_lat.csv"


################################################
# Distance UDF
################################################
def haversine(lat1, lon1, lat2, lon2):
    # Converting longitudes and latitudes to radians.
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Applying the Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))

    # The radius of earth is approximately 6371 kilometers.
    km = 6371 * c
    return km


distance_udf = udf(lambda lat1, long1, lat2, long2: haversine(lat1, long1, lat2, long2), DoubleType())

#####################################################################
# Preparation
#####################################################################
spark = SparkSession.builder \
    .appName("Query 4 - DataFrame API") \
    .getOrCreate()

df_crimes = spark.read.csv(CRIME_DATA_CSV_PATH, header=True, inferSchema=True)
df_stations = spark.read.csv(STATION_LOCATIONS_CSV_PATH, header=True, inferSchema=True)

#############################################################
# Querying
#############################################################
results = (df_crimes
           .filter("LON != 0.0 AND LAT != 0.0")
           .filter("100 <= `Weapon Used Cd` AND `Weapon Used Cd` <= 199")
           .join(df_stations, df_crimes["AREA "] == df_stations["PREC"], how="inner")
           .withColumn("distance_km", distance_udf(col("LAT"), col("LON"), col("y"), col("x")))
           .groupBy("DIVISION")
           .agg(round(avg(col("distance_km")), 3).alias("avg_distance_km"), count("*").alias("incidents total"))
           .orderBy(desc("incidents total")))

print(results.show(df_stations.count()))
spark.stop()
