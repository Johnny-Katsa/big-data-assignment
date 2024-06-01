from pyspark.sql import SparkSession

CRIME_DATA_CSV_PATH = "hdfs://master:9000/csv/Crime_Data"
STATION_LOCATIONS_CSV_PATH = "hdfs://master:9000/data/LAPD_Police_Stations_long_lat.csv"

#####################################################################
# Preparation
#####################################################################
spark = SparkSession.builder \
    .appName("Query 4 - Repartition Join") \
    .getOrCreate()

crime_data_rdd = spark.read.csv(CRIME_DATA_CSV_PATH, header=True, inferSchema=True).rdd
police_stations_rdd = spark.read.csv(STATION_LOCATIONS_CSV_PATH, header=True, inferSchema=True).rdd

#####################################################################
# Repartitioning
#####################################################################
# Converting datasets to dictionary key-value pairs which can be repartitioned
crimes_key_values = crime_data_rdd.map(lambda x: (x['AREA'], x))
police_stations_key_values = police_stations_rdd.map(lambda x: (x['PREC'], x))

# Repartitioning with the station AREA/PREC as key
# We have two workers with 2 cores each, so we will repartition to 4 parts.
crime_data_repartitioned = crimes_key_values.partitionBy(4)
police_stations_repartitioned = police_stations_key_values.partitionBy(4)

#####################################################################
# Join operation
#####################################################################
joined_rdd = crime_data_repartitioned.join(police_stations_repartitioned)

# Printing head of result
for result in joined_rdd.take(5):
    print(result)

spark.stop()
