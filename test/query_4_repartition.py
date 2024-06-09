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
#     S O L U T I O N   1
#####################################################################

#####################################################################
# Repartitioning
#####################################################################
# Converting datasets to dictionary key-value pairs which can be repartitioned
crimes_key_values = crime_data_rdd.map(lambda x: (x['AREA'], (x, 'crime')))
police_stations_key_values = police_stations_rdd.map(lambda x: (x['PREC'], (x, 'station')))

# united = crimes_key_values.union(police_stations_key_values)


def my_reduce(key):
    return [1, 2, 3]


# united = united.reduceByKey(my_reduce)

united = police_stations_key_values.reduceByKey(lambda x, y: 1)

# joined_rdd = ...

# Printing head of result
print("\n" + "#" * 100)
print("Some results from the second solution.")
print("#" * 100 + "\n")
for result in united.collect():
    print(result)

for result in police_stations_key_values.collect():
    print(result)

spark.stop()
