from pyspark import SparkConf
from pyspark.sql import SparkSession

CRIME_DATA_CSV_PATH = "hdfs://master:9000/csv/Crime_Data"
STATION_LOCATIONS_CSV_PATH = "hdfs://master:9000/data/LAPD_Police_Stations_long_lat.csv"

#####################################################################
# Preparation
#####################################################################
spark = SparkSession.builder \
    .appName("Query 4 - Repartition Join") \
    .config(conf=SparkConf().set("spark.memory.offHeap.enable", "true").set("spark.memory.offHeap.size", "1")) \
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
crimes_key_values = crime_data_rdd.map(lambda x: (x['AREA'], (x['DR_NO'], x['AREA'], 'crime')))
police_stations_key_values = police_stations_rdd.map(lambda x: (x['PREC'], (x['OBJECTID'], x['PREC'], 'station')))

united = crimes_key_values.union(police_stations_key_values)

united = united.partitionBy(4)


def my_reduce(crime_or_station_records):
    crimes_buffer = []
    stations_buffer = []
    for record, tag in crime_or_station_records[1]:
        if tag == 'crime':
            crimes_buffer.append(record)
        elif tag == 'station':
            stations_buffer.append(record)
        else:
            raise Exception("Unexpected tag was found!")

    combined_rows = []
    for crime in crimes_buffer:
        for station in stations_buffer:
            combined_rows.append((crime, station))

    del crimes_buffer
    del stations_buffer
    return combined_rows


# united = united.reduceByKey(my_reduce)

# Collecting by key to apply reduce step of map-reduce
joined_rdd = united.groupByKey().flatMap(my_reduce)

# Printing head of result
print("\n" + "#" * 100)
print("Some results from the second solution.")
print("#" * 100 + "\n")
for result in joined_rdd.collect():
    print(result)

spark.stop()
