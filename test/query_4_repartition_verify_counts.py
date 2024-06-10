from pyspark.sql import SparkSession

CRIME_DATA_CSV_PATH = "hdfs://master:9000/csv/Crime_Data"
STATION_LOCATIONS_CSV_PATH = "hdfs://master:9000/data/LAPD_Police_Stations_long_lat.csv"

#####################################################################
# Preparation
#####################################################################
spark = SparkSession.builder \
    .appName("Query 4 - Repartition Join") \
    .getOrCreate()

crime_data_df = spark.read.csv(CRIME_DATA_CSV_PATH, header=True, inferSchema=True)
police_stations_df = spark.read.csv(STATION_LOCATIONS_CSV_PATH, header=True, inferSchema=True)

# Extracting RDD. Will use just a few columns since the join is for demonstration purposes
crime_data_rdd = crime_data_df.select('DR_NO', 'AREA').rdd
police_stations_rdd = police_stations_df.select('OBJECTID', 'PREC').rdd

#####################################################################
#     S O L U T I O N   1
#####################################################################

# Converting datasets to dictionary key-value pairs. The key is the join column. Also adding a
# "tag" to each value, so the later stages can figure out which table each record has come from.
crimes_key_values = crime_data_rdd.map(lambda x: (x['AREA'], (x, 'crime')))
police_stations_key_values = police_stations_rdd.map(lambda x: (x['PREC'], (x, 'station')))

# Concatenating the two RDDs to allow grouping by key, regardless of source table.
united = crimes_key_values.union(police_stations_key_values)


def my_reduce(crime_or_station_records):
    """
    Repartition join reduce function based on A.1 of
    Blanas, Spyros, et al. 'A comparison of join algorithms for log processing in mapreduce.'
    Proceedings of the 2010 ACM SIGMOD International Conference on Management of data. 2010.
    """

    # Collecting crime and station records into two separate buffers.
    # Decision is made based on the tag each record carries.
    crimes_buffer = []
    stations_buffer = []
    for record, tag in crime_or_station_records[1]:
        if tag == 'crime':
            crimes_buffer.append(record)
        elif tag == 'station':
            stations_buffer.append(record)
        else:
            raise Exception("Unexpected tag was found!")

    # Since all records here have the same key,
    # we combine every crime and station combination into a single record.
    combined_rows = []
    for crime in crimes_buffer:
        for station in stations_buffer:
            combined_rows.append((crime, station))

    del crimes_buffer
    del stations_buffer
    return combined_rows


# Collecting by key to apply the reduce step of map-reduce. Reduce is applied via flatMap.
joined_rdd = united.groupByKey().flatMap(my_reduce)

count1 = joined_rdd.count()

del joined_rdd

#####################################################################
#     S O L U T I O N   2
#####################################################################

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
count2 = joined_rdd.count()

del joined_rdd

count3 = (crime_data_df
          .select('DR_NO', 'AREA')
          .join(police_stations_df
                .withColumnRenamed("PREC", "AREA")
                .select('OBJECTID', 'AREA'), on='AREA')
          .count())

print(f"C1: {count1}, C2: {count2}, C3: {count3}")
#

spark.stop()
