from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType

CRIME_DATA_CSV_PATH = "hdfs://master:9000/csv/Crime_Data"
STATION_LOCATIONS_CSV_PATH = "hdfs://master:9000/data/LAPD_Police_Stations_long_lat.csv"

#####################################################################
# Preparation
#####################################################################
spark = SparkSession.builder \
    .appName("Repartition Join Verification") \
    .getOrCreate()

crime_data_df = spark.read.csv(CRIME_DATA_CSV_PATH, header=True, inferSchema=True)
police_stations_df = spark.read.csv(STATION_LOCATIONS_CSV_PATH, header=True, inferSchema=True)
crime_data_df = crime_data_df.select('DATE OCC', 'AREA')
police_stations_df = police_stations_df.select('DIVISION', 'PREC')

crime_data_rdd = crime_data_df.rdd
police_stations_rdd = police_stations_df.rdd

combined_schema_fields = {field.name: field for field in crime_data_df.schema}
for field in police_stations_df.schema:
    combined_schema_fields[field.name] = field

combined_schema = StructType(list(combined_schema_fields.values()))

# Extracting RDD. Will use just a few columns since the join is for demonstration purposes
crime_data_rdd = crime_data_df.rdd
police_stations_rdd = police_stations_df.rdd

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

spark.createDataFrame(joined_rdd, schema=combined_schema).createOrReplaceTempView("joined_data1")
del joined_rdd
#
# #####################################################################
# #     S O L U T I O N   2
# #####################################################################
#
# #####################################################################
# # Repartitioning
# #####################################################################
# # Converting datasets to dictionary key-value pairs which can be repartitioned
# crimes_key_values = crime_data_rdd.map(lambda x: (x['AREA'], x))
# police_stations_key_values = police_stations_rdd.map(lambda x: (x['PREC'], x))
#
# # Repartitioning with the station AREA/PREC as key
# # We have two workers with 2 cores each, so we will repartition to 4 parts.
# crime_data_repartitioned = crimes_key_values.partitionBy(4)
# police_stations_repartitioned = police_stations_key_values.partitionBy(4)
#
# #####################################################################
# # Join operation
# #####################################################################
# joined_rdd2 = crime_data_repartitioned.join(police_stations_repartitioned)
#
# spark.createDataFrame(joined_rdd2, schema=combined_schema).createOrReplaceTempView("joined_data2")
# del joined_rdd2
#
#
# ##################################################################
# #                 V E R I F I C A T I O N
# ##################################################################
# # Verifying the join results with a simple query. We will find the number of crimes
# # from 2015 per division. We'll do this with the tables we found via the algorithms
# # and compare the results with the same query using the SQL API.
#
# crime_data_df.createOrReplaceTempView("crime_data")
# police_stations_df.createOrReplaceTempView("police_stations")
# joined_data_alternatives = ["joined_data1", "joined_data2", "crime_data JOIN police_stations ON AREA = PREC"]
#
# results_for_each_solution = []
#
# for joined_data in joined_data_alternatives:
#     query = f"""
#         SELECT DIVISION, count(*) FROM {joined_data}
#         WHERE SUBSTRING(`DATE OCC`, 7, 4) = 2015
#         GROUP BY DIVISION
#         ORDER BY DIVISION
#     """
#
#     results_for_each_solution.append(spark.sql(query).show(100))
#
# for results in results_for_each_solution:
#     print("\n" + "#" * 100)
#     print(results)
#     print("#" * 100 + "\n")

spark.stop()
