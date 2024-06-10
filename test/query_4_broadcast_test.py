from pyspark.sql import SparkSession, Row

CRIME_DATA_CSV_PATH = "hdfs://master:9000/csv/Crime_Data"
STATION_LOCATIONS_CSV_PATH = "hdfs://master:9000/data/LAPD_Police_Stations_long_lat.csv"

#####################################################################
# Preparation
#####################################################################
spark = SparkSession.builder \
    .appName("Query 4 - Broadcast Join") \
    .getOrCreate()

crime_data_rdd = spark.read.csv(CRIME_DATA_CSV_PATH, header=True, inferSchema=True).rdd
police_stations_rdd = spark.read.csv(STATION_LOCATIONS_CSV_PATH, header=True, inferSchema=True).rdd

#####################################################################
# Broadcasting
#####################################################################
# We could have broadcast the dataframe like so, but we will work with RDDs as requested.
# police_stations_df = spark.read.csv("path/to/la_police_stations.csv", header=True)
# broadcast_stations = spark.sparkContext.broadcast(police_stations.collect())

# Convert stations to dictionary key-value pairs which can be broadcast
police_stations_key_values = (police_stations_rdd
                              .map(lambda x: (x['PREC'], x))
                              .collectAsMap())

# Broadcasting the smaller table to all nodes.
broadcast_stations = spark.sparkContext.broadcast(police_stations_key_values)


#####################################################################
# Join operations
#####################################################################

#####################################################################
#        S O L U T I O N   1
#####################################################################
def broadcast_join(crime_row):
    stations_hash_table = broadcast_stations.value

    # We know there is only going to be one match here since it's a one-to-many relationship.
    # A many-to-many would require more special handling, now and earlier in the broadcast stage.
    matched_station_row = stations_hash_table.get(crime_row['AREA'])
    combined_rows = Row(**(crime_row.asDict() | matched_station_row.asDict()))

    return combined_rows


# Execution of the join operation
joined_rdd = crime_data_rdd.map(broadcast_join)

# Printing part of the result to demonstrate the join.
first_five_rows = joined_rdd.take(5)
print("\n" + "#" * 100)
print("Showing first 5 rows with just a few columns for demonstration.")
print("#" * 100 + "\n")
print("DR_NO, AREA, DIVISION, PREC")
for row in first_five_rows:
    print(row['DR_NO'], row['AREA'], row['DIVISION'], row['PREC'])


#####################################################################
#        S O L U T I O N   2
#####################################################################

# This solution is the same as the first, we just make the for loop
# applied to each partition more "visible".

def broadcast_join2(crimes_partition):
    combined_rows = []
    for crime_row in crimes_partition:
        stations_hash_table = broadcast_stations.value

        # We know there is only going to be one match here since it's a one-to-many relationship.
        # A many-to-many would require more special handling, now and earlier in the broadcast stage.
        matched_station_row = stations_hash_table.get(crime_row['AREA'])
        combined_rows.append(Row(**(crime_row.asDict() | matched_station_row.asDict())))

    return iter(combined_rows)


# Execution of the join operation
joined_rdd = crime_data_rdd.mapPartitions(broadcast_join2)

# Printing part of the result to demonstrate the join.
first_five_rows = joined_rdd.take(5)
print("\n" + "#" * 100)
print("Showing first 5 rows with just a few columns for demonstration.")
print("#" * 100 + "\n")
print("DR_NO, AREA, DIVISION, PREC")
for row in first_five_rows:
    print(row['DR_NO'], row['AREA'], row['DIVISION'], row['PREC'])

spark.stop()
