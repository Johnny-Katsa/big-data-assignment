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

#####################################################################
#     S O L U T I O N   1
#####################################################################

# Converting datasets to dictionary key-value pairs. The key is the join column. Also adding a
# "tag" to each value, so the later stages can figure out which table each record has come from.
crimes_key_values = crime_data_rdd.map(lambda x: (x['AREA'], (x, 'crime')))
police_stations_key_values = police_stations_rdd.map(lambda x: (x['PREC'], (x, 'station')))

#####################################################################
# Repartitioning
#####################################################################
# Repartitioning with the station AREA/PREC as key
# We have two workers with 2 cores each, so we will repartition to 4 parts.
crime_data_repartitioned = crimes_key_values.partitionBy(4)
police_stations_repartitioned = police_stations_key_values.partitionBy(4)

#####################################################################
# Join operation
#####################################################################
joined_rdd2 = crime_data_repartitioned.join(police_stations_repartitioned)

# Printing head of result
results = joined_rdd2.take(10)
print("\n" + "#" * 100)
print("Some results from the second solution.")
print("#" * 100 + "\n")
for result in results:
    print(result)

spark.stop()
