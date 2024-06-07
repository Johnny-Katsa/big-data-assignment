from pyspark.sql import SparkSession
import csv
from io import StringIO

DATA_CSV_PATH = "hdfs://master:9000/csv/Crime_Data"


###############################################
# Mapping methods
###############################################
def parse_csv(line):
    sio = StringIO(line)
    reader = csv.reader(sio)
    return next(reader)


def time_to_segment(time):
    try:
        time = int(time)
    except ValueError:
        print("ERROR CASTING TIME TO INT!")
        return 'NA'
    if 500 <= time <= 1159:
        return 'Morning'
    elif 1200 <= time <= 1659:
        return 'Afternoon'
    elif 1700 <= time <= 2059:
        return 'Evening'
    elif 2100 <= time <= 2359 or 0 <= time <= 499:
        return 'Night'
    else:
        return 'NA'


#############################
# Preparation
#############################
sc = SparkSession.builder \
    .appName("Test - Query 2 - RDD API - CSV") \
    .getOrCreate() \
    .sparkContext

rdd = sc.textFile(DATA_CSV_PATH)

# column_names = parse_csv(rdd.first())
# premis_desc_index = column_names.index("Premis Desc")
# time_occ_index = column_names.index("TIME OCC")
#
# ##############################
# # Querying
# ##############################
# transformed_rdd = (rdd
#                    .map(parse_csv)
#                    .filter(lambda row: row[premis_desc_index] == "STREET" and row != column_names)
#                    .map(lambda row: time_to_segment(row[time_occ_index])))
#
# # Collect the transformed data without aggregation
# results = transformed_rdd.collect()
# print("\n" + "#" * 100)
# print(results)
# print("#" * 100 + "\n")


# Perform the countByValue operation
# results = transformed_rdd.countByValue()

# # We could have done this since we only have 4 results. No need for distributed computing.
# # However, we will do the sorting again using RDD just for demonstration purposes.
# print("\n" + "#" * 100)
# print(dict(sorted(results.items(), key=lambda item: item[1], reverse=True)))
# print("#" * 100 + "\n")
#
# # For demonstration purposes, sorting with rdd as well.
# counts_rdd = sc.parallelize(list(results.items())).sortBy(lambda x: x[1], ascending=False).take(5)
# print("\n" + "#" * 100)
# print(counts_rdd)
# print("#" * 100 + "\n")

# This is an alternative way to apply the sorting on the first rdd chain.
# However, optimizations in 'countByValue' method which was used earlier
# make it much more efficient than this.
# results = (rdd
#            .map(parse_csv)
#            .filter(lambda row: row[premis_desc_index] == "STREET" and row != column_names)
#            .map(lambda row: (time_to_segment(row[time_occ_index]), 1))
#            .reduceByKey(lambda a, b: a + b)
#            .sortBy(lambda x: x[1], ascending=False))

sc.stop()
