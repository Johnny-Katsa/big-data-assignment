from pyspark.sql import SparkSession

DATA_PATH = "data/"
FILE_1 = "Crime_Data_from_2010_to_2019"
FILE_2 = "Crime_Data_from_2020_to_Present"

spark = SparkSession.builder \
    .appName("CSV to Parquet") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df1 = spark.read.csv(f"hdfs://master:9000/{DATA_PATH}{FILE_1}.csv", header=True, inferSchema=True)
df2 = spark.read.csv(f"hdfs://master:9000/{DATA_PATH}{FILE_2}.csv", header=True, inferSchema=True)
df = df1.union(df2)

print(f"################### Schema of dataset ####################### ")
df.printSchema()
df.show(5)

# Converting to parquet
df.write.parquet(f"/parquet/Crime_Data")

# Print schema and some data of the Parquet file to verify correct conversion
df_parquet = spark.read.parquet(f"hdfs://master:9000/parquet/Crime_Data")

print(f"################### Schema of dataset after Parquet conversion ####################### ")
df_parquet.printSchema()
df_parquet.show(5)

spark.stop()

