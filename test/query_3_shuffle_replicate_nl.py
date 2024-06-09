from pyspark import SparkConf
from pyspark.sql import SparkSession

CRIME_DATA_CSV_PATH = "hdfs://master:9000/csv/Crime_Data"
INCOME_DATA_CSV_PATH = "hdfs://master:9000/data/LA_income_2015.csv"
REVGEO_DATA_CSV_PATH = "hdfs://master:9000/data/revgecoding.csv"

#############################################################
# Defining the mappings for victim descents and their coding.
#############################################################
descent_mappings = {
    "A": "Other Asian", "B": "Black", "C": "Chinese",
    "D": "Cambodian", "F": "Filipino", "G": "Guamanian",
    "H": "Hispanic/Latin/Mexican", "I": "American Indian/Alaskan Native",
    "J": "Japanese", "K": "Korean", "L": "Laotian", "O": "Other",
    "P": "Pacific Islander", "S": "Samoan", "U": "Hawaiian",
    "V": "Vietnamese", "W": "White", "X": "Unknown", "Z": "Asian Indian"
}

descent_column = "CASE "
for code, descent in descent_mappings.items():
    descent_column += f"WHEN `Vict Descent` = '{code}' THEN '{descent}'\n"
descent_column += "END"

#####################################################################
# Preparation
#####################################################################\
#
# .set("spark.executor.memory", "10g") \
#     .set("spark.executor.cores", "2") \
#     .set("spark.driver.memory", "4g") \
#     .set("spark.sql.autoBroadcastJoinThreshold", "-1") \
#     .set("spark.sql.shuffle.partitions", "100") \
#     .set("spark.executor.memory", "4g") \


spark = SparkSession.builder \
    .appName("Query 3 - SQL API - Shuffle Replicate NL") \
    .getOrCreate()

df_crimes = spark.read.csv(CRIME_DATA_CSV_PATH, header=True, inferSchema=True)
df_codes = spark.read.csv(REVGEO_DATA_CSV_PATH, header=True, inferSchema=True)
df_incomes = spark.read.csv(INCOME_DATA_CSV_PATH, header=True, inferSchema=True)

df_crimes.createOrReplaceTempView("crime_data")
df_codes.createOrReplaceTempView("revgeo")
df_incomes.createOrReplaceTempView("incomes")

#############################################################
# Querying
#############################################################
query = f"""

WITH distinct_revgeo AS (
    SELECT LAT, LON, MIN(ZIPcode) AS zip_code
    FROM revgeo
    GROUP BY LAT, LON
),
joined_data AS (
    SELECT /*+ SHUFFLE_REPLICATE_NL(distinct_revgeo) */ 
           {descent_column} AS victim_descent, zip_code 
    FROM crime_data 
    JOIN distinct_revgeo USING(LAT, LON)

)
SELECT * FROM joined_data

"""

# GROUP BY victim_descent
# ORDER BY count(*) DESC;

result = spark.sql(query)
result.explain(True)
result.show(1000)

spark.stop()
