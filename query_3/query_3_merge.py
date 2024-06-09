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
#####################################################################
spark = SparkSession.builder \
    .appName("Query 3 - SQL API - No hint merge") \
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
for income_direction in ["ASC", "DESC"]:
    query = f"""

    WITH distinct_revgeo AS (
        SELECT LAT, LON, MIN(ZIPcode) AS zip_code
        FROM revgeo
        GROUP BY LAT, LON
    ),
    crime_data_with_country_code AS (
        SELECT /*+ MERGE(distinct_revgeo) */ {descent_column} AS victim_descent, zip_code 
        FROM crime_data JOIN distinct_revgeo USING(LAT, LON)
    ),
    highest_income_country_codes AS (
        SELECT `Zip Code` FROM incomes
        WHERE `Zip Code` IN (SELECT DISTINCT(zip_code) FROM crime_data_with_country_code)
        ORDER BY `Estimated Median Income` {income_direction} 
        LIMIT 3
    )
    SELECT victim_descent, count(*) AS victims FROM crime_data_with_country_code 
    WHERE ZIP_CODE IN (SELECT `Zip Code` FROM highest_income_country_codes)
    GROUP BY victim_descent
    ORDER BY count(*) DESC

    """

    print("\n" + "#" * 100)
    print(f"Results for {'lowest income' if income_direction == 'ASC' else 'highest income'} country codes.")
    print("#" * 100 + "\n")

    result = spark.sql(query)
    result.explain(True)
    result.show(len(descent_mappings))

spark.stop()
