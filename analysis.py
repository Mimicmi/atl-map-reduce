# 5.5 Affichage des courbes de comparaison
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("atl-map-reduce-analysis") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.option("header", "true").option(
    "recursiveFileLookup", "true").parquet("Archive/window_average_df.parquet")

df.show()
