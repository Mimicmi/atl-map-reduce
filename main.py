from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("atl-map-reduce") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

PATH = "Archive/applications_activity_per_user_per_hour_1.csv"

df = spark.read.csv(PATH)

df.show()
