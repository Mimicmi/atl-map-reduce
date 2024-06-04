from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("atl-map-reduce") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

PATH_1 = "Archive/applications_activity_per_user_per_hour_1.csv"
PATH_2 = "Archive/applications_activity_per_user_per_hour_2.csv"


# Create schema
schema = StructType([
    StructField("timestamp", DateType()),
    StructField("user_id", IntegerType()),
    StructField("age_sexe", StringType()),
    StructField("application", StringType()),
    StructField("time_spent", IntegerType()),
    StructField("times_opened", IntegerType()),
    StructField("notifications_received", IntegerType()),
    StructField("times_opened_after_notification", IntegerType()),
])

# Create 1st DF with schema
df_1 = spark.read.format("csv").schema(
    schema).option("header", True).load(PATH_1)

df_1.printSchema()
df_1.show()

# Create 2nd DF with schema
df_2 = spark.read.format("csv").schema(
    schema).option("header", True).load(PATH_2)

df_2.printSchema()
df_2.show()

# Union of the two DF
union_df = df_1.union(df_2)

union_df.show()
