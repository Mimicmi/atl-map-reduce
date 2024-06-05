from pyspark.sql import SparkSession
from pyspark.sql.functions import split, when, avg, mean, broadcast, from_utc_timestamp, lag, col, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("atl-map-reduce") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

PATH_1 = "Archive/applications_activity_per_user_per_hour_1.csv"
PATH_2 = "Archive/applications_activity_per_user_per_hour_2.csv"

# 5 - 1 : Lecture des données

# Create schema
schema = StructType([
    StructField("timestamp", TimestampType()),
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

# df_1.printSchema()
# df_1.show()

# Create 2nd DF with schema
df_2 = spark.read.format("csv").schema(
    schema).option("header", True).load(PATH_2)

# df_2.printSchema()
# df_2.show()

# Union of the two DF
union_df = df_1.union(df_2)

# union_df.show()

# 5 - 2 : Nettoyage des données
# Séparation age_sexe en 2 colonnes
age_sexe_df = split(union_df["age_sexe"], "-")

union_df = union_df.withColumn("age", age_sexe_df.getItem(0).cast("integer"))
union_df = union_df.withColumn("sexe", age_sexe_df.getItem(1).cast("string"))
union_df = union_df.drop('age_sexe')


union_df.printSchema()
# union_df.show()

# distinctValuesDF = union_df.select("sexe").distinct().show()

# Harmonisation des données de la colonne Sexe
union_df = union_df.withColumn("sexe",
                               when(union_df.sexe == "m", "M")
                               .when(union_df.sexe == "f", "F")
                               .when(union_df.sexe == "H", "M")
                               .otherwise(union_df.sexe)
                               )

# distinctValuesDF = union_df.select("sexe").distinct().show()

# Agrégation des colonnes : date/sexe/age/application
union_df_agg = union_df.groupBy("timestamp", "sexe", "age", "application").agg(
    mean("time_spent").alias("mean-time-spent"),
    mean("times_opened").alias("mean-times-openend"),
    mean("notifications_received").alias("mean-notifications-received"),
    mean("times_opened_after_notification").alias(
        "mean-times-opened-after-notifications")
)

# union_df_agg.show()
union_df_agg.printSchema()


# Adding new path for new csv applications_categories & new schema
PATH_3 = "Archive/applications_categories.csv"

schema2 = StructType([
    StructField("application", StringType()),
    StructField("category", StringType()),
])

# Creating new dataframes from applications_categories.csv
df_3 = spark.read.format("csv").schema(
    schema2).option("header", True).load(PATH_3)

# Join union_df_agg with the new csv with broadcast of newer csv as it's a little dataframe
union_agg = union_df_agg.join(broadcast(df_3), on="application", how="left")

# new_union.show()

union = union_df.join(
    broadcast(df_3), on="application", how="left")

# union.show()


# Convert timestamp from DateType to timezone Europe/Paris
union_agg = union_agg.withColumn(
    "timestamp", col("timestamp").cast("timestamp"))
union_agg = union_agg.withColumn(
    "timestamp", from_utc_timestamp(union_agg.timestamp, "Europe/Paris"))
union_agg = union_agg.withColumn(
    "timestamp", to_date("timestamp", "yyyy-MM-dd"))


# 5-3.1 : Comparaison par tranche d’âge
union_agg_age_df = union_agg

union_agg_age_df = union_agg_age_df.groupBy("timestamp", "age").agg(
    mean("mean-time-spent").alias("value"))

union_agg_age_df = union_agg_age_df.withColumn(
    "variable",
    when(union_agg_age_df.age < 15, "moins de 15 ans")
    .when((union_agg_age_df.age >= 15) & (union_agg_age_df.age <= 25), "15-25 ans")
    .when((union_agg_age_df.age >= 26) & (union_agg_age_df.age <= 35), "26-35 ans")
    .when((union_agg_age_df.age >= 36) & (union_agg_age_df.age <= 45), "35-45 ans")
    .when(union_agg_age_df.age > 45, "plus de 45 ans")
)

union_agg_age_df = union_agg_age_df.withColumn("criterion", when(
    union_agg_age_df.variable.endswith("ans"), "age").otherwise(union_agg_age_df.variable))

union_agg_age_df = union_agg_age_df.select(
    "timestamp", "criterion", "variable", "value")
# union_agg_age_df.show()

# 5-3.2 : Comparaison par sexe
union_agg_day_sexe = union_agg.groupBy("timestamp", "sexe").agg(
    mean("mean-time-spent").alias("value"))

union_agg_day_sexe = union_agg_day_sexe.orderBy("timestamp", "sexe")

union_agg_day_sexe = union_agg_day_sexe.withColumn(
    "variable", union_agg_day_sexe.sexe)
union_agg_day_sexe = union_agg_day_sexe.withColumnRenamed(
    "sexe", "criterion")
union_agg_day_sexe = union_agg_day_sexe.withColumn("criterion", when(
    union_agg_day_sexe.variable == union_agg_day_sexe.criterion, "sexe").otherwise(union_agg_day_sexe.variable))

union_agg_day_sexe = union_agg_day_sexe.select(
    "timestamp", "criterion", "variable", "value")

# union_agg_day_sexe.show()

# 5-3.3 : Comparaison par catégorie
union_agg_category = union_agg.groupBy(
    "timestamp", "category").agg(mean("mean-time-spent").alias("value"))

union_agg_category = union_agg_category.withColumn(
    "variable", union_agg_category.category)
union_agg_category = union_agg_category.withColumnRenamed(
    "category", "criterion")
union_agg_category = union_agg_category.withColumn("criterion", when(
    union_agg_category.variable == union_agg_category.criterion, "category").otherwise(union_agg_category.variable))

union_agg_category = union_agg_category.select(
    "timestamp", "criterion", "variable", "value")

# union_agg_category.show()

# Combine all three dataframes
combined_df = union_agg_age_df.union(
    union_agg_day_sexe).union(union_agg_category)

combined_df.show()

# 5-3.4 Calcul de l’indice
# Filter only the range of criterion's age of 15-25
windowSpec = Window.partitionBy("criterion", "variable").orderBy(
    "timestamp")

window_df = combined_df.withColumn("index", lag("value").over(windowSpec))

window_df.show()

# Utiliser la fonction lag pour obtenir la valeur de l'année précédente
