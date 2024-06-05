from pyspark.sql import SparkSession
from pyspark.sql.functions import split, when, avg, mean, broadcast, from_utc_timestamp, lag, col, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.window import Window

def create_spark_session():
    # Create spark session
    return SparkSession.builder \
        .master("local") \
        .appName("atl-map-reduce") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

# 5 - 1 : Lecture des données
def read_csv(spark, path):
    # Create schema
    schema = StructType([
        StructField("timestamp", TimestampType()),
        StructField("user_id", IntegerType()),
        StructField("age_gender", StringType()),
        StructField("application", StringType()),
        StructField("time_spent", IntegerType()),
        StructField("times_opened", IntegerType()),
        StructField("notifications_received", IntegerType()),
        StructField("times_opened_after_notification", IntegerType()),
    ])
    return spark.read.format("csv").schema(schema).option("header", True).load(path)

# 5 - 2 : Nettoyage des données
def clean_data(df):
    # Séparation age_sexe en 2 colonnes
    age_gender_df = split(df["age_gender"], "-")
    df = df.withColumn("age", age_gender_df.getItem(0).cast("integer")) \
           .withColumn("gender", age_gender_df.getItem(1).cast("string")) \
           .drop('age_gender')

    # Harmonisation des données de la colonne Sexe
    df = df.withColumn("gender",
                       when(df.gender == "m", "M")
                       .when(df.gender == "f", "F")
                       .when(df.gender == "H", "M")
                       .otherwise(df.gender))

    return df

def aggregate_data(df):
    # Agrégation des colonnes : date/sexe/age/application
    return df.groupBy("timestamp", "gender", "age", "application").agg(
        mean("time_spent").alias("mean_time_spent"),
        mean("times_opened").alias("mean_times_opened"),
        mean("notifications_received").alias("mean_notifications_received"),
        mean("times_opened_after_notification").alias("mean_times_opened_after_notifications")
    )

def read_new_csv(spark, path):
    # Create new schema
    schema = StructType([
        StructField("application", StringType()),
        StructField("category", StringType()),
    ])
    return spark.read.format("csv").schema(schema).option("header", True).load(path)

# Join union_df_agg with the new csv with broadcast of newer csv as it's a little dataframe
def join_with_categories(df, categories_df):
    return df.join(broadcast(categories_df), on="application", how="left")

# Convert timestamp from DateType to timezone Europe/Paris
def convert_timestamp(df):
    df = df.withColumn("timestamp", col("timestamp").cast("timestamp")) \
           .withColumn("timestamp", from_utc_timestamp(col("timestamp"), "Europe/Paris")) \
           .withColumn("timestamp", to_date("timestamp", "yyyy-MM-dd"))
    return df

# 5-3.1 : Comparaison par différentes tranches
# Comparaisons par tranche d'âge
def process_age_comparison(df):
    df = df.groupBy("timestamp", "age").agg(mean("mean_time_spent").alias("value")) \
           .withColumn("variable", when(df.age < 15, "moins de 15 ans") \
                                     .when((df.age >= 15) & (df.age <= 25), "15-25 ans") \
                                     .when((df.age >= 26) & (df.age <= 35), "26-35 ans") \
                                     .when((df.age >= 36) & (df.age <= 45), "35-45 ans") \
                                     .when(df.age > 45, "plus de 45 ans")) \
           .withColumn("criterion", when(col("variable").endswith("ans"), "age").otherwise(col("variable"))) \
           .select("timestamp", "criterion", "variable", "value")
    return df

# Comparaison par tranche de sexe
def process_gender_comparison(df):
    df = df.groupBy("timestamp", "gender").agg(mean("mean_time_spent").alias("value")) \
           .withColumn("variable", df.gender) \
           .withColumnRenamed("gender", "criterion") \
           .withColumn("criterion", when(col("variable") == col("criterion"), "gender").otherwise(col("variable"))) \
           .select("timestamp", "criterion", "variable", "value")
    return df

# Comparaison par tranche de category
def process_category_comparison(df):
    df = df.groupBy("timestamp", "category").agg(mean("mean_time_spent").alias("value")) \
           .withColumn("variable", col("category")) \
           .withColumnRenamed("category", "criterion") \
           .withColumn("criterion", when(col("variable") == col("criterion"), "category").otherwise(col("variable"))) \
           .select("timestamp", "criterion", "variable", "value")
    return df

# 5-3.4 Calcul de l’indice
def calculate_index(df):
    windowSpec = Window.partitionBy("criterion", "variable").orderBy("timestamp")
    df = df.withColumn("index", lag("value").over(windowSpec))
    return df

# moving-average
def calculate_moving_average(df):
    window_moving_avg = Window.orderBy(col("timestamp")).rowsBetween(-4, 0)
    df = df.withColumn("moving_avg", avg(col("index")).over(window_moving_avg))
    return df

def main():
    # 5 - 1 : Lecture des données
    spark = create_spark_session()
    # Create DF with schema
    df_1 = read_csv(spark, "Archive/applications_activity_per_user_per_hour_1.csv")
    df_2 = read_csv(spark, "Archive/applications_activity_per_user_per_hour_2.csv")
    
    # Union of the two DF
    union_df = df_1.union(df_2).cache()
    
    # 5 - 2 : Nettoyage des données
    union_df = clean_data(union_df)
    
    # Agrégation des colonnes : date/sexe/age/application
    union_df_agg = aggregate_data(union_df)
    
    # Create DF with new schema for new csv applications_categories
    categories_df = read_new_csv(spark, "Archive/applications_categories.csv")
    
    # Join union_df_agg with the new csv with broadcast of newer csv as it's a little dataframe
    union_agg = join_with_categories(union_df_agg, categories_df).cache()
    
    # Convert timestamp from DateType to timezone Europe/Paris
    union_agg.show()
    union_agg = convert_timestamp(union_agg)
    
    # 5-3.1 : Comparaison par différentes tranches
    age_comparison_df = process_age_comparison(union_agg)
    gender_comparison_df = process_gender_comparison(union_agg)
    category_comparison_df = process_category_comparison(union_agg)
    
    # Combine all three dataframes
    combined_df = age_comparison_df.union(gender_comparison_df).union(category_comparison_df)
    combined_df.show()

    # 5-3.4 Calcul de l’indice
    window_df = calculate_index(combined_df)
    # moving-average
    window_average_df = calculate_moving_average(window_df)
    
    window_average_df.orderBy("timestamp", ascending=False).show()
    
    # 5.4 Stockage du résultat
    # Save to parquet
    #window_average_df.write.parquet("Archive/window_average_df.parquet")

if __name__ == "__main__":
    main()