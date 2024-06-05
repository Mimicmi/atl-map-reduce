from refacto import *
from io import StringIO
from pyspark.sql import Row
from datetime import datetime
import pandas as pd


def test_create_spark_session(spark):
    """
    Test utilisant le conftest.py pour la création de la session spark
    On va vérifier si la session spark est initialisée si c'est une instance de 
    SparkSession et si le nom correspond
    """
    assert spark is not None
    assert isinstance(spark, SparkSession)
    assert spark.sparkContext.appName == "atl-map-reduce"


def test_read_csv(spark):
    csv_data = StringIO("""
timestamp,user_id,age_gender,application,time_spent,times_opened,notifications_received,times_opened_after_notification
2021-01-01 00:00:00,1,Male 25-34,App1,30,2,1,1
2021-01-02 00:00:00,2,Female 18-24,App2,40,3,2,2
    """)

    # Convertir les données CSV en DataFrame Pandas
    pdf = pd.read_csv(csv_data, parse_dates=['timestamp'])

    # Convertir le DataFrame Pandas en DataFrame Spark
    df = spark.createDataFrame(pdf)

    # Écrire les données DataFrame Spark en tant que fichier CSV temporaire pour les tests
    temp_csv_path = "/tmp/test_data.csv"
    df.write.csv(temp_csv_path, header=True, mode='overwrite')

    # Appeler la fonction read_csv
    result_df = read_csv(spark, temp_csv_path)

    # Définir le schéma attendu
    expected_schema = StructType([
        StructField("timestamp", TimestampType()),
        StructField("user_id", IntegerType()),
        StructField("age_gender", StringType()),
        StructField("application", StringType()),
        StructField("time_spent", IntegerType()),
        StructField("times_opened", IntegerType()),
        StructField("notifications_received", IntegerType()),
        StructField("times_opened_after_notification", IntegerType()),
    ])

    # Vérifier que le schéma est correct
    assert result_df.schema == expected_schema

    # Vérifier que les données sont correctes
    expected_data = [
        Row(timestamp=pd.Timestamp("2021-01-01 00:00:00"), user_id=1, age_gender="Male 25-34", application="App1",
            time_spent=30, times_opened=2, notifications_received=1, times_opened_after_notification=1),
        Row(timestamp=pd.Timestamp("2021-01-02 00:00:00"), user_id=2, age_gender="Female 18-24", application="App2",
            time_spent=40, times_opened=3, notifications_received=2, times_opened_after_notification=2),
    ]

    result_data = result_df.collect()

    assert result_data == expected_data


def test_clean_data(spark):
    data = [
        Row(age_gender="25-M", user_id=1),
        Row(age_gender="34-F", user_id=2),
        Row(age_gender="40-H", user_id=3),
        Row(age_gender="29-m", user_id=4),
        Row(age_gender="22-f", user_id=5)
    ]

    df = spark.createDataFrame(data)

    # Appeler la fonction clean_data
    result_df = clean_data(df)

    # Données attendues
    expected_data = [
        Row(user_id=1, age=25, gender="M"),
        Row(user_id=2, age=34, gender="F"),
        Row(user_id=3, age=40, gender="M"),
        Row(user_id=4, age=29, gender="M"),
        Row(user_id=5, age=22, gender="F")
    ]

    expected_df = spark.createDataFrame(expected_data)

    # On compare les résultats et voir si ils sont OK
    assert result_df.collect() == expected_df.collect()


def test_aggregate_data(spark):
    # Créer un DataFrame de test
    data = [
        Row(timestamp="2021-01-01 00:00:00", gender="M", age=25, application="App1", time_spent=30,
            times_opened=2, notifications_received=1, times_opened_after_notification=1),
        Row(timestamp="2021-01-01 00:00:00", gender="M", age=25, application="App1", time_spent=40,
            times_opened=3, notifications_received=2, times_opened_after_notification=2),
        Row(timestamp="2021-01-01 00:00:00", gender="F", age=30, application="App2", time_spent=20,
            times_opened=1, notifications_received=1, times_opened_after_notification=0),
        Row(timestamp="2021-01-01 00:00:00", gender="F", age=30, application="App2", time_spent=25,
            times_opened=2, notifications_received=1, times_opened_after_notification=1)
    ]

    df = spark.createDataFrame(data)

    # Appeler la fonction aggregate_data
    result_df = aggregate_data(df)

    # Définir les données attendues
    expected_data = [
        Row(timestamp="2021-01-01 00:00:00", gender="M", age=25, application="App1", mean_time_spent=35.0,
            mean_times_opened=2.5, mean_notifications_received=1.5, mean_times_opened_after_notifications=1.5),
        Row(timestamp="2021-01-01 00:00:00", gender="F", age=30, application="App2", mean_time_spent=22.5,
            mean_times_opened=1.5, mean_notifications_received=1.0, mean_times_opened_after_notifications=0.5)
    ]

    expected_df = spark.createDataFrame(expected_data)

    # Vérifier que les résultats sont corrects
    assert result_df.collect() == expected_df.collect()


def test_read_new_csv(spark):
    # Créer des données CSV pour le test
    csv_data = """application,category
App1,Category1
App2,Category2
"""

    # Écrire les données CSV dans un fichier temporaire
    temp_csv_path = "/tmp/test_category_data.csv"
    with open(temp_csv_path, 'w') as f:
        f.write(csv_data)

    # Appeler la fonction read_new_csv
    result_df = read_new_csv(spark, temp_csv_path)

    # Définir le schéma attendu
    expected_schema = StructType([
        StructField("application", StringType(), True),
        StructField("category", StringType(), True),
    ])

    # Vérifier que le schéma est correct
    assert result_df.schema == expected_schema

    # Définir les données attendues
    expected_data = [
        Row(application="App1", category="Category1"),
        Row(application="App2", category="Category2")
    ]

    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    # Vérifier que les données sont correctes
    assert result_df.collect() == expected_df.collect()


def test_join_with_categories(spark):
    # Créer des DataFrames de test
    df_data = [
        Row(application="App1", time_spent=30),
        Row(application="App2", time_spent=40),
        Row(application="App3", time_spent=50)
    ]
    df = spark.createDataFrame(df_data)

    categories_data = [
        Row(application="App1", category="Category1"),
        Row(application="App2", category="Category2")
    ]
    categories_df = spark.createDataFrame(categories_data)

    # Appeler la fonction join_with_categories
    result_df = join_with_categories(df, categories_df)

    # Définir les données attendues
    expected_data = [
        Row(application="App1", time_spent=30, category="Category1"),
        Row(application="App2", time_spent=40, category="Category2"),
        Row(application="App3", time_spent=50, category=None)
    ]
    expected_df = spark.createDataFrame(expected_data)

    # Vérifier que les résultats sont corrects
    assert result_df.collect() == expected_df.collect()


def test_convert_timestamp(spark):
    # Créer un DataFrame de test
    data = [
        Row(timestamp="2021-01-01 00:00:00"),
        Row(timestamp="2021-02-01 00:00:00"),
        Row(timestamp="2021-03-01 00:00:00")
    ]

    df = spark.createDataFrame(data)

    # Appeler la fonction convert_timestamp
    result_df = convert_timestamp(df)

    # Définir les données attendues
    expected_data = [
        Row(timestamp=datetime.strptime("2021-01-01", "%Y-%m-%d")),
        Row(timestamp=datetime.strptime("2021-02-01", "%Y-%m-%d")),
        Row(timestamp=datetime.strptime("2021-03-01", "%Y-%m-%d"))
    ]

    # Convertir les objets datetime en objets date uniquement
    expected_data = [Row(timestamp=row.timestamp.date())
                     for row in expected_data]

    expected_df = spark.createDataFrame(expected_data)

    # Vérifier que les résultats sont corrects
    assert result_df.collect() == expected_df.collect()


def test_calculate_index(spark):
    # Créer un DataFrame de test
    data = [
        Row(timestamp="2021-01-01", criterion="Crit1",
            variable="Var1", value=30.0),
        Row(timestamp="2021-01-02", criterion="Crit1",
            variable="Var1", value=40.0),
        Row(timestamp="2021-01-03", criterion="Crit1",
            variable="Var1", value=50.0),
        Row(timestamp="2021-01-01", criterion="Crit2",
            variable="Var2", value=60.0),
        Row(timestamp="2021-01-02", criterion="Crit2",
            variable="Var2", value=70.0),
        Row(timestamp="2021-01-03", criterion="Crit2",
            variable="Var2", value=80.0),
    ]

    df = spark.createDataFrame(data)

    # Appeler la fonction calculate_index
    result_df = calculate_index(df)

    # Définir les données attendues
    expected_data = [
        Row(timestamp="2021-01-01", criterion="Crit1",
            variable="Var1", value=30.0, index=None),
        Row(timestamp="2021-01-02", criterion="Crit1",
            variable="Var1", value=40.0, index=30.0),
        Row(timestamp="2021-01-03", criterion="Crit1",
            variable="Var1", value=50.0, index=40.0),
        Row(timestamp="2021-01-01", criterion="Crit2",
            variable="Var2", value=60.0, index=None),
        Row(timestamp="2021-01-02", criterion="Crit2",
            variable="Var2", value=70.0, index=60.0),
        Row(timestamp="2021-01-03", criterion="Crit2",
            variable="Var2", value=80.0, index=70.0),
    ]

    expected_df = spark.createDataFrame(expected_data)

    # Vérifier que les résultats sont corrects
    assert result_df.collect() == expected_df.collect()
