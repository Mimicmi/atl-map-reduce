import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Fixture pour créer une SparkSession utilisable dans les tests.
    """
    spark = SparkSession.builder \
        .master("local") \
        .appName("atl-map-reduce") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    yield spark

    spark.stop()
