from refacto import *

spark = create_spark_session()
path = "Archive/applications_activity_per_user_per_hour_1.csv"
new_path = "Archive/applications_categories.csv"


def test_create_spark_session():
    assert create_spark_session().active() != False


def test_read_csv():
    assert read_csv(spark, path)


def test_clean_data():
    assert clean_data(read_csv(spark, path))


def test_aggregate_data():
    assert aggregate_data(read_csv(spark, path))


def test_read_new_csv():
    assert read_new_csv(read_csv(spark, new_path))


def test_join_with_categories():
    assert join_with_categories(
        read_csv(spark, path), read_new_csv(spark, new_path))


def test_convert_timestamp():
    assert convert_timestamp(read_csv(spark, path))


def test_process_age_comparison():
    assert process_age_comparison(read_csv(spark, path))


def test_process_gender_comparison():
    assert process_gender_comparison(read_csv(spark, path))


def test_process_category_comparison():
    assert process_category_comparison(read_csv(spark, path))


def test_calculate_index():
    assert calculate_index(read_csv(spark, path))


def test_calculate_moving_average():
    assert calculate_moving_average(read_csv(spark, path))
