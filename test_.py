from refacto import *

def test_create_spark_session():
    assert create_spark_session().active()!=False

# def test_read_csv():
#     spark=create_spark_session();
#     path1="Archive/applications_activity_per_user_per_hour_1.csv"
#     path2="Archive/applications_activity_per_user_per_hour.csv"
#     assert read_csv(spark,path1)
#     assert read_csv(spark,path2)

