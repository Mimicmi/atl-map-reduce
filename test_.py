from refacto import *


def test_create_spark_session():
    assert create_spark_session().active() != False
