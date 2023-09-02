import os
import sys

from src.Module.app import App
from src.constants.constants import FIFA23_OFFICIAL_DATA, FIFA_CLUBS_DATA
import pytest
import pyspark
from pyspark.sql import SparkSession
import mock


# os.environ['SPARK_HOME'] = "/home/siva/hadoop/spark"
# os.environ['HADOOP_HOME'] = "/home/siva/hadoop/hadoop-3.3.0"
# sys.path.append("/home/siva/hadoop/spark/python")
# sys.path.append("/home/siva/hadoop/spark/python/lib")
@pytest.fixture(scope='session')
def spark_session():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("local test") \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.driver.host", "localhost") \
        .config("spark.default.parallelism", 1) \
        .config("spark.sql.shuffle.partitions", 1) \
        .getOrCreate()
    yield spark
    spark.stop()


@mock.patch('src.Module.app.get_spark_session')
def test_do(mock_spark, spark_session):
    mock_spark.return_value = spark_session
    with App() as app:
        assert app.do() is None


@mock.patch('src.Module.app.get_spark_session')
def test_data_cleaning(mock_spark, spark_session):
    mock_spark.return_value = spark_session
    with App() as app:
        fifa_official = app.read_csv(FIFA23_OFFICIAL_DATA)
        fifa_clubs = app.read_csv(FIFA_CLUBS_DATA)
        fifa_official, fifa_clubs_df = app.data_cleaning(fifa_official, fifa_clubs)
        assert fifa_clubs_df.count() == 51
        assert len(fifa_official.columns) == 25


@mock.patch('src.Module.app.get_spark_session')
def test_data_cleaning(mock_spark, spark_session):
    mock_spark.return_value = spark_session
    with pytest.raises(Exception):
        with App() as app:
            app.data_cleaning("E1.csv", "E2.csv")


@mock.patch('src.Module.app.get_spark_session')
def test_read_csv(mock_spark, spark_session):
    # Negative test case
    mock_spark.return_value = spark_session
    with App() as app:
        fifa_official = app.read_csv(FIFA23_OFFICIAL_DATA)
        assert fifa_official.count() == 17660


@mock.patch('src.Module.app.get_spark_session')
def test_read_csv2(mock_spark, spark_session):
    # Negative test case
    mock_spark.return_value = spark_session
    with pytest.raises(Exception):
        with App() as app:
            app.read_csv("ERROR.csv")


@mock.patch('src.Module.app.get_spark_session')
def test_get_top_5_countries(mock_spark, spark_session):
    mock_spark.return_value = spark_session
    with App() as app:
        fifa_official = app.read_csv(FIFA23_OFFICIAL_DATA)
        df = app.get_top_5_countries(fifa_official)
    expected_list = [('England', 1531), ('Germany', 1038), ('Spain', 990), ('France', 864), ('Argentina', 843)]
    actual_list = df.rdd.map(tuple).collect()
    assert expected_list == actual_list


@mock.patch('src.Module.app.get_spark_session')
def test_get_top_5_countries2(mock_spark, spark_session):
    # negative test case
    mock_spark.return_value = spark_session
    with pytest.raises(Exception):
        with App() as app:
            app.get_top_5_countries("Err.csv")
