import pyspark
from pyspark.sql import SparkSession


def get_spark_session(app_name):
    spark_conf = [("spark.sql.shuffle.partitions", "30"),
                  ("spark.driver.host", "localhost"),
                  ("spark.driver.bindAddress", "localhost")]
    spark = SparkSession.builder \
        .master("local") \
        .appName(app_name) \
        .config(conf=pyspark.SparkConf().setAll(spark_conf)) \
        .getOrCreate()
    return spark
