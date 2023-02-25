import configparser

from pyspark import SparkConf


def get_spark_app_config():
    """Read spark config"""
    spark_conf = SparkConf()
    config_parser = configparser.ConfigParser()
    config_parser.read("spark.conf")

    for (key, val) in config_parser.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)

    return spark_conf
