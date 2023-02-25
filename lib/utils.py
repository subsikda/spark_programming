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


def load_survey_df(spark, data_file):
    """Loads the data frame"""
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(data_file)


def count_by_country(survey_df):

    return survey_df \
        .where("Age < 40") \
        .select("Age", "Gender", "Country", "state") \
        .groupBy("Country") \
        .count()
