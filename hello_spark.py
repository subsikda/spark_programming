import sys

from pyspark.sql import SparkSession

from lib.logger import Log4J
from lib.utils import count_by_country, get_spark_app_config, load_survey_df

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info("Starting Hello Spark")

    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())
    survey_df = load_survey_df(spark, sys.argv[1])

    # foreful partioning the dataframe to test shuffle sort behaviour
    partioned_survey_df = survey_df.repartition(2)

    count_df = count_by_country(partioned_survey_df)

    logger.info(count_df.collect())
    # survey_df.show()

    logger.info("Finished Hello Spark")
    spark.stop()
