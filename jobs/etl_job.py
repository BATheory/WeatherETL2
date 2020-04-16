from dependencies import util
import argparse
from pyspark.sql import SparkSession


def main(spark, args):
    """
     The function representing the actual ETL task
    :param: spark
    :param argement:  input arguments
    :return:  None
    """
    validate(spark, args)
    raw_df = util.Util.file_to_dataframe(spark, args.weather_files_dir + "/*", "csv")
    raw_df.write.parquet(args.output_path)
    raw_parquet_df = util.Util.file_to_dataframe(spark, args.output_path, "parquet")
    raw_parquet_df.createOrReplaceTempView("weather_table")
    qurery_df = spark.sql("""select t.ObservationDate,t.Region,t.ScreenTemperature from weather_table t 
                              join (select max(ScreenTemperature) as ScreenTemperature from weather_table) as v 
                              on t.ScreenTemperature = v.ScreenTemperature""")
    qurery_df.show(truncate=False)


def validate(spark, args):
    """
    Funtion validates the input parameters
    :param spark: This is the instance of the spark session
    :param args:  arguments
    :return:  None
    """
    if not util.Util.path_exist(spark, args.weather_files_dir):
        raise Exception("Invalid input directory, kindly provide valid input directory")
    if util.Util.path_exist(spark, args.output_path):
        raise Exception("Invalid output path, output path exists, kindly provide path that doesn't exists")


def start_spark(app_name='etl-app'):
    """Funtion which starts the Spark session
    :param: app_name name of the application
    :param: master name of the master
    :return: None
    """
    spark_builder = SparkSession.builder.appName(app_name)
    return spark_builder.enableHiveSupport().getOrCreate()


# Start - entry point of code: The below runs the PySpark ETL application:

if __name__ == '__main__':
    #use the argparse to pass arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--weather_files_dir", help="input directory that contains csv files", required=True)
    parser.add_argument("--output_path", help="output path ", required=True)
    args = parser.parse_args()
    spark = start_spark()
    main(spark, args)
    spark.stop()
