from pyspark.sql import SparkSession


class Util:
    """
    Class supporting the etl_jobs.
    """
    def path_exist(spark: SparkSession, path: str):
        """
        check if file path exists.  If it exist returns true otherwise false
        :param spark: spark session
        :param path: path with file name validated
        :return: Boolean
        """
        jvm = spark._jvm
        jsc = spark._jsc
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
        return True if fs.exists(jvm.org.apache.hadoop.fs.Path(path)) else False

    def file_to_dataframe(spark: SparkSession, path: str, format: str):
        """
        Function reads file and return as data frame
        :param spark  SparkSession
        :param path:  path with file name
        :param format:  format of the file can be either a csv, orc or parquet
        :return:  dataframe
        """
        if format.lower() == "csv":
            return spark.read.option("header", "True").option("inferSchema", 'true').option("ignoreTrailingWhiteSpace", "true")\
                             .option("ignoreLeadingWhiteSpace", "true").csv(path)
        elif format.lower() == "orc":
            return spark.read.orc(path)
        elif format.lower() == "parquet":
            return spark.read.parquet(path)
        else:
            raise Exception("Invalid file format, this format is support at moment")
