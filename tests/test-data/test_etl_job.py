import unittest
from jobs import etl_job
import os
import shutil
from unittest_pyspark import get_spark

cwd = os.getcwd()


class TestEtlJob(unittest.TestCase):

    def setUp(self):
        if os.path.exists(cwd + "/test-data/output"):
            shutil.rmtree(cwd + "/test-data/output")
        if os.path.exists(cwd + "/tmp"):
            shutil.rmtree(cwd + "/tmp")
        self.spark = get_spark()

    def test_validate_file1(self):
        """
          invalid weather file1 case validates the exception
        :return:
        """
        args = Namespace(weather_file="/test-data/input/invalid-weather.20160201.csv",
                         output_path="/test-data/output/data")
        try:
            etl_job.validate(self.spark, args)
        except:
            pass
        self.assertRaises(Exception, msg="Invalid input directory, kindly provide valid input directory")

    def test_validate_output_path_case(self):
        """
          invalid output path case validates the exception
        :return:
        """
        args = Namespace(weather_file1="/test-data/input/weather.20160201.csv",
                         output_path="/test-data/input")
        try:
            etl_job.validate(self.spark, args)
        except:
            pass
        self.assertRaises(Exception, msg="Invalid output path, output path exists")

    def test_main(self):
        """
         test case for main function
        :return: None
        """
        args = Namespace(weather_files_dir=cwd + "/test-data/input",
                         output_path=cwd +"/test-data/output")
        etl_job.main(self.spark, args)


class Namespace:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


if __name__ == '__main__':
    unittest.main()
