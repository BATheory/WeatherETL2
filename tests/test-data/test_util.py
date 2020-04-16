
import unittest
from unittest_pyspark import get_spark
from dependencies import util


class UtilTest(unittest.TestCase):

    def setUp(self):
        self.spark = get_spark()

    def test_path_exist_true_case(self):
        """
            testing path_exists method true case
        :return: None
        """
        self.assertTrue(util.Util.path_exist(self.spark.sparkContext, "test-data/input/weather.20160201.csv"))

    def test_path_exist_false_case(self):
        """
          testing path_exists method true case
        :return: None
            """
        self.assertFalse(util.Util.path_exist(self.spark, "invalid-file.txt"))

    def test_file_to_dataframe_csv(self):
        """
          testing file_to_dataframe method for csv format
        :return:
        """
        df = util.Util.file_to_dataframe(self.spark, "test-data/input/weather.20160201.csv", "csv")
        df.show()
        self.assertGreater(len(df.take(1)), 0)


if __name__ == '__main__':
    unittest.main()
