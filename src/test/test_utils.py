import unittest
from pyspark.sql.session import SparkSession
from src.main.utils import Utils
class TestUtils(unittest.TestCase):
    sparkCommonSession = SparkSession.builder.appName("Ptroines-compute").config("spark.jars.packages","com.crealytics:spark-excel_2.12:0.13.7").getOrCreate()
    utilObject = Utils(sparkCommonSession)


    def test_utils(self):
        self.assertEqual(self.utilObject.makeUpper("abc"), "ABC")

if __name__ == '__main__':
        unittest.main()