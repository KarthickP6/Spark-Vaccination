import unittest
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

class sampleTest(unittest.TestCase):
 
    @classmethod
    def setUpClass(cls):
        """
        Start Spark, define config and path to test data
        """
        
        logging.info("Logging from within setup")
        cls.spark=SparkSession \
            .builder \
            .appName("sampleTest") \
            .master("local") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
        
    def test_step_one(self):
        file_name = "step1"
        input_df = self.read_file(file_name)
        if input_df != None:
            actual_count = input_df.count()
            expected_count = 8096          
            self.assertEqual(actual_count,expected_count,"Count of records does not match")
        else:
            self.fail("The required input file seems missing")
    
    def test_step_two(self):
        file_name = "step2"
        input_df = self.read_file(file_name)
        if input_df != None:
            actual_count = input_df.count()
            expected_count = 193          
            self.assertEqual(actual_count,expected_count,"Count of records does not match")
        else:
            self.fail("The required input file seems missing")
            
    def test_step_three(self):
        file_name = "step3"
        input_df = self.read_file(file_name)
        if input_df != None:
            actual_count = input_df.count()
            expected_count = 5          
            self.assertEqual(actual_count,expected_count,"Count of records does not match")
        else:
            self.fail("The required input file seems missing")
    
    def test_step_four(self):
        file_name = "step4"
        input_df = self.read_file(file_name)
        if input_df != None:
            actual_count = input_df.count()
            expected_count = 5          
            self.assertEqual(actual_count,expected_count,"Count of records does not match")
        else:
            self.fail("The required input file seems missing")
    
    
    def read_file(self,file_name):
        try:
            path = "/home/jai/input/kickoffs-spark-vaccination/output/" + file_name
            df = self.spark.read.parquet(path)
            return df
        except:
            print("----------------------------------------------------------------------------------------------------------")
            print("Looks like the output directory for following file is missing.{}".format(file_name))
            print("Please check whether the output file directory name matches the directory name given in instructions.")
            print("Note that you can still go ahead and submit your test. Scoring will happen accordingly.")
            print("----------------------------------------------------------------------------------------------------------")

    @classmethod
    def tearDownClass(cls):
        """
        Stop Spark
        """
        cls.spark.stop()
        
    
    
if __name__ == "__main__":
    unittest.main()
