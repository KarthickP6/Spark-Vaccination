import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import os
import shutil
from pyspark.sql.types import *
from pyspark.sql.functions import col, greatest


# The input and output file paths are already configured to work with the lab. 

# PLEASE don't modify these paths as it may affect the program execution.

input_loc = "/home/jai/input/kickoffs-spark-vaccination/input"
output_loc = "/home/jai/input/kickoffs-spark-vaccination/output"
input_file_1 = "country_vaccinations.csv"

step1_loc = output_loc+"/"+"step1"
step2_loc = output_loc+"/"+"step2"
step3_loc = output_loc+"/"+"step3"
step4_loc = output_loc+"/"+"step4"

#Spark session created for your use
spark = SparkSession.builder.appName("Vaccine analysis").master("local").getOrCreate()

#IGNORE this variable dummy_schema 
dummy_schema = StructType([StructField("Dummy", StringType(), True)])

# Method to get the data from the input file location.
def extract_data(fileloc, filename):
	path = fileloc+"/"+filename
	df = spark.read.format("csv").option("header", "true").load(path)
	return df

# Method to load the data to a file after tranformations.
def load_data(df, file_loc):
	df.coalesce(1).write.parquet(file_loc)

# Method to clear the output path for each and every execution.
# PLEASE don't change anything in the below function.
def clear_output_path():
	output_files =["step1","step2","step3","step4"]
	for f in output_files:
		path = "./output"+f
		if (os.path.isdir(path)):
			try:
				shutil.rmtree(path)
				print("% s removed successfully" % path)
			except OSError as error:
				print(error)
		else:
			print("The directory does not exist. Moving on..%s", f)

def step1(df1):
    # Clean non null values in total_vaccinations col and remove the last columns in df

    # Put your code here
    # Replace the below two lines with your code and return the resulting dataframe
	
    df = df1.drop('source_name','source_website')
    df = df.na.drop(subset=["total_vaccinations"])

    return df

def step2(df2):
    # MOst common vaccine used by each country

    # Put your code here
    
    # Replace the below two lines with your code and return the resulting dataframe
  df = df2.groupBy("country","vaccines").count()

  return df

def step3(df3):
    # top 5 countries with most number of vaccinations 

    # Put your code here
    
    # Replace the below two lines with your code and return the resulting dataframe
  top_5 = spark.createDataFrame(
    [
        (1, "foo"),  # create your data here, be consistent in the types.
        (2, "bar"),
         (1, "foo"),  # create your data here, be consistent in the types.
        (2, "bar"),
         (1, "foo"),  # create your data here, be consistent in the types.
        
    ],
    ["id", "label"]  # add your column names here
)

  return top_5

def step4(df4):
    # top_5 countries with maximum  number of daily_vaccinations per million

    #Put your code here
    
    # Replace the below two lines with your code and return the resulting dataframe
   df = spark.sparkContext.parallelize([]).toDF(dummy_schema)

   df = spark.createDataFrame(
    [
        (1, "foo"),  # create your data here, be consistent in the types.
        (2, "bar"),
         (1, "foo"),  # create your data here, be consistent in the types.
        (2, "bar"),
         (1, "foo"),  # create your data here, be consistent in the types.
        
    ],
    ["id", "label"]  # add your column names here
)

   return df

# This is the main driver program with all function calls.

# PLEASE don't edit these as this may affect the execution of the challenge.
def main():
    df1 = extract_data(input_loc, input_file_1)
    clear_output_path()
    one = step1(df1)
    load_data(one, step1_loc)
    two = step2(one)
    load_data(two, step2_loc)
    three = step3(one)
    load_data(three, step3_loc)
    four = step4(one)
    load_data(four, step4_loc)
    	
if __name__ == "__main__":
	main()
