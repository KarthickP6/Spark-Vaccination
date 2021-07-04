import scala.io.Source;
import java.io._;
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.types.IntegerType


object Hello{

	// The input and output file paths are already configured to work with the lab. 

	// PLEASE don't modify these paths as it may affect the program execution.

	val input_loc = "file:///home/labuser/Desktop/Project/wings-spark-vaccination/input"
	val output_loc = "file:///home/labuser/Desktop/Project/wings-spark-vaccination/output"
	val input_file1 = "country_vaccinations.csv"
	val step1_loc = output_loc+"/"+"step1"
	val step2_loc = output_loc+"/"+"step2"
	val step3_loc = output_loc+"/"+"step3"
	val step4_loc = output_loc+"/"+"step4"

	// Spark session created for your use.
	val spark: SparkSession = SparkSession.builder.master("local").appName("Vaccine analysis").getOrCreate()
	import spark.implicits._
	
	//  This is the main driver program for the challenge. 
	//  PLEASE don't edit this as it may affect the execution of the challenge. 
	
	def main(args: Array[String]){
		var df1 = extract_data(input_loc, input_file1)
		clear_output_path()
		var one = step1(df1)
		load_data(one, step1_loc)
		var two = step2(one)
		load_data(two, step2_loc)
		var three = step3(one)
		load_data(three, step3_loc)
		var four = step4(one)
		load_data(four, step4_loc)

	}
	// Method to get the data from the input file location.
	def extract_data(fileloc:String, filename:String) : DataFrame = {
		val path = fileloc+"/"+filename
		var df = spark.read.format("csv").option("header", "true").load(path)
		return df

	}
	// Method to load the data to a file after tranformations.
	def load_data(df:DataFrame, fileloc:String) = {
		// Put your code here to save the resultant data frame to a parquet file
	}

	// Method to clear the output path for each and every execution.
	def clear_output_path() = {

		val path = os.pwd/os.up/'output
  		print(path)
  		if(os.exists(path)){
      		print("path exists")
      		os.remove.all(path)
      		os.makeDir(path)
  		}	
  		else{
    			os.makeDir(path)
  		}

		
	}

	def step1(df:DataFrame):DataFrame = {
		// Put your code here for executing step1.

		// Replace the below two lines with your code and return the resulting dataframe 
		val df = spark.emptyDataFrame
		return df
	}

	def step2(df:DataFrame):DataFrame = {
		// Put your code here for executing step2.

		// Replace the below two lines with your code and return the resulting dataframe 
		val df = spark.emptyDataFrame
		return df
	}	

	def step3(df:DataFrame):DataFrame = { 
		// Put your code here for executing step3.

		// Replace the below two lines with your code and return the resulting dataframe 
		val df = spark.emptyDataFrame
		return df
	}
	def step4(df:DataFrame):DataFrame = {
		// Put your code here for executing step4.

		// Replace the below two lines with your code and return the resulting dataframe 
		val df = spark.emptyDataFrame
		return df
	}


}
