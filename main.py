from pyspark.sql import SparkSession
from utils.dataprocessing import perform_analysis

if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder.appName("OpenFlightsAnalysis").getOrCreate()

    # Path to the data directory
    data_path = "./data"

    # Perform data analysis
    perform_analysis(spark, data_path)

    # Stop the SparkSession
    spark.stop()