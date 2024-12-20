from pyspark.sql import SparkSession
from utils.dataprocessing import perform_analysis

'''
NOTE: Either Java 8 or 11 have to be installed in order to properly run PySpark!
'''

if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder.appName("AlcoholConsumptionRussianWar").getOrCreate()

    # Path to the data directory
    data_path = "./data"

    # Perform data analysis
    perform_analysis(spark, data_path)

    # Stop the SparkSession
    spark.stop()
