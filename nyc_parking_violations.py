# Importing necessary modules
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import os
import kagglehub

# Initialize a Spark Session
def get_spark_session():
    """
    Initializes a Spark Session.
    """
    return SparkSession.builder \
        .appName("NYC Parking Violations Analysis") \
        .master("local[*]") \
        .getOrCreate()

# Read CSV file into Spark DataFrame
def read_data(spark, file_path):
    """
    Reads the CSV file containing NYC parking violations data.
    :param spark: SparkSession
    :param file_path: Path to the CSV file
    :return: DataFrame
    """
    return spark.read.option("header", True).option("inferSchema", True).csv(file_path)

# Reduce dataset size to approximately 95MB randomly
def reduce_dataset_size(df, target_size_mb=95):
    """
    Reduces the dataset size to approximately 95MB by taking a random sample.
    :param df: DataFrame with the full data
    :param target_size_mb: Target size in MB
    :return: DataFrame with reduced size
    """
    sample_fraction = target_size_mb / (df.count() * df.columns.__len__() * 8 / 1024 / 1024)
    return df.sample(withReplacement=False, fraction=min(sample_fraction, 1.0))

# Analyze the top violations by issuing agency
def analyse_top_violations(df):
    """
    Analyzes the top types of violations by issuing agency.
    :param df: DataFrame with parking violations data
    :return: DataFrame with top violations by agency
    """
    top_violations = df.groupBy("Issuing Agency", "Violation Code").count().orderBy(F.desc("count"))
    return top_violations

# Calculate the average number of violations per violation code
def calculate_average_violations(df):
    """
    Calculates the average number of violations for each violation code.
    :param df: DataFrame with parking violations data
    :return: DataFrame with average number of violations per violation code
    """
    average_violations = df.groupBy("Violation Code").count().withColumnRenamed("count", "average_violations").orderBy(F.desc("average_violations"))
    return average_violations

# Analyze the most common violation times
def analyse_violation_times(df):
    """
    Analyzes the most common times for violations.
    :param df: DataFrame with parking violations data
    :return: DataFrame with most common violation times
    """
    if "Violation Time" in df.columns:
        violation_times = df.groupBy("Violation Time").count().orderBy(F.desc("count"))
        return violation_times
    else:
        print("Skipping analysis: Violation Time column not found in dataset.")
        return None

# Save results to CSV
def save_results(df, output_path):
    """
    Saves the results into a CSV file.
    :param df: DataFrame with the results
    :param output_path: Path where to save the CSV
    """
    if df is not None:
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

# Print results to console
def print_results(df, description):
    """
    Prints the results to the console.
    :param df: DataFrame with the results
    :param description: Description of the results
    """
    if df is not None:
        print(description)
        df.show(truncate=False)

if __name__ == "__main__":
    # Initialize SparkSession
    spark = get_spark_session()

    # Download latest version of the dataset using kagglehub
    path = kagglehub.dataset_download("davinascimento/nyc-parking-violations-issued")
    print("Path to dataset files:", path)

    # Path to the CSV file (in the downloaded dataset)
    input_file = os.path.join(path, "Parking_Violations_Issued.csv")
    output_dir = "./"

    # Read the dataset
    parking_data = read_data(spark, input_file)

    # Reduce dataset size to approximately 95MB
    reduced_parking_data = reduce_dataset_size(parking_data)

    # Analysis 1: Top violations by issuing agency
    top_violations_result = analyse_top_violations(reduced_parking_data)
    print_results(top_violations_result, "Top violations by issuing agency:")
    save_results(top_violations_result, os.path.join(output_dir, "top_violations_by_agency"))

    # Analysis 2: Average number of violations per violation code
    average_violations_result = calculate_average_violations(reduced_parking_data)
    print_results(average_violations_result, "Average number of violations per violation code:")
    save_results(average_violations_result, os.path.join(output_dir, "average_violations_per_code"))

    # Analysis 3: Most common violation times
    violation_times_result = analyse_violation_times(reduced_parking_data)
    print_results(violation_times_result, "Most common violation times:")
    save_results(violation_times_result, os.path.join(output_dir, "most_common_violation_times"))

    # Stop SparkSession
    spark.stop()
