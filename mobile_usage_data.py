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
        .appName("Mobile Device Usage Analysis") \
        .master("local[*]") \
        .getOrCreate()

# Read CSV file into Spark DataFrame
def read_data(spark, file_path):
    """
    Reads the CSV file containing mobile device usage data.
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

# Analyze average screen time by age group
def analyse_screen_time_by_age(df):
    """
    Analyzes average screen time by age group.
    :param df: DataFrame with mobile usage data
    :return: DataFrame with average screen time by age group
    """
    df = df.withColumn("age_group", (F.col("Age") / 10).cast("integer") * 10)
    avg_screen_time = df.groupBy("age_group").agg(F.avg("Screen On Time (hours/day)").alias("average_screen_time")).orderBy(F.desc("average_screen_time"))
    return avg_screen_time

# Analyze data usage correlation with screen time
def analyse_data_usage_vs_screen_time(df):
    """
    Analyzes how data usage correlates with screen time.
    :param df: DataFrame with mobile usage data
    :return: DataFrame with correlation between data usage and screen time
    """
    data_usage_vs_screen_time = df.groupBy("User ID").agg(F.avg("Data Usage (MB/day)").alias("avg_data_usage"), F.avg("Screen On Time (hours/day)").alias("avg_screen_time"))
    return data_usage_vs_screen_time.orderBy(F.desc("avg_screen_time"))

# Aggregate data usage by avg_screen_time groups
def aggregate_data_usage_by_screen_time_group(df):
    """
    Aggregates data usage by grouping users into 5 different avg_screen_time groups.
    :param df: DataFrame with avg data usage and screen time
    :return: DataFrame with aggregated data usage by screen time group
    """
    df = df.withColumn("screen_time_group", 
                       F.when(F.col("avg_screen_time") <= 3, "0-3 hours")
                       .when((F.col("avg_screen_time") > 3) & (F.col("avg_screen_time") <= 6), "3-6 hours")
                       .when((F.col("avg_screen_time") > 6) & (F.col("avg_screen_time") <= 9), "6-9 hours")
                       .when((F.col("avg_screen_time") > 9) & (F.col("avg_screen_time") <= 12), "9-12 hours")
                       .otherwise("> 12 hours"))
    aggregated_data = df.groupBy("screen_time_group").agg(F.avg("avg_data_usage").alias("average_data_usage"), F.count("User ID").alias("user_count")).orderBy("screen_time_group")
    return aggregated_data

# Analyze data usage by age group
def analyse_data_usage_by_age(df):
    """
    Analyzes average data usage by age group.
    :param df: DataFrame with mobile usage data
    :return: DataFrame with average data usage by age group
    """
    df = df.withColumn("age_group", (F.col("Age") / 10).cast("integer") * 10)
    avg_data_usage = df.groupBy("age_group").agg(F.avg("Data Usage (MB/day)").alias("average_data_usage")).orderBy(F.desc("average_data_usage"))
    return avg_data_usage

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
    path = kagglehub.dataset_download("valakhorasani/mobile-device-usage-and-user-behavior-dataset")
    print("Path to dataset files:", path)

    # Path to the CSV file (in the downloaded dataset)
    input_file = os.path.join(path, "user_behavior_dataset.csv")
    output_dir = "./"

    # Read the dataset
    usage_data = read_data(spark, input_file)

    # Reduce dataset size to approximately 95MB
    reduced_usage_data = reduce_dataset_size(usage_data)

    # Analysis 1: Average screen time by age group
    avg_screen_time_result = analyse_screen_time_by_age(reduced_usage_data)
    print_results(avg_screen_time_result, "Average screen time by age group:")
    save_results(avg_screen_time_result, os.path.join(output_dir, "average_screen_time_by_age"))

    # Analysis 2: Data usage vs. screen time correlation
    data_usage_vs_screen_time_result = analyse_data_usage_vs_screen_time(reduced_usage_data)
    print_results(data_usage_vs_screen_time_result, "Data usage vs. screen time correlation:")
    save_results(data_usage_vs_screen_time_result, os.path.join(output_dir, "data_usage_vs_screen_time"))

    # Analysis 3: Aggregate data usage by avg_screen_time groups
    aggregated_data_usage_result = aggregate_data_usage_by_screen_time_group(data_usage_vs_screen_time_result)
    print_results(aggregated_data_usage_result, "Aggregated data usage by screen time group:")
    save_results(aggregated_data_usage_result, os.path.join(output_dir, "aggregated_data_usage_by_screen_time_group"))

    # Analysis 4: Average data usage by age group
    avg_data_usage_result = analyse_data_usage_by_age(reduced_usage_data)
    print_results(avg_data_usage_result, "Average data usage by age group:")
    save_results(avg_data_usage_result, os.path.join(output_dir, "average_data_usage_by_age"))

    # Stop SparkSession
    spark.stop()
