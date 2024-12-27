from pyspark.sql.functions import col, mean
from functools import reduce
import os


def perform_analysis(spark, file_path):
    """
    Analyzes alcohol consumption in Russia and generates statistical insights.

    This function performs the following tasks:
    1. Reads the given CSV file containing data
    2. Performs basic data exploration
    3. Renames column to 'consumption (l)' for easier analysis
    4. Adds the column 'pure_alcohol_consumption (l)' for further processing
    5. Calculates average consumption of pure alcohol before and after the start of the Russian-Ukrainian war
    6. Calculates average consumption of different alcoholic beverages for pre-war and wartime periods
    7. Calculates average consumption of pure alcohol per year
    8. Calculates trends in consumption of pure alcohol from 2017 until 2023
    9. Saves results to a .txt file in the 'Output' folder

    Parameters:
    spark (SparkSession): The Spark session to use for data processing
    file_path (str): Path to the input CSV file

    Returns:  # Outputs results to the console and saves them to a .txt file
    """
    # Read input data
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Define output folder and file
    output_folder = "Output"
    os.makedirs(output_folder, exist_ok=True)
    results_path = os.path.join(output_folder, "results.txt")

    # Rename column for clarity
    df = df.withColumnRenamed("Total alcohol consumption (in liters of pure alcohol per capita)", "consumption (l)")

    # Add pure alcohol content to calculate total pure alcohol consumption for each row
    alcohol_content = {
        "Wine": 0.12,
        "Beer": 0.05,
        "Vodka": 0.40,
        "Sparkling wine": 0.12,
        "Brandy": 0.40,
        "Сider": 0.05,
        "Liqueurs": 0.20
    }

    # Calculate total pure alcohol consumption for each row using reduce
    pure_alcohol_expr = reduce(lambda x, y: x + y, [col(beverage) * factor for beverage, factor in alcohol_content.items()])
    df = df.withColumn("pure_alcohol_consumption", pure_alcohol_expr)

    # Display initial data for debugging purposes
    df.show(5)
    df.printSchema()

    # Filter data based on year and calculate average consumption
    start_year = 2022
    before_war = df.filter(col("Year") < start_year)
    after_war = df.filter(col("Year") >= start_year)

    # Calculate average consumption for both periods (using "pure_alcohol_consumption")
    mean_before_war = before_war.agg(mean(col("pure_alcohol_consumption"))).collect()[0][0]
    mean_after_war = after_war.agg(mean(col("pure_alcohol_consumption"))).collect()[0][0]

    # Print and save results
    print(f'Durchschnittlicher reiner Alkoholkonsum vor dem Krieg: {mean_before_war:.2f} Liter')
    print(f'Durchschnittlicher reiner Alkoholkonsum nach Beginn des Krieges: {mean_after_war:.2f} Liter')

    with open(results_path, "w") as f:
        f.write(f"Durchschnittlicher reiner Alkoholkonsum pro Kopf vor dem Krieg: {mean_before_war:.2f} Liter\n")
        f.write(f"Durchschnittlicher reiner Alkoholkonsum pro Kopf nach Beginn des Krieges: {mean_after_war:.2f} Liter\n")
        f.write("----------\n")

    print("---------")

    # Calculate average consumption for each type of drink (using individual columns)
    drink_columns = ["Wine", "Beer", "Vodka", "Sparkling wine", "Brandy", "Сider", "Liqueurs"]

    # Filter data for pre-war (2017–2021) and wartime (2022–2023) periods
    before_war = df.filter(col("Year").between(2017, 2021))
    after_war = df.filter(col("Year").between(2022, 2023))

    total_drinks_before_war = {}
    total_drinks_after_war = {}

    # Calculate and print average consumption for each drink type
    for c in drink_columns:
        total_drinks_before_war[c] = round(before_war.select(mean(col(c))).collect()[0][0], 2)
        print(f"Durchschnittlicher pro Kopf Verbrauch von {c} in der Vorkriegszeit: {total_drinks_before_war[c]}")
        total_drinks_after_war[c] = round(after_war.select(mean(col(c))).collect()[0][0], 2)
        print(f"Durchschnittlicher pro Kopf Verbrauch von {c} nach der Vorkriegszeit: {total_drinks_after_war[c]}")

    # Save results
    with open(results_path, "a") as f:
        for c in drink_columns:
            f.write(f"Durchschnittlicher pro Kopf Verbrauch von {c} in der Vorkriegszeit: {total_drinks_before_war[c]}\n")
            f.write(f"Durchschnittlicher pro Kopf Verbrauch von {c} nach der Vorkriegszeit: {total_drinks_after_war[c]}\n")
        f.write("----------\n")

    print("---------")

    # Calculate yearly trends (using "pure_alcohol_consumption")
    years = list(range(2017, 2024))
    year_means = {}

    for year in years:
        year_data = df.filter(col("Year") == year)
        year_mean = year_data.agg(mean(col("pure_alcohol_consumption"))).collect()[0][0]
        year_means[year] = year_mean

    # Print and save results
    for y in years:
        print(f"Durchschnittlicher reiner Alkoholkonsum im Jahr {y}: {year_means[y]:.2f} Liter")

    with open(results_path, "a") as f:
        for y in years:
            f.write(f"Durchschnittlicher reiner Alkoholkonsum im Jahr {y}: {year_means[y]:.2f} Liter\n")
        f.write("----------\n")

    print("---------")

    # Calculate and print percentage increases year-over-year
    for i in range(2017, 2023):
        percentage_increase = ((year_means[i + 1] - year_means[i]) / year_means[i]) * 100
        print(f'Prozentuale Steigerung des reinen Alkoholkonsums von {i} auf {i + 1}: {percentage_increase:.2f}%')

    # Save results
    with open(results_path, "a") as f:
        for i in range(2017, 2023):
            percentage_increase = ((year_means[i + 1] - year_means[i]) / year_means[i]) * 100
            f.write(f"Prozentuale Steigerung des reinen Alkoholkonsums von {i} auf {i + 1}: {percentage_increase:.2f}%\n")

    print("---------")
    print(f"Die Ergebnisse wurden im Ordner '{output_folder}' gespeichert.")
