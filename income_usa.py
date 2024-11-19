# Importing necessary modules
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os

def get_spark_session():
    """
    Initialisiert eine Spark Session.
    """
    return SparkSession.builder \
        .appName("Income Analysis") \
        .master("local[*]") \
        .getOrCreate()

def read_income_data(spark, file_path):
    """
    Liest die .data-Datei mit den Einkommensdaten ein.
    :param spark: SparkSession
    :param file_path: Pfad zur .data-Datei
    :return: DataFrame
    """
    columns = [
        "age", "workclass", "fnlwgt", "education", "education_num",
        "marital_status", "occupation", "relationship", "race", "sex",
        "capital_gain", "capital_loss", "hours_per_week", "native_country", "income"
    ]
    df = spark.read.option("header", False).option("inferSchema", True).csv(file_path)
    df = df.toDF(*columns)
    return df

def analyse_income_fluctuations(df):
    """
    Findet die Altersgruppen mit den höchsten Einkommensschwankungen.
    :param df: DataFrame mit Einkommensinformationen
    :return: DataFrame mit Altersgruppen und der maximalen Einkommensschwankung
    """
    # Konvertiere das Einkommen in numerische Werte
    df = df.withColumn("income_numeric", F.when(F.col("income") == " <=50K", 0).otherwise(1))
    df = df.withColumn("age_group", (F.col("age") / 10).cast("integer") * 10)
    window_spec = Window.partitionBy("age_group")
    df = df.withColumn("income_diff", F.max("income_numeric").over(window_spec) - F.min("income_numeric").over(window_spec))
    max_fluctuation = df.groupBy("age_group").agg(F.max("income_diff").alias("max_income_diff"))
    return max_fluctuation.orderBy(F.desc("max_income_diff")).limit(1)

def calculate_average_income_by_education(df):
    """
    Berechnet das durchschnittliche Einkommen pro Bildungsniveau.
    :param df: DataFrame mit Einkommensinformationen
    :return: DataFrame mit durchschnittlichem Einkommen pro Bildungsniveau
    """
    df = df.withColumn("income_numeric", F.when(F.col("income") == " <=50K", 0).otherwise(1))
    return df.groupBy("education").agg(F.avg("income_numeric").alias("average_income")).orderBy("education")

def find_relationship_between_hours_and_income(df):
    """
    Findet das Muster zwischen der Anzahl der Arbeitsstunden pro Woche und dem Einkommen.
    :param df: DataFrame mit Einkommensinformationen
    :return: DataFrame mit durchschnittlichem Einkommen pro Arbeitsstundenkategorie
    """
    df = df.withColumn("income_numeric", F.when(F.col("income") == " <=50K", 0).otherwise(1))
    df = df.withColumn("hours_category", (F.col("hours_per_week") / 10).cast("integer") * 10)
    avg_income_per_hours = df.groupBy("hours_category").agg(F.avg("income_numeric").alias("avg_income")).orderBy("hours_category")
    return avg_income_per_hours

def save_results(df, output_path):
    """
    Speichert die Ergebnisse in eine CSV-Datei.
    :param df: DataFrame mit den Ergebnissen
    :param output_path: Speicherort der Datei
    """
    df.coalesce(1).write.mode('overwrite').option("header", True).csv(output_path)

if __name__ == "__main__":
    # Initialisieren der SparkSession
    spark = get_spark_session()

    # Pfad zur .data-Datei
    input_file = "adult.data"

    # Optional: Pfad zur Ausgabe
    output_dir = "./output"

    # Lesen des Datensatzes
    income_data = read_income_data(spark, input_file)

    # Überprüfen der geladenen Daten
    income_data.printSchema()
    income_data.show(5)

    # Analyse: Altersgruppen mit den stärksten Einkommensschwankungen
    fluctuation_result = analyse_income_fluctuations(income_data)
    print("Ergebnis der Einkommensschwankungen nach Altersgruppen:")
    fluctuation_result.show()
    save_results(fluctuation_result, os.path.join(output_dir, "max_income_fluctuation"))

    # Analyse: Durchschnittliches Einkommen pro Bildungsniveau
    average_income_result = calculate_average_income_by_education(income_data)
    print("Durchschnittliches Einkommen pro Bildungsniveau:")
    average_income_result.show()
    save_results(average_income_result, os.path.join(output_dir, "average_income_by_education"))

    # Analyse: Beziehung zwischen Arbeitsstunden und Einkommen
    hours_income_result = find_relationship_between_hours_and_income(income_data)
    print("Beziehung zwischen Arbeitsstunden und Einkommen:")
    hours_income_result.show()
    save_results(hours_income_result, os.path.join(output_dir, "hours_income_relationship"))
