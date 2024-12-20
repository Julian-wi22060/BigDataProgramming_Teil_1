from pyspark.sql.functions import col, mean, sum
import os

def perform_analysis(spark, file_path):

    # Daten einlesen
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Ouput definieren
    output_folder = "Output"
    os.makedirs(output_folder, exist_ok=True)
    results_path = os.path.join(output_folder, "results.txt")

    # Spalte umbenennen
    df = df.withColumnRenamed("Total alcohol consumption (in liters of pure alcohol per capita)", "consumption (l)")

    # Daten anzeigen
    df.show(5)
    df.printSchema()

    # Filtere die Daten auf Jahr und berechne den durchschnittlichen Konsum
    start_year = 2022
    before_war = df.filter(col("Year") < start_year)
    after_war = df.filter(col("Year") >= start_year)

    # Durchschnittlichen Konsum berechnen
    mean_before_war = before_war.agg(mean(col("consumption (l)"))).collect()[0][0]
    mean_after_war = after_war.agg(mean(col("consumption (l)"))).collect()[0][0]

    print(f'Durchschnittlicher Alkoholkonsum vor dem Krieg: {mean_before_war:.2f}')
    print(f'Durchschnittlicher Alkoholkonsum nach Beginn des Krieges: {mean_after_war:.2f}')

    with open(results_path, "w") as f:
        f.write(f"Durchschnittlicher Alkoholkonsum pro Kopf vor dem Krieg: {mean_before_war:.2f}\n")
        f.write(f"Durchschnittlicher Alkoholkonsum pro Kopf nach Beginn des Krieges: {mean_after_war:.2f}\n")
        f.write("----------\n")

    # Berechnung der prozentualen Verteilung von Getränkearten
    drink_columns = ["Wine", "Beer", "Vodka", "Sparkling wine", "Brandy", "Сider", "Liqueurs"]

    # Filtere die Daten für Vorkriegszeit und Kriegszeit
    before_war = df.filter(col("Year").between(2017, 2021))
    after_war = df.filter(col("Year").between(2022, 2023))

    total_drinks_before_war = {}
    total_drinks_after_war = {}

    # Summiere die Werte aller Getränkearten für beide Zeiträume
    for c in drink_columns:
        total_drinks_before_war[c] = round(before_war.select(mean(col(c))).collect()[0][0], 2)
        print(f"Durchschnittlicher pro Kopf Verbrauch von {c} in der Vorkriegszeit: {total_drinks_before_war[c]}")
        total_drinks_after_war[c] = round(after_war.select(mean(col(c))).collect()[0][0], 2)
        print(f"Durchschnittlicher pro Kopf Verbrauch von {c} nach der Vorkriegszeit: {total_drinks_after_war[c]}")

    with open(results_path, "a") as f:
        for c in drink_columns:
            total_drinks_before_war[c] = round(before_war.select(mean(col(c))).collect()[0][0], 2)
            f.write(f"Durchschnittlicher pro Kopf Verbrauch von {c} in der Vorkriegszeit: {total_drinks_before_war[c]}\n")
            total_drinks_after_war[c] = round(after_war.select(mean(col(c))).collect()[0][0], 2)
            f.write(f"Durchschnittlicher pro Kopf Verbrauch von {c} nach der Vorkriegszeit: {total_drinks_after_war[c]}\n")
        f.write("----------\n")

    print("---------")

    # Berechnung der prozentualen Steigerung zwischen allen Jahren
    years = list(range(2017, 2024))
    year_means = {}

    for year in years:
        year_data = df.filter(col("Year") == year)
        year_mean = year_data.agg(mean(col("consumption (l)"))).collect()[0][0]
        year_means[year] = year_mean

    print("---------")

    # Durchschnittlicher Alkoholkonsum im Jahr
    for y in years:
        print(f"Durchschnittlicher Alkoholkonsum im Jahr {y}: {year_means[y]:.2f} Liter")
    
    with open(results_path, "a") as f:
        for y in years:
            f.write(f"Durchschnittlicher Alkoholkonsum im Jahr {y}: {year_means[y]:.2f} Liter\n")
        f.write("----------\n")

    print("---------")

    for i in range(2017, 2023):
        percentage_increase = ((year_means[i + 1] - year_means[i]) / year_means[i]) * 100
        print(f'Prozentuale Steigerung des Alkoholkonsums von {i} auf {i + 1}: {percentage_increase:.2f}%')

    with open(results_path, "a") as f:
        for i in range(2017, 2023):
            percentage_increase = ((year_means[i + 1] - year_means[i]) / year_means[i]) * 100
            f.write(f"Prozentuale Steigerung des Alkoholkonsums von {i} auf {i + 1}: {percentage_increase:.2f}%\n")

    print("---------")
    print(f"Die Ergebnisse wurden im Ordner '{output_folder}' gespeichert.")
