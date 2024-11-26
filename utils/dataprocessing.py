from pyspark.sql.functions import col, count, avg, desc


def perform_analysis(spark, data_path):
    """
    Performs data analysis on the OpenFlights routes dataset.

    Parameters:
    - spark: SparkSession object
    - data_path: Path to the directory containing the routes.dat file
    """
    # Read the routes data
    routes = spark.read.option("inferSchema", "true").option("header", "false").csv(f"{data_path}/routes.dat")

    # Assign column names to the DataFrame
    routes = routes.toDF("Airline", "AirlineID", "SourceAirport", "SourceAirportID",
                         "DestAirport", "DestAirportID", "Codeshare", "Stops", "Equipment")

    # Analysis 1: Which airline operates the most routes?
    airline_counts = routes.groupBy("Airline").agg(count("*").alias("TotalRoutes"))
    top_airlines = airline_counts.orderBy(desc("TotalRoutes")).limit(10)
    top_airlines.show()

    # Save the result to a CSV file
    top_airlines.write.csv(f"{data_path}/output/top_airlines", header=True, mode="overwrite")

    # Analysis 2: What is the average number of stops per route?
    avg_stops = routes.agg(avg(col("Stops").cast("integer")).alias("AverageStops"))
    avg_stops.show()

    # Save the result to a CSV file
    avg_stops.write.csv(f"{data_path}/output/avg_stops", header=True, mode="overwrite")

    # Analysis 3: Which source airport has the highest number of outgoing routes?
    airport_counts = routes.groupBy("SourceAirport").agg(count("*").alias("OutgoingRoutes"))
    top_airports = airport_counts.orderBy(desc("OutgoingRoutes")).limit(10)
    top_airports.show()

    # Save the result to a CSV file
    top_airports.write.csv(f"{data_path}/output/top_airports", header=True, mode="overwrite")