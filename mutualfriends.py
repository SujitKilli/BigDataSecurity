from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Sample data
data = [("John", 28), ("Alice", 22), ("Bob", 30), ("Charlie", 25)]
columns = ["Name", "Age"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Perform a simple transformation
df_filtered = df.filter(df["Age"] > 25)

# Show the transformed DataFrame
df_filtered.show()

# Stop the Spark session
spark.stop()
