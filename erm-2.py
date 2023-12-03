from pyspark.sql import SparkSession
import sys

# Create a Spark session
spark = SparkSession.builder.appName("spltopics").getOrCreate()
parameter_value = sys.argv[1] if len(sys.argv) > 1 else "employee"
s3file = "s3://spltopicsbucket/employee-data.csv"

# Create the DataFrame
df = spark.read.option("inferSchema","true").option("header","true").csv(s3file)
df_filtered = None

if parameter_value == 'employee':
  selected_columns = ["First Name", "Last Name", "Age", "Role"]
  df_filtered = df.select(selected_columns)
elif parameter_value == 'manager':
  selected_columns = ["First Name", "Last Name", "Age", "Role","Remaining Leave Days"]
  df_filtered = df.select(selected_columns)
elif parameter_value == 'sr.manager':
  df_filtered = df

df_filtered.show()

# Stop the Spark session
spark.stop()