from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()
parameter_value = sys.argv[1] if len(sys.argv) > 1 else "employee"

data = [("John","Krasinski", 22, "SDE I", 20, 100000, 20000, 1), 
 ("Alice","Mertens", 24, "SDE II", 15, 160000,30000,3), 
  ("Bobby","Paul", 25,"SDE II", 21, 180000,35000,4), 
   ("Charlie","Puth", 28, "SDM", 16, 320000, 75000, 7),
    ("Draco","Malfoy", 29, "SDE III", 17, 240000, 45000, 8),
     ("Elvis","Presley", 27, "QA Eng", 18, 120000, 20000, 6),
      ("Franky","Jaeger", 35, "Sr. SDM", 19, 450000, 100000, 14)]
schema = StructType([StructField("First Name", StringType(), True),StructField("Last Name", StringType(), True),
                     StructField("Age", IntegerType(), True),StructField("Role", StringType(), True),
                     StructField("Remaining Leave Days", IntegerType(), True), StructField("Salary", LongType(), True),
                     StructField("Bonus", LongType(), True),StructField("Experience(Yrs)", IntegerType(), True)])

# Create the DataFrame
df = spark.createDataFrame(data, schema)
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
