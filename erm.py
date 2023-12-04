from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.window import Window
import sys

# Create a Spark session
spark = SparkSession.builder.appName("spltopics").getOrCreate()

parameter_value = sys.argv[1] if len(sys.argv) > 1 else "employee"
file = "./employee-data.csv"

df = spark.read.option("inferSchema", "true").option("header", "true").csv(file)
df_filtered = None

if parameter_value == 'employee':
    selected_columns = ["First Name", "Last Name", "Age", "Role", "Salary"]
    df_filtered = df.select(selected_columns)
    print(f"Filtered DataFrame for {parameter_value}:")
    df_filtered.show()  

    df_filtered.createOrReplaceTempView("employee_table")
    sql_query = f"SELECT * FROM employee_table WHERE Age > 25"
    df_sql_result = spark.sql(sql_query)
    print(f"Result of SQL query for {parameter_value}:")
    df_sql_result.show()

    df_filtered.groupBy("Role").agg(expr("avg(Salary)").alias("AvgSalaryByRole")).show()
    df_filtered.groupBy("Role").agg(expr("min(Salary)").alias("MinSalary")).show()
    df_filtered.filter("Salary > 50000").show()

    df_filtered.withColumn("ExpectedBonus", col("Salary") * 0.05).show()
    df_filtered.withColumn("Seniority", when(col("Age") >= 30, "Senior").otherwise("Junior")).show()

    df_filtered.groupBy("Role").agg(expr("avg(Age)").alias("AvgAge")).show()

elif parameter_value == 'manager':
    selected_columns = ["First Name", "Last Name", "Age", "Role", "Remaining Leave Days", "Salary"]
    df_filtered = df.select(selected_columns)
    print(f"Filtered DataFrame for {parameter_value}:")
    df_filtered.show()

    window_spec = Window.partitionBy("Role")
    df_avg_salary_by_role = df_filtered.withColumn("AvgSalaryByRole", expr("avg(Salary)").over(window_spec))
    print(f"DataFrame for {parameter_value} with average salary by role:")
    df_avg_salary_by_role.show()

    df_filtered.groupBy("Role").agg(expr("max(Salary)").alias("MaxSalary")).show()
    df_filtered.groupBy("Role").agg(expr("stddev(Salary)").alias("SalaryStdDev")).show()
    df_filtered.withColumn("ExpectedBonus", col("Salary") * 0.1).show()

    df_filtered.withColumn("PerformanceScore", when(col("Remaining Leave Days") > 5, "High").otherwise("Low")).show()

    df_filtered.groupBy("Role").agg(expr("max(Age)").alias("MaxAge")).show()
    df_filtered.filter("Remaining Leave Days < 10").show()

elif parameter_value == 'sr.manager':
    df_filtered = df
    print(f"Filtered DataFrame for {parameter_value}:")
    df_filtered.show()

    df_with_salary_per_year = df_filtered.withColumn("SalaryPerYear", col("Salary") * 12)
    print(f"For {parameter_value} with calculated column 'SalaryPerYear':")
    df_with_salary_per_year.show()

    df_filtered.groupBy("Age").agg(expr("sum(Salary)").alias("TotalSalary")).show()
    df_filtered.groupBy("Age").agg(expr("count(*)").alias("EmployeeCount")).show()
    df_filtered.withColumn("Tax", col("Salary") * 0.2).show()

    df_filtered.withColumn("ExperienceLevel", when(col("Age") > 35, "Experienced").otherwise("Mid-Level")).show()
    df_filtered.groupBy("Role").agg(expr("min(Age)").alias("MinAge")).show()

    df_filtered.withColumn("NetSalary", col("Salary") * 0.8).show()
    df_filtered.withColumn("LeaveStatus", when(col("Remaining Leave Days") > 0, "Available").otherwise("Not Available")).show()

spark.stop()
