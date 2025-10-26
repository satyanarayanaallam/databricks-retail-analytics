from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("RetailAnalytics").getOrCreate()

# Define file path
file_path = "/Workspace/Users/allam.satyanarayana@gmail.com/databricks-retail-analytics/data/sales_data.csv"

# Read CSV into DataFrame
sales_df = spark.read.csv(file_path, header=True, inferSchema=True)

print("✅ Data Ingested Successfully")
sales_df.show(5)
sales_df.printSchema()