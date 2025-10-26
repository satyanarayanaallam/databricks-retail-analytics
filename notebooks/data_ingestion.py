# notebooks/data_ingestion.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RetailSales").getOrCreate()

# Read CSV
sales_df = spark.read.csv("/Workspace/Repos/your_user/databricks-retail-analytics/data/sample_sales.csv", header=True, inferSchema=True)
sales_df.show(5)
