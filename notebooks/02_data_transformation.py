from pyspark.sql import functions as F

# Assume sales_df is already loaded
# (or you can re-read from Delta/CSV if in a new session)
file_path = "/Workspace/Repos/<your_user>/retail-analytics/data/sales_data.csv"
sales_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Add computed column
sales_df = sales_df.withColumn("TotalAmount", F.col("Quantity") * F.col("Price"))

# Clean: remove nulls, standardize casing, etc.
sales_df = sales_df.dropna()
sales_df = sales_df.withColumn("Region", F.upper(F.col("Region")))

sales_df.show()
