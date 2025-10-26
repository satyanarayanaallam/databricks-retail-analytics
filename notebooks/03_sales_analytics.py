from pyspark.sql import functions as F

file_path = "/Workspace/Repos/<your_user>/retail-analytics/data/sales_data.csv"
sales_df = spark.read.csv(file_path, header=True, inferSchema=True)
sales_df = sales_df.withColumn("TotalAmount", F.col("Quantity") * F.col("Price"))

# Top Products by Sales
top_products = sales_df.groupBy("Product").agg(F.sum("TotalAmount").alias("TotalSales"))
top_products = top_products.orderBy(F.desc("TotalSales"))
print("🏆 Top Products by Total Sales")
top_products.show()

# Region-wise sales summary
region_sales = sales_df.groupBy("Region").agg(
    F.sum("TotalAmount").alias("RegionSales"),
    F.countDistinct("Product").alias("UniqueProducts")
)
region_sales.show()
