# notebooks/data_transformation.py
from pyspark.sql import functions as F

sales_df = sales_df.withColumn("TotalAmount", F.col("Quantity") * F.col("Price"))
sales_df.groupBy("Product").agg(F.sum("TotalAmount").alias("TotalSales")).show()