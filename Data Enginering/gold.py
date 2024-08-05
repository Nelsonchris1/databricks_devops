# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

dbutils.fs.ls('/mnt/silver')

# COMMAND ----------

customer = spark.read.format('delta').load('/mnt/silver/customers/')
sales = spark.read.format('delta').load('/mnt/silver/sales/')

# COMMAND ----------

display(sales)

# COMMAND ----------

from pyspark.sql.functions import col, round

df_sales_agg = sales.groupBy("MonthName", "ProductSKU", "ProductName") \
                                .agg({"ProductPrice": "sum", "OrderQuantity": "sum"}) \
                                .withColumnRenamed("sum(ProductPrice)", "TotalSales") \
                                .withColumnRenamed("sum(OrderQuantity)", "TotalQuantity") \
                                .withColumn("TotalSales", round(col("TotalSales"), 2))

# COMMAND ----------

df_sales_agg.write.format('delta').mode('overwrite').save('/mnt/gold/sales_aggregated')
sales.write.format('delta').mode('overwrite').save('/mnt/gold/sales')
