# Databricks notebook source
from pyspark.sql import SparkSession

#spark = SparkSession.builder.appName("Adventurework").getOrCreate()

df_products = spark.read.csv('/mnt/bronze/AdventureWorks_Products.csv', header=True, inferSchema=True)
df_sales_2015 = spark.read.csv('/mnt/bronze/AdventureWorks_Sales_2015.csv', header=True, inferSchema=True)
df_sales_2016 = spark.read.csv('/mnt/bronze/AdventureWorks_Sales_2016.csv', header=True, inferSchema=True)
df_sales_2017 = spark.read.csv('/mnt/bronze/AdventureWorks_Sales_2017.csv', header=True, inferSchema=True)
df_customer = spark.read.csv('/mnt/bronze/AdventureWorks_Customers.csv', header=True, inferSchema=True)
df_calender = spark.read.csv('/mnt/bronze/AdventureWorks_Calendar.csv', header=True, inferSchema=True)


# COMMAND ----------

## Merge these sales table together into one

df_sales = df_sales_2015.union(df_sales_2016).union(df_sales_2017)
df_sales.count(), len(df_sales.columns)

# COMMAND ----------

from pyspark.sql.functions import col, date_format, weekofyear

## Extract Datetime field

df_calender = df_calender.withColumn("MonthName", date_format(col("Date"), "MMMM")) \
                                     .withColumn("DayOfWeek", date_format(col("Date"), "EEEE")) \
                                     .withColumn("WeekNumber", weekofyear(col("Date")))


# COMMAND ----------

# Example: Clean and transform customer data
df_customers = df_customer.dropna()

# Example: Join sales data with products, customers, and calendar
df_sales = df_sales.dropna()
df_sales_enriched = df_sales.join(df_products, "ProductKey", "left") \
                                  .join(df_customers, "CustomerKey", "left") \
                                  .join(df_calender, df_sales["OrderDate"] == df_calender["Date"], "left")


# Save DataFrames as Delta tables
df_customers.write.format("delta").mode("overwrite").save("/mnt/silver/customers")
df_sales_enriched.write.format("delta").mode("overwrite").save("/mnt/silver/sales")


# COMMAND ----------


