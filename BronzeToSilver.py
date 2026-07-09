# Databricks notebook source
spark.sql("SHOW CATALOGS").show()

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN RetailCatalog").show()

# COMMAND ----------

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .csv("abfss://bronze@retailadls2026.dfs.core.windows.net/Global_Superstore2.csv")
)

# COMMAND ----------

df_raw.show(5)

# COMMAND ----------

df_raw.printSchema()

# COMMAND ----------

df_raw.describe().show()

# COMMAND ----------

print("Total Records:", df_raw.count())

# COMMAND ----------

duplicate_count = df_raw.count() - df_raw.dropDuplicates().count()
print("Duplicate Records:", duplicate_count)

# COMMAND ----------

from pyspark.sql.functions import col, count, when

df_raw.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df_raw.columns
]).show()

# COMMAND ----------

from pyspark.sql.functions import sum, when

df_raw.select(
    sum(when(col("Sales") < 0, 1).otherwise(0)).alias("Negative_Sales"),
    sum(when(col("Quantity") < 0, 1).otherwise(0)).alias("Negative_Quantity"),
    sum(when((col("Discount") < 0) | (col("Discount") > 1), 1).otherwise(0)).alias("Invalid_Discount"),
    sum(when(col("Shipping Cost") < 0, 1).otherwise(0)).alias("Negative_Shipping_Cost"),
    sum(when(col("Ship Date") < col("Order Date"), 1).otherwise(0)).alias("Invalid_Ship_Date")
).show()

# COMMAND ----------

df_silver = df_raw

# COMMAND ----------

from pyspark.sql.functions import datediff, col

df_silver = df_silver.withColumn(
    "Shipping_Days",
    datediff(col("Ship Date"), col("Order Date"))
)

df_silver.select(
    "Order Date",
    "Ship Date",
    "Shipping_Days"
).show(5)

# COMMAND ----------

from pyspark.sql.functions import year

df_silver = df_silver.withColumn(
    "Order_Year",
    year(col("Order Date"))
)

df_silver.select(
    "Order Date",
    "Order_Year"
).show(5)

# COMMAND ----------

from pyspark.sql.functions import month

df_silver = df_silver.withColumn(
    "Order_Month",
    month(col("Order Date"))
)

df_silver.select(
    "Order Date",
    "Order_Month"
).show(5)

# COMMAND ----------

from pyspark.sql.functions import quarter

df_silver = df_silver.withColumn(
    "Order_Quarter",
    quarter(col("Order Date"))
)

df_silver.select(
    "Order Date",
    "Order_Quarter"
).show(5)

# COMMAND ----------

from pyspark.sql.functions import round

df_silver = df_silver.withColumn(
    "Profit_Margin",
    round((col("Profit") / col("Sales")) * 100, 2)
)

df_silver.select(
    "Sales",
    "Profit",
    "Profit_Margin"
).show(5)

# COMMAND ----------

from pyspark.sql.functions import trim, col

text_columns = [
    "Customer Name",
    "City",
    "State",
    "Country",
    "Category",
    "Sub-Category",
    "Product Name"
]

for c in text_columns:
    df_silver = df_silver.withColumn(c, trim(col(c)))

# COMMAND ----------

df_silver.select(
    "Customer Name",
    "City",
    "Country",
    "Product Name"
).show(5, False)

# COMMAND ----------

df_silver.filter(col("Shipping_Days") < 0).show()

# COMMAND ----------

df_silver.select(
    "Sales",
    "Profit",
    "Profit_Margin"
).show(10)

# COMMAND ----------

df_silver.select(
    "Shipping_Days",
    "Order_Year",
    "Order_Month",
    "Order_Quarter",
    "Profit_Margin"
).show(10)

# COMMAND ----------

df_silver.filter(col("Shipping_Days") < 0).count()

# COMMAND ----------

print("Silver Records:", df_silver.count())

# COMMAND ----------

df_silver = (
    df_silver
    .withColumnRenamed("Row ID", "Row_ID")
    .withColumnRenamed("Order ID", "Order_ID")
    .withColumnRenamed("Order Date", "Order_Date")
    .withColumnRenamed("Ship Date", "Ship_Date")
    .withColumnRenamed("Ship Mode", "Ship_Mode")
    .withColumnRenamed("Customer ID", "Customer_ID")
    .withColumnRenamed("Customer Name", "Customer_Name")
    .withColumnRenamed("Postal Code", "Postal_Code")
    .withColumnRenamed("Product ID", "Product_ID")
    .withColumnRenamed("Sub-Category", "Sub_Category")
    .withColumnRenamed("Product Name", "Product_Name")
    .withColumnRenamed("Shipping Cost", "Shipping_Cost")
    .withColumnRenamed("Order Priority", "Order_Priority")
)

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("RetailCatalog.RetailSchema.Sales_Silver")