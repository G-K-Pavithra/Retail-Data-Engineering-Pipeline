# Databricks notebook source
df_gold = spark.table("RetailCatalog.RetailSchema.Sales_Silver")

df_gold.show(5)

# COMMAND ----------

dim_customer = (
    df_gold
    .select(
        "Customer_ID",
        "Customer_Name",
        "Segment"
    )
    .dropDuplicates()
)

# COMMAND ----------

dim_customer.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("RetailCatalog.RetailSchema.DimCustomer")

# COMMAND ----------

dim_product = (
    df_gold
    .select(
        "Product_ID",
        "Product_Name",
        "Category",
        "Sub_Category"
    )
    .dropDuplicates()
)

# COMMAND ----------

dim_product.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("RetailCatalog.RetailSchema.Dimproduct")

# COMMAND ----------

dim_location = (
    df_gold
    .select(
        "City",
        "State",
        "Country",
        "Market",
        "Region"
    )
    .dropDuplicates()
)

# COMMAND ----------

dim_location.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("RetailCatalog.RetailSchema.Dimlocation")

# COMMAND ----------

dim_date = (
    df_gold
    .select(
        "Order_Date",
        "Order_Year",
        "Order_Month",
        "Order_Quarter"
    )
    .dropDuplicates()
)

# COMMAND ----------

dim_date.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("RetailCatalog.RetailSchema.Dimdate")

# COMMAND ----------

fact_sales = df_gold.select(
    "Row_ID",
    "Order_ID",
    "Order_Date",
    "Customer_ID",
    "Product_ID",
    "City",
    "Sales",
    "Quantity",
    "Discount",
    "Profit",
    "Shipping_Cost",
    "Shipping_Days",
    "Profit_Margin"
)

# COMMAND ----------

fact_sales.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("RetailCatalog.RetailSchema.FactSales")

# COMMAND ----------

spark.sql("SHOW TABLES IN RetailCatalog.RetailSchema").show()

# COMMAND ----------

tables = [
    "DimCustomer",
    "DimProduct",
    "DimLocation",
    "DimDate",
    "FactSales"
]

for table in tables:
    print(f"\n===== {table} =====")
    spark.table(f"RetailCatalog.RetailSchema.{table}").show(5, truncate=False)

# COMMAND ----------

tables = [
    "Sales_Silver",
    "DimCustomer",
    "DimProduct",
    "DimLocation",
    "DimDate",
    "FactSales"
]

for table in tables:
    count = spark.table(f"RetailCatalog.RetailSchema.{table}").count()
    print(f"{table}: {count}")