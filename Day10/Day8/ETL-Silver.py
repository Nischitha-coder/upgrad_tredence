# Databricks notebook source
# MAGIC %run /Workspace/Users/r_1724385724741@npupgradassessment.onmicrosoft.com/Day10/Day8/Includes

# COMMAND ----------

df = spark.read.table("upgrad_databricks_1095471328393118.bronze.products_bronze")

# COMMAND ----------

df.dropDuplicates().count()
# so there are no duplicates

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from upgrad_databricks_1095471328393118.bronze.products_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists silver.products_silver as (select ProductID as product_id, ProductName as product_name ,Category as category, ListPrice as list_price from upgrad_databricks_1095471328393118.bronze.products_bronze)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists gold.category_count as select category, count(*) as count from upgrad_databricks_1095471328393118.silver.products_silver group by category order by count desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.category_count
