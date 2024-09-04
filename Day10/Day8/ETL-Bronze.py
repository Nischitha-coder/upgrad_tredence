# Databricks notebook source
dbutils.widgets.text("environment", "dev")
v=dbutils.widgets.get("environment")

# COMMAND ----------

# DBTITLE 1,automate this path
# MAGIC %run /Workspace/Users/r_1724385724741@npupgradassessment.onmicrosoft.com/Day10/Day8/Includes

# COMMAND ----------

input_path

# COMMAND ----------

# df1.withColumn("environment", lit(v).display())

# COMMAND ----------

# df = spark.read.csv(f"{input_path}products.csv", header=True, inferSchema=True)
df = spark.read.csv("dbfs:/FileStore/tables/products.csv", header=True, inferSchema=True)

# COMMAND ----------

# df1 = df.withColumn("ingestion_date")
df1 = add_ingestion_col(df)

# COMMAND ----------

df2 = df1.withColumn("environment", lit(v))
df2.write.mode("overwrite").mode("overwrite").option("mergeSchema","true").saveAsTable("upgrad_databricks_1095471328393118.bronze.products_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from upgrad_databricks_1095471328393118.bronze.products_bronze

# COMMAND ----------


