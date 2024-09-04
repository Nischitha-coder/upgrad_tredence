# Databricks notebook source
# MAGIC %md
# MAGIC Batch
# MAGIC df = spark.read.csv("path")
# MAGIC df.write.mode("append").saveAsTable("table_name")
# MAGIC
# MAGIC Spark Structured Streaming
# MAGIC df = spark.readStream.csv("path")
# MAGIC df.writeStream.table("table_name")
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Users/r_1724385724741@npupgradassessment.onmicrosoft.com/Day8/Includes

# COMMAND ----------

user_schema = "Id int, Name string, Gender string, Salary int, Country string, Date string"

# COMMAND ----------

input_path

# COMMAND ----------

# path should be upto the folder and not the file
df = spark.readStream.schema(user_schema).csv(f"{input_path}", header=True)

# COMMAND ----------

df.writeStream.option("checkpointLocation","dbfs:/FileStore/tables/checkpoint").trigger(once=True).table("stream")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stream

# COMMAND ----------

df.withColumn("number", split("Name", ","))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stream

# COMMAND ----------


