# Databricks notebook source
from pyspark.sql import *

# COMMAND ----------

movies = spark.read.csv("/FileStore/tables/movies-2.csv", header = True)

# COMMAND ----------

print(type(movies))

# COMMAND ----------

movies.collect()

# COMMAND ----------

movies.collect()

# COMMAND ----------

movies.take(2)

# COMMAND ----------

movies.count()

# COMMAND ----------

movies.first()

# COMMAND ----------

movies.head()

# COMMAND ----------

movies.head(3)

# COMMAND ----------

movies.show()

# COMMAND ----------

movies.show(25)

# COMMAND ----------

movies.printSchema()

# COMMAND ----------

karthick = spark.read.csv("/FileStore/tables/movies-2.csv", header = True,inferSchema = True)

# COMMAND ----------

karthick.printSchema()

# COMMAND ----------

len(movies.columns)

# COMMAND ----------

movies.columns

# COMMAND ----------

movies.describe('movieId').show()

# COMMAND ----------

movies.registerTempTable('karthick')

# COMMAND ----------

sqlContext.sql('select * from karthick where movieId > 100').show()

# COMMAND ----------


