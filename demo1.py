# Databricks notebook source
import pyspark

# COMMAND ----------

from pyspark import SparkContext

# COMMAND ----------

sc = SparkContext.getOrCreate()

# COMMAND ----------

karthick = list(range(1,100))

# COMMAND ----------

print(karthick)

# COMMAND ----------

print(type(karthick))

# COMMAND ----------

karthickrdd = sc.parallelize(karthick)

# COMMAND ----------

print(type(karthickrdd))

# COMMAND ----------

print(karthickrdd)

# COMMAND ----------

karthickrdd.collect()

# COMMAND ----------

selvamrdd = karthickrdd

# COMMAND ----------

print(selvamrdd)

# COMMAND ----------

print(type(selvamrdd))

# COMMAND ----------

selvamrdd.collect()

# COMMAND ----------

selvamrdd.count()

# COMMAND ----------

karthickrdd.count()

# COMMAND ----------

karthickrdd.first()

# COMMAND ----------

karthickrdd.take(5)

# COMMAND ----------

karrdd = karthickrdd.map(lambda x: (x + 1000))

# COMMAND ----------

karthickrdd.take(5)

# COMMAND ----------

karrdd.take(5)

# COMMAND ----------

selvamrdd.take(1)

# COMMAND ----------

selrdd = selvamrdd.map(lambda x: (x,1))

# COMMAND ----------

selrdd.take(5)

# COMMAND ----------

karthickrdd.count()

# COMMAND ----------

karrdd.count()

# COMMAND ----------

demo=sc.parallelize(["apple","mango","strawberry","cherry","grapes"])

# COMMAND ----------

demo.collect()

# COMMAND ----------

fruits=demo.map(lambda x: (x))

# COMMAND ----------

fruits.collect()

# COMMAND ----------

numbers = karthickrdd

# COMMAND ----------

print(type(numbers))

# COMMAND ----------

from operator import add

# COMMAND ----------

nums = numbers

# COMMAND ----------

adding = nums.reduce(add)

# COMMAND ----------

print(adding)

# COMMAND ----------

print(type(adding))

# COMMAND ----------

numfilter = selvamrdd.filter(lambda x: 'Iceland' in x)

# COMMAND ----------

print(type(numfilter))

# COMMAND ----------

print(numfilter)

# COMMAND ----------

numfilter.collect()

# COMMAND ----------

numfilter.count()

# COMMAND ----------

numcollection = list(range(1,1000))

# COMMAND ----------

numcollection1 = list(range(1000,2000))

# COMMAND ----------

numcollectionrdd = sc.parallelize(numcollection)

# COMMAND ----------

numcollectionrdd1 = sc.parallelize(numcollection1)

# COMMAND ----------

numcollectionrdd1.count()

# COMMAND ----------

numcollectionrdd.count()

# COMMAND ----------

joined = numcollectionrdd.join(numcollectionrdd1)

# COMMAND ----------

collection=sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark"]
)

# COMMAND ----------

collection1=sc.parallelize(["akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"])

# COMMAND ----------

joined=collection.join(collection1)

# COMMAND ----------

collection.collect()

# COMMAND ----------

collection1.collect()

# COMMAND ----------

joined.collect()

# COMMAND ----------

x = sc.parallelize([("spark", 1), ("hadoop", 4)])
y = sc.parallelize([("spark", 2), ("hadoop", 5)])
joined = x.join(y)

# COMMAND ----------

joined.collect()

# COMMAND ----------

selvamrdd.collect()

# COMMAND ----------

selvamrdd.take(1)

# COMMAND ----------

selvamflatrdd= selvamrdd.flatMap(lambda x: x.split(","))

# COMMAND ----------

selvamflatrdd.take(15)

# COMMAND ----------


