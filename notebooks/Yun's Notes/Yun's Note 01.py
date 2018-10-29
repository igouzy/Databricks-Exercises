# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Lesson 1 - Getting Started ###
# MAGIC 
# MAGIC ##### Practice 1 #####
# MAGIC 
# MAGIC 1. Read 'README.md' file and count lines
# MAGIC 2. Split word by space and count by word

# COMMAND ----------

# MAGIC %py
# MAGIC import re
# MAGIC 
# MAGIC sourceFile = "databricks-datasets/README.md"
# MAGIC textFile = sc.textFile(sourceFile)
# MAGIC textFile.count()
# MAGIC 
# MAGIC linesWithFilter = textFile.filter(lambda line : "Databricks" in line).collect()
# MAGIC print linesWithFilter
# MAGIC 
# MAGIC countByWord = textFile.flatMap(lambda line : re.split('[ |,]', line)).map(lambda x : (x, 1)).reduceByKey(lambda x, y : x + y).collect()
# MAGIC print sorted(countByWord)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Practice 2 #####
# MAGIC 
# MAGIC 1. Create tables with 'Classroom-Setup' script

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %fs ls /mnt/training/dataframes/people-10m.parquet

# COMMAND ----------

peopleDF = spark.read.parquet("/mnt/training/dataframes/people-10m.parquet")
peopleDF.printSchema()

# COMMAND ----------

