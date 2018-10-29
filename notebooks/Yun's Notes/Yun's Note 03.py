# Databricks notebook source
# MAGIC %py
# MAGIC 
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC 
# MAGIC ehConf = {
# MAGIC   'eventhubs.connectionString' : "Endpoint=sb://apj-eh.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=hf9vqikmN3DLWng8uYQBqq2Z0GOpgLAIaU4NeNVjH6o=",
# MAGIC }
# MAGIC 
# MAGIC df = spark.readStream.format("eventhubs").options(**ehConf).load()
# MAGIC 
# MAGIC readInStreamBody = df.withColumn("body", df["body"].cast("string"))
# MAGIC display(readInStreamBody)
# MAGIC 
# MAGIC Schema = StructType([
# MAGIC   StructField("currentTime", StringType(), True),
# MAGIC   StructField("objectId", StringType(), True),
# MAGIC   StructField("routeId", StringType(), True),
# MAGIC   StructField("lng", StringType(), True),
# MAGIC   StructField("lat", StringType(), True),
# MAGIC   StructField("origin", StringType(), True),
# MAGIC   StructField("destination", StringType(), True),
# MAGIC   StructField("velocity", StringType(), True)
# MAGIC ])
# MAGIC rawData = df.selectExpr("cast(Body as string) as json").select(from_json("json", Schema).alias("data")).select("data.*") 
# MAGIC 
# MAGIC readInStreamBody = df.withColumn("body", df["body"].cast("string"))
# MAGIC display(readInStreamBody)

# COMMAND ----------

