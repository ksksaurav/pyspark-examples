# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import pandas as pd    
# data = [['Scott', 50], ['Jeff', 45], ['Thomas', 54],['Ann',34]]

data = [['Scott', 30],['Abott', 50],['Trott', 60]]
# Create the pandas DataFrame


pandasDF = pd.DataFrame(data=data,columns=['Name','Age'])
# pandasDF = pd.DataFrame(data, columns = ['Name', 'Age'])
  
# print dataframe. 
# print(pandasDF)
print(pandasDF)

from pyspark.sql import  SparkSession

spark: SparkSession
# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder \
#     .master("local[1]") \
#     .appName("SparkByExamples.com") \
#     .getOrCreate()

spark=SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate();

sparkDF = spark.createDataFrame(pandasDF)
sparkDF.printSchema()
sparkDF.show()
# sparkDF=spark.createDataFrame(pandasDF)
# sparkDF.printSchema()
# sparkDF.show()

from pyspark.sql.types import StructType,StructField,StringType,IntegerType
mySchema = StructType([StructField("First Name ",StringType(),True),
                       StructField("Age", IntegerType(),True)])


sparkDF2 = spark.createDataFrame(pandasDF,schema=mySchema)

#sparkDF=spark.createDataFrame(pandasDF.astype(str)) 
# from pyspark.sql.types import StructType,StructField, StringType, IntegerType
# mySchema = StructType([ StructField("First Name", StringType(), True)\
#                        ,StructField("Age", IntegerType(), True)])

sparkDF2.printSchema()
sparkDF2.show()
# sparkDF2 = spark.createDataFrame(pandasDF,schema=mySchema)
# sparkDF2.printSchema()
# sparkDF2.show()


spark.conf.set("spark.sql.execution.arrow.enabled",'true')
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
# spark.conf.set("spark.sql.execution.arrow.enabled","true")
# spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")

pandasDF2 = sparkDF2.select("*").toPandas()
print(pandasDF2)
# pandasDF2=sparkDF2.select("*").toPandas
# print(pandasDF2)

test=spark.conf.get("spark.sql.execution.arrow.enabled")
print(test)
# test=spark.conf.get("spark.sql.execution.arrow.enabled")
# print(test)

test123 = spark.conf.get("spark.sql.execution.arrow.pyspark.fallback.enabled")
# test123=spark.conf.get("spark.sql.execution.arrow.pyspark.fallback.enabled")
# print(test123)
print(test123)