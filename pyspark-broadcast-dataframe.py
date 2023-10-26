# -*- coding: utf-8 -*-
'''
Created on Sat Jan 11 19:38:27 2020

@author: sparkbyexamples.com
'''

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# from pyspark.sql import SparkSession
from pyspark.sql import SparkSession

spark: SparkSession
spark = SparkSession.builder.master("local[3]").appName("SparkByExample.com").getOrCreate();
# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# states = {"NY":"New York", "CA":"California", "FL":"Florida"}
states = {"NY": "New York", "CA": "California", "FL": "Florida"}
broadcastStates = spark.sparkContext.broadcast(states)
# broadcastStates = spark.sparkContext.broadcast(states)

# data = [("James","Smith","USA","CA"),
#     ("Michael","Rose","USA","NY"),
#     ("Robert","Williams","USA","CA"),
#     ("Maria","Jones","USA","FL")
#   ]

data = [("James","Smith","USA","CA"),
     ("Michael","Rose","USA","NY"),
     ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
    ]

# columns = ["firstname","lastname","country","state"]
columns = ["firstname","lastname","country","state"]
df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
df.show()

# df = spark.createDataFrame(data = data, schema = columns)
# df.printSchema()
# df.show(truncate=False)

# def state_convert(code):
#     return broadcastStates.value[code]

def state_convert(code):
    return broadcastStates.value[code]


result = df.rdd.map(lambda x: (x[0], x[1], x[2], state_convert(x[3]))).toDF(columns)
# result = df.rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).toDF(columns)
result.show(truncate=False)

# Broadcast variable on filter
filterDf = df.where((df['state'].isin(broadcastStates.value)))
# filteDf= df.where((df['state'].isin(broadcastStates.value)))

input("Enter any key")
