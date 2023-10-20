# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 22:42:50 2019

@author: saurav
"""

import pyspark
from pyspark.sql import SparkSession
import sys
import os
from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark: SparkSession

spark = SparkSession.builder.appName("SparkByExample.com").getOrCreate()
schema = StructType([StructField("seq",StringType(),True)])
dates = ["1"]
df = spark.createDataFrame(list('1'), schema = schema)
df.show()
# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
#
# schema = StructType([
#             StructField("seq", StringType(), True)])
#
# dates = ['1']
#
# df = spark.createDataFrame(list('1'), schema=schema)
#
# df.show()