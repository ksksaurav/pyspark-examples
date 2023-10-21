# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,ArrayType,StructType,StructField

spark: SparkSession
spark = SparkSession.builder\
                    .appName("SparkByExample.com")\
                    .getOrCreate()

# from pyspark.sql import SparkSession
# from pyspark.sql.types import StringType, ArrayType,StructType,StructField
# spark = SparkSession.builder \
#                     .appName('SparkByExamples.com') \
#                     .getOrCreate()

arrayCol = ArrayType(StringType(),False)
# arrayCol = ArrayType(StringType(),False)

data = [
    ("James,,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
   ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
   ("Robert,,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")
        ]


schema = StructType([
    StructField("name",StringType(),True),
    StructField("languagesAtSchool",ArrayType(StringType()),True),
    StructField("languagesAtwork",ArrayType(StringType()),True),
    StructField("currentState",StringType(),True),
    StructField("previousState",StringType(),True)
])
# schema = StructType([
#     StructField("name",StringType(),True),
#     StructField("languagesAtSchool",ArrayType(StringType()),True),
#     StructField("languagesAtWork",ArrayType(StringType()),True),
#     StructField("currentState", StringType(), True),
#     StructField("previousState", StringType(), True)
#   ])

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show()
# df = spark.createDataFrame(data=data,schema=schema)
# df.printSchema()
# df.show()

from pyspark.sql.functions import explode
df.select(df.name,explode(df.languagesAtSchool)).show()

from pyspark.sql.functions import  split
df.select(split(df.name,',').alias("nameAsArray")).show()

from pyspark.sql.functions import array
df.select(df.name,array(df.currentState,df.previousState).alias("States")).show()

from pyspark.sql.functions import array_contains
df.select(df.name,array_contains(df.languagesAtSchool,"Java").alias("Array_contains")).show()
# from pyspark.sql.functions import array_contains
# df.select(df.name,array_contains(df.languagesAtSchool,"Java")
#     .alias("array_contains")).show()
