# -*- coding: utf-8 -*-
"""
author Saurav
"""
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


from pyspark.sql import SparkSession

spark: SparkSession
spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

from pyspark.sql.functions import col,expr

# from pyspark.sql.functions import col,expr
data = [("2019-01-23",1),("2019-06-24",2),("2019-09-20",3)]
# data=[("2019-01-23",1),("2019-06-24",2),("2019-09-20",3)]


spark.createDataFrame(data).toDF("date","increment")\
    .select(col("date"),col("increment"),\
            expr("add_months(to_date(date,'yyyy-MM-dd'),cast(increment as int))").alias("inc_date"))\
    .show()

# spark.createDataFrame(data).toDF("date","increment") \
#     .select(col("date"),col("increment"), \
#       expr("add_months(to_date(date,'yyyy-MM-dd'),cast(increment as int))").alias("inc_date")) \
#     .show()