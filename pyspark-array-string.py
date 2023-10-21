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
spark = SparkSession.builder.master("local[1]")\
                            .appName("SparkByExample.com")\
                            .getOrCreate()


columns = ["name","languageAtSchool","currentState"]
data = [("James,Smith",["Java","Scala","C++"],"CA"), \
     ("Michael,Rose,",["Spark","Java","C++"],"NJ"), \
     ("Robert,,Williams",["CSharp","VB"],"NV")]
# columns = ["name","languagesAtSchool","currentState"]
# data = [("James,,Smith",["Java","Scala","C++"],"CA"), \
#     ("Michael,Rose,",["Spark","Java","C++"],"NJ"), \
#     ("Robert,,Williams",["CSharp","VB"],"NV")]

df = spark.createDataFrame(data=data,schema=columns)
df.printSchema()
df.show()
# df = spark.createDataFrame(data=data,schema=columns)
# df.printSchema()
# df.show(truncate=False)

from pyspark.sql.functions import col,concat_ws
df2= df.withColumn("languageAtSchool",
              concat_ws(",",col("languageAtSchool")))

df2.printSchema()
df2.show(truncate=False)
# from pyspark.sql.functions import col, concat_ws
# df2 = df.withColumn("languagesAtSchool",
#    concat_ws(",",col("languagesAtSchool")))
# df2.printSchema()
# df2.show(truncate=False)

df.createOrReplaceTempView("ARRAY_STRING")

spark.sql("select name,concat_ws(',',languageAtSchool) as languageAtSchool, currentState from ARRAY_STRING").show()
# df.createOrReplaceTempView("ARRAY_STRING")
# spark.sql("select name, concat_ws(',',languagesAtSchool) as languagesAtSchool,currentState from ARRAY_STRING").show(truncate=False)
