import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

from pyspark.sql import functions as F

ss = SparkSession.builder.master('local[*]').appName("Test Knowledge").getOrCreate()
ss.sparkContext.setLogLevel("ERROR")
data1 = [(1, "John", 25), (2, "Jane", 30)]
data2 = [(1, "John", 25), (2, "Jane", 30)]

df1 = ss.createDataFrame(data1, schema=["id", "name", "age"])
df2 = ss.createDataFrame(data2, schema=["id", "name", "age"])

df1 = df1.withColumn('hash', F.hash('id','name'))
df2 = df2.withColumn('hash', F.hash('id','name'))

df3 = df1.unionAll(df2)



