import findspark

findspark.init()

from pyspark.sql import SparkSession

from logging import *

ss = SparkSession.builder.master('local[*]').appName("Test Knowledge").getOrCreate()
ss.sparkContext.setLogLevel("ERROR")
rdd1 = ss.sparkContext.textFile("C:/Users/001Y1S744/PycharmProjects/BP/PySpark/data.txt")

rdd2 = rdd1.flatMap(lambda x: x.split(" "))
rdd3 = rdd2.map(lambda x: (x,1))

rdd4 = rdd3.reduceByKey(lambda x,y: x+y)

for element in rdd4.collect():
    print(element)
