import findspark


findspark.init()

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import explode


spark_session = SparkSession.builder.master("local[2]").appName("Collect_list_vs_Set").getOrCreate()
data1=[('sai',100),('sai',100),('sai',200),('ram',300),('bunny',400),('pramod',500),('pramod',500),('avi',5000),('avi',5000),('avi',800)]
header1=['name','marks']

df1 = spark_session.createDataFrame(data = data1, schema=header1)
df2 = df1.groupby('name').agg(F.collect_list('marks'))
df2.printSchema()
df3 = df2.select(df2.name, explode(df2.collect_list(marks)))
df3.printSchema()

#df3 = df1.groupby('name').agg(F.collect_set('marks')).alias('Collect_Set')