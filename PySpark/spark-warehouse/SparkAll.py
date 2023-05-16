'''
1. Spark Session
2. Read File
3. Create Schema
4. Manually Data
5. High level API
    a) withColumn
    b) withColumnRenamed
    c) dropDuplicate, distinct
    d) partition, repartition, coalease,
    e) filter,where, when, F.lit
    f) window function
    g) UDF, Explode, broadcast, accumulator
    h) join's
    i) cache, persist
    h) collect, take, show, printSchema
    j) map, flatmap, groupBykey,
'''

import findspark


findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.functions import coalesce
ss = SparkSession.builder.master('local[*]').appName('PlayBook').getOrCreate()


df = ss.createDataFrame([('2023-03-03','2023-03-04'),(None, None)],('a','b'))
#df1 = df.withColumn('a', (df['a'].cast(DateType()))).withColumn('b',(df['b'].cast(DateType())))

df2.select('*').show()

'''

data_list = ['Project Gutenberg’s',
                'Alice’s Adventures in Wonderland'
                'Project Gutenberg’s',
                'Adventures in Wonderland',
                'Project Gutenberg’s']

print("FlatMap")

rdd = ss.sparkContext.parallelize(data_list)

rdd1 = rdd.flatMap(lambda x: x.split(" "))
rdd2 = rdd1.map(lambda x: (x,1))
rdd3 = rdd2.reduceByKey(lambda x,y : x+y)

'''

for element in rdd3.collect():
    print(element)