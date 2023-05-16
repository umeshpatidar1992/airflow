import findspark


findspark.init()

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StringType, StructField, DecimalType, DateType, IntegerType
from pyspark.sql.functions import desc, dense_rank
from pyspark.sql import functions as F


ss = SparkSession.builder.master('local[*]').appName('TestSpark').getOrCreate()
'''
#manual Data
data_list = ['Project Gutenberg’s',
                'Alice’s Adventures in Wonderland'
                'Project Gutenberg’s',
                'Adventures in Wonderland',
                'Project Gutenberg’s']
# RDD Operation
rdd = ss.sparkContext.parallelize(data_list)
#rdd =  ss.sparkContext.textFile("C:/Users/001Y1S744/PycharmProjects/BP/PySpark/source/test_date.csv")
for element in rdd.collect():
    print(element)

flat_map_rdd = rdd.flatMap(lambda x: x.split(" "))
for element in flat_map_rdd.collect():
    print(element)


map_rdd = flat_map_rdd.map(lambda x: (x,1))
for element in map_rdd.collect():
    print(element)

reduced_rdd = map_rdd.reduceByKey(lambda x,y : x+y)
for key,value in reduced_rdd.collect():
    print(key, value)
'''

def_schema = StructType(
                [StructField('branch',StringType()),
                StructField('date',StringType()),
                StructField('seller',DateType()),
                StructField('item',StringType()),
                StructField('quantity',DecimalType(10,10)),
                StructField('unit_price',IntegerType())]
            )

sdf = ss.read.format("csv").\
    option("header",True).\
    option("inferSchema",True).\
    option("path", "C:/Users/001Y1S744/PycharmProjects/BP/PySpark/source/test_date.csv").\
    load()


window = Window.partitionBy('branch').orderBy(desc('unit_price'))

df1 = sdf.select('branch','unit_price').\
    withColumn('rnk', F.dense_rank().over(window)).\
    filter(F.col('rnk') == 1)

df1.show()






