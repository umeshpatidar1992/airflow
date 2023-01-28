import findspark

findspark.init()
import self as self
from pyspark.sql.functions import udf

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import StringType,DateType

class TestPySpark:
    def check_count(self,ss):
        data_list = ['Project Gutenberg’s',
                'Alice’s Adventures in Wonderland'
                'Project Gutenberg’s',
                'Adventures in Wonderland',
                'Project Gutenberg’s']

        rdd = ss.sparkContext.parallelize(data_list)
        for element in rdd.collect():
            print(element)

        print("Flat Map")
        # Flatmap
        rdd2 = rdd.flatMap(lambda x: x.split(" "))
        for element in rdd2.collect():
            print(element)

        print("Map")
        # MAP
        rdd3 = rdd2.map(lambda x : (x,1))
        for element in rdd3.collect():
            print(element)

        # Reduce By Key
        rdd4 = rdd3.reduceByKey(lambda x,y: x+y)
        for element in rdd4.collect():
            print(element)


    def data_frame(self,ss):
        data = [('James', '', 'Smith', '20220802130554020', 'M', 3000),
                ('Michael', 'Rose', '', '20220802130554020', 'M', 4000),
                ('Robert', '', 'Williams', '20220802130554020', 'M', 4000),
                ('Maria', 'Anne', 'Jones', '20220802130554020', 'F', 4000),
                ('Jen', 'Mary', 'Brown', '20220802130554020', 'F', -1)
                ]

        columns = ["firstname", "middlename", "lastname", "transtime", "gender", "salary"]
        df = ss.createDataFrame(data, columns)



        df1 = df.withColumn("dh_audit_record_timestamp_new3",
                           F.lit(F.col('transtime').cast(StringType())))

        df1.printSchema()
        df1.show(n=5)

if __name__ == '__main__':
    ss = SparkSession.builder.master('local[1]').appName('Test').getOrCreate()
    TestPySpark.check_count(self,ss)
    #TestPySpark.data_frame(self,ss)

