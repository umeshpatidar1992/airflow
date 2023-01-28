import findspark
findspark.init()
from pyspark.sql import SparkSession

if __name__ == '__main__':
    ss = SparkSession.builder.master('local[*]').appName("Interview Practice").getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")
    sc = ss.sparkContext
    ls = list(range(1,10))
    rdd = sc.parallelize(ls)
    rdd1 = rdd.treeReduce(lambda a,b:a+b)
    ls1 = rdd1.collect()
    for i in ls1:
        print(i)