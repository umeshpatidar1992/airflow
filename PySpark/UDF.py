
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType

if __name__ == '__main__':
    ss = SparkSession.builder.master('local[*]').appName("test").getOrCreate()
    def bonus(sal,per):
        return sal*per
    ss.udf.register('bunus', bonus, DoubleType())
    ls = [(1, 5000),(2,10000)]

    schema = ['id','sal']
    rdd = ss.sparkContext.parallelize(ls)
    df = rdd.toDF(schema= schema)

    inc_df = df.withColumn('bonus', bonus(df.sal, 10.0))
    inc_df.show()


