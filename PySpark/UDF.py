
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType

if __name__ == '__main__':
    ss = SparkSession.builder.master('local[*]').appName("test").getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")
    def bunus_cal(sal,per):
        return sal*per
    ss.udf.register('bunus_cal', bunus_cal, DoubleType())
    ls = [(1, 5000),(2,10000)]

    schema = ['id','sal']
    rdd = ss.sparkContext.parallelize(ls)
    df = rdd.toDF(schema= schema)

    #inc_df = df.withColumn('bonus', bonus(df.sal, 10.0))
    df.createOrReplaceTempView("temp_table")
    df_sql = ss.sql("""SELECT id,sal,bunus_cal(sal,10.0) as bonus from temp_table""")
    df_sql.show()


