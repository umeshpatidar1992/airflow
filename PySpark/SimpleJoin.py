"""

Shuffle sort merge join


"""


import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, IntegerType,StringType,TimestampType


spark_session = SparkSession.builder.master("local[1]").appName("SparkSQL").getOrCreate()
spark_session.sparkContext.setLogLevel("ERROR")


if __name__ == "__main__":

    l1 = [("pirate",1),("monkey",2),("ninja",3),("rider",1)]

    l2 = [("hero", 1), ("monkey", 5), ("ninja", 4), ("rider", 1)]


    df1 = spark_session.createDataFrame(l1).toDF("name","id")
    df2 = spark_session.createDataFrame(l2).toDF("name","id")

    df1.printSchema()
    df2.printSchema()

    inner_df = df1.join(df2, df1.id == df2.id)
    inner_df.show()

    left_df = df1.join(df2, df1.id == df2.id, how="left")
    left_df.show()
    right_df = df1.join(df2, df1.id == df2.id, how="right")
    right_df.show()
    full = df1.join(df2, df1.id == df2.id, how="full")
    full.show()
