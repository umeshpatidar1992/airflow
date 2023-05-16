"""
Calculate:
1. column object >>df.select(count(*) as total,sum(sal) as sum_sal,avg(sal) as av_sal).show()
2. string operation >> df.selectExpr("count(*) as total","sum(sal) as sum_sal","avg(sal) as av_sal").
3. spark sql >> createOrReplceTempView
spark.sql(select count(*) as total,sum(sal) as sum_sal,avg(sal) as av_sal from table_name)

Aggregation : groupBy
df.groupBy("Col1","Col2").agg(sum()

Aggregate:
Window Function: Window.partitionBy
window = Window.partitionBy("source_file_name").orderBy(desc("creation_date"))
raw_df = east_raw.toDF()
raw_df = east_raw_df.select(east_raw_df.columns).\
            withColumn('rnk',F.dense_rank().over(window)).\
                filter("rnk in (1)").drop('rnk')



"""
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, IntegerType,StringType,TimestampType

spark_session = SparkSession.builder.master("local[1]").appName("SparkSQL").getOrCreate()
spark_session.sparkContext.setLogLevel("ERROR")


if __name__ == "__main__":
    print("Aggergation")