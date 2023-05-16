
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, IntegerType,StringType,TimestampType


spark_session = SparkSession.builder.master("local[1]").appName("SparkSQL").getOrCreate()
spark_session.sparkContext.setLogLevel("ERROR")


if __name__ == "__main__":
    df_schema = StructType([
        StructField("order_id", IntegerType()),
        StructField("order_date", TimestampType()),
        StructField("order_customer_id", IntegerType()),
        StructField("order_status", StringType())]
    )

    df_non_s = spark_session.read.\
        format("csv").\
        option("header",True).\
        schema(df_schema).\
        option("path","C:/Users/001Y1S744/PycharmProjects/BP/PySpark/source/orders_noschema.csv").\
        load()

    df_non_s.show(n=10, truncate=False)

    df_non_s.selectExpr("order_id","order_date" ,"order_customer_id",
    "order_status").show(n=10, truncate=False)
    """
    df_s = spark_session.read.\
        format("csv").\
        option("header", "true"). \
        option("inferSchema", "true"). \
        option("path", "C:/Users/001Y1S744/PycharmProjects/BP/PySpark/source/orders-schema.csv").\
        load()

    """

    df_non_s.write.format("csv").\
        mode("Overwrite").\
        partitionBy("order_status").\
        bucketBy(4,"order_customer_id").\
        sortBy("order_customer_id").\
        saveAsTable("order1")
        
    """option("path", "C:/Users/001Y1S744/PycharmProjects/BP/PySpark/target_directory/")"""

