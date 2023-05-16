"""
Broadcast join
"""
import findspark
from pyspark.sql.functions import explode

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == "__main__":


    schema = StructType(
        [StructField("id", IntegerType()),
         StructField("empname", StringType()),
         StructField("age", IntegerType()),
         StructField("salary", IntegerType()),
         StructField("addess", StringType()),
         StructField("deptid", IntegerType())
         ]
    )

    ss = SparkSession.builder.master("local[*]").appName("OptimisedJoin").getOrCreate()
    emp_df = ss.read. \
        format("json"). \
        option("header", True). \
        option("inferSchema", True).\
        option("path", "C:/Users/001Y1S744/PycharmProjects/BP/PySpark/source/employee.json"). \
        load()

    clean_emp_df = emp_df.select(emp_df.id,emp_df.empname,emp_df.salary,emp_df.age,emp_df.deptid,
                                 explode(emp_df.address))
    clean_emp_df.show()

    dept_df = ss.read. \
        format("json"). \
        option("header", True). \
        option("inferSchema", True). \
        option("path", "C:/Users/001Y1S744/PycharmProjects/BP/PySpark/source/dept.json"). \
        load()