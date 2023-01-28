import findspark
findspark.init()
from pyspark.sql.functions import countDistinct

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


ss = SparkSession.\
    builder.master("local[1]").\
    appName("CountCheck").\
    getOrCreate()

df = ss.read.option("header", True)\
    .options(delimiter=',')\
    .csv("path of the file")
print("Extracted Data...", df.count())
df1 = df.distinct()
print("removed duplicates....", df1.count())

df.show(truncate = False)

'''
col = ['Source', 'SiteKey', 'POSSite', 'BPSiteNo', 'TransDate', 'TransNumber',
       'TransTimeStamp', 'TransType', 'LineType', 'BMC', 'SKU', 'Product', 'UnitRetail',
       'SalesRetail', 'PromoPrice', 'PromoNumber', 'PromoName', 'PackSize', 'PromoQuantity',
       'PromoDiscountAmount', 'FeeAmount', 'SalesQuantity', 'SalesAmount']


df = line_item.toDF(*col)
df.show(n=1)
df1 = df.withColumn("New_Date", F.to_timestamp('20220802130554020','yyyy-MM-dd HH:mm:ss'))
df1.printSchema()

df.registerTempTable("test")
agg_df = ss.sql("""SELECT 
                  TransDate as transaction_date, 
                  cast(SUM(SalesAmount),decimal(18,10)) AS total_sales_amount, 
                  SUM(salesquantity) AS total_sales_qty, 
                  COUNT(DISTINCT sitekey) AS total_site_key, 
                  COUNT(1) AS total_transaction
                  FROM test
                  GROUP BY  
                  transaction_date""")
'''