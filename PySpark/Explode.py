

import findspark


findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode


arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})]


if __name__ == '__main__':
    spark = SparkSession.builder.master("local[1]").appName("Explode Example").getOrCreate()
    df = spark.createDataFrame(data =arrayData , schema=['name','knownLanguages','properties'])
    df.printSchema()
    df.show()

    # Can not use to explode two col in same statement >> through multiple generator error
    df1 = df.select(df.name, explode(df.knownLanguages))
    df1.printSchema()
    df1.show()
    df2 = df.select(df.name, explode(df.properties))
    df2.printSchema()
    df2.show()