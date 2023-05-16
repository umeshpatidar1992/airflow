import findspark
import self

findspark.init()

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, ArrayType, StructType, StructField, StringType, IntegerType, DecimalType
from datetime import timedelta
from pyspark.sql.functions import current_date, add_months, datediff, months_between, month

if __name__ == "__main__":
    spark_session = SparkSession.builder.master("local[1]").appName("SparkSQL").getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")


    data_list = [("2023-04-10",1112,"EAST"),
                 ("2023-04-10", 1112, "EAST"),
                 ("2023-04-05",1112,"EAST"),
                 ("2023-04-08",1112,"EAST"),
                 ("2023-04-07",1112,"EAST"),
                 ("2023-04-06",1112,"EAST"),
                 ("2023-04-10", 1113,"WEST"),
                 ("2023-04-09", 1113,"WEST"),
                 ("2023-04-08", 1113,"WEST"),
                 ("2023-04-07", 1113,"WEST"),
                 ("2023-04-06", 1113,"WEST")]

    df = spark_session.createDataFrame(data_list).toDF("SurveyDate","OpisId","Category")
    df = df.withColumn("SurveyDate", F.col("SurveyDate").cast(DateType()))
    '''
    def missing_dates(ref_date, days):
        """ create missing dates array
            returns list of [ref_date, ref_date+1D, ... ref_date+daysD]
        """

        ls_missing_dates = []

        if days == 0:
            ls_missing_dates = [ref_date]
        elif days is None:
            ls_missing_dates = [ref_date]
        elif days == -1:
            ls_missing_dates = [ref_date]
            ls_missing_dates.append(ref_date + timedelta(days=1))
        else:
            for day in range(0, days + 1):
                ls_missing_dates.append(ref_date + timedelta(days=day))

        return ls_missing_dates


    missing_dates_function = F.udf(missing_dates, ArrayType(DateType()))

    df_NextSurveyDate = df.coalesce(1).orderBy(F.col("OpisId"), F.col("SurveyDate")) \
        .withColumn("NextSurveyDate",
                    F.lead(F.col("SurveyDate")).over(Window.partitionBy("OpisId").orderBy("SurveyDate")))

    # df_NextSurveyDate.select(F.max("SurveyDate")).show()

    df_LatestSurveyDate = df_NextSurveyDate.withColumn("LatestSurveyDate", F.max(F.col("SurveyDate")).over(
        Window.partitionBy("Category")))
    # df_LatestSurveyDate.select(F.max("SurveyDate")).show()

    df_currentDays = df_LatestSurveyDate.withColumn("currentDays",
                                                    F.datediff(F.col("LatestSurveyDate"), F.col("SurveyDate")))

    # df_currentDays.select(F.max("SurveyDate")).show()

    df_days = df_currentDays.withColumn("days", F.when(F.col("NextSurveyDate").isNull(), F.col("currentDays")) \
                                        .otherwise(F.datediff(F.col("NextSurveyDate"), F.col("SurveyDate")))-1)
    df_days = df_days.withColumn("days",
                                 F.when(F.col("days") < F.col("currentDays"), F.col("days")).otherwise(
                                     F.col("currentDays")))

    df_days = df_days.withColumn("NextSurveyDate", \
                                 missing_dates_function(F.col("SurveyDate"), \
                                                        F.col("days")))


    df_days = df_days.filter(F.size(F.col("NextSurveyDate")) <= 3) \
        .withColumn("NextSurveyDate", F.explode(F.col("NextSurveyDate"))) \
        .withColumn("hasCalculation", F.when(F.col("NextSurveyDate") != F.col("SurveyDate"), True) \
                    .otherwise(False))
    # \
    # .drop(*["SurveyDate", "LatestSurveyDate"]) \
    # .withColumnRenamed("NextSurveyDate", "SurveyDate")

    df_days = df_days.filter(F.col("NextSurveyDate") <= current_date())

    df_days.show()
    def test_date.csv(date_str1,date_str2):
        ls = [('2023-04-05', '2023-04-10')]
        df1 = spark_session.createDataFrame(data=ls).toDF("date1","date2")
        df.createOrReplaceTempView('df')
        df1.createOrReplaceTempView('df1')
        df3 = spark_session.sql(f"SELECT count(*) from df "
                                f"WHERE SurveyDate  > cast ('{date_str1}' as date ) AND "
                                f"SurveyDate  < cast ('{date_str2}' as date)")


    #.format(date_str1, date_str2))
        df3.show()
    

    df1 = spark_session.sql(""" select 
                case 
                    when '2022-11-28' < add_months('2022-01-07', 12) 
                then 
                    case
                        when(DATEDIFF('2022-11-28', '2022-01-07')+1) > 0
                            then (DATEDIFF( '2022-11-28', '2022-01-07')+1) 
                        else 0
                    end
            else 
                DATEDIFF(add_months('2022-01-07',12),'2022-01-07') 
        end as churn_duration from df
        """)

    #df1.show()

    df2 = spark_session.sql("""SELECT DATEDIFF('2022-11-28','2022-01-07')+1 from df""")
    #df2.show()

    df3 = spark_session.sql("""SELECT DATEDIFF('2022-01-07', add_months('2022-01-07',12)) from df""")
    #df3.show()

    '''

    df_schema = StructType([ \
        StructField("account", StringType(), True), \
        StructField("sci_trans_key", StringType(), True), \
        StructField("fuel_quantity", StringType(), True), \
        StructField("fuel_flag", IntegerType(), True)
        ])

    df4 = spark_session.read.\
        format("csv"). \
        option("header", True). \
        load("C:/Users/001Y1S744/Downloads/labcode/target-file.csv")
    df4.printSchema()
    df4.createOrReplaceTempView('df4')

    df4 = spark_session.sql("""select account,
                            (count(distinct case when fuel_quantity > 0.0 and fuel_quantity <=25.0 and fuel_flag=1 then sci_trans_key end))*1.0 /
                            ((case when count(distinct sci_trans_key) = 0 then 1 else 1.00* count(distinct sci_trans_key) end)) as share_fuel_trans_0_25,
                            (count(distinct case when fuel_quantity > 25.0 and fuel_quantity <=50.0 and fuel_flag=1 then sci_trans_key end))*1.0 /
                            ((case when count(distinct sci_trans_key) = 0 then 1 else 1.00* count(distinct sci_trans_key) end)) as share_fuel_trans_25_50,
                            (count(distinct case when fuel_quantity > 50.0 and fuel_quantity <=100.0 and fuel_flag=1 then sci_trans_key end))*1.0 /
                            ((case when count(distinct sci_trans_key) = 0 then 1 else 1.00* count(distinct sci_trans_key) end)) as share_fuel_trans_50_100,
                            (count(distinct case when fuel_quantity > 100.0 and fuel_flag=1 then sci_trans_key end))*1.0 /
                            ((case when count(distinct sci_trans_key) = 0 then 1 else 1.00* count(distinct sci_trans_key) end)) as share_fuel_trans_100_plus                                        
                            from df4
                            group by account""")
    df4.show()