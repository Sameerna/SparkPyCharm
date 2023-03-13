
from pyspark.sql.functions import *

from pyspark.sql.types import *
from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("Starting spark...")

    spark = SparkSession \
            .builder \
            .appName("fireData")\
            .master("local[1]")\
            .getOrCreate()

    Fire_Schema = StructType([StructField("CalLNumber", IntegerType(), True),
                          StructField( 'UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True),
                          StructField('CallDate', StringType(), True),
                          StructField( 'WatchDate', StringType(), True),
                          StructField( 'CallFinalDisposition', StringType(), True),
                          StructField( 'AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True),
                          StructField( 'City', StringType(), True),
                          StructField( 'Zipcode', IntegerType(), True),
                          StructField( 'Battalion', StringType(), True),
                          StructField('StationArea', StringType(), True),
                          StructField('Box', StringType(), True),
                          StructField('OriginalPriority', StringType(), True),
                          StructField('Priority', StringType(), True),
                          StructField( 'FinalPriority', IntegerType(), True),
                          StructField('ALSUnit', BooleanType(), True),
                          StructField( 'CallTypeGroup', StringType(), True),
                          StructField( 'NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                          StructField('FirePreventionDistrict', StringType(), True),
                          StructField('SupervisorDistrict', StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Delay', FloatType(), True)])

    sf_fire_file = "file:///D://sparkdatset//sf-fire-calls.csv"
    fire_df = spark.read.csv(sf_fire_file,header=True,schema=Fire_Schema)


    few_fire_df = (fire_df
                   .select("IncidentNumber","AvailableDtTm","CallType")
                   .where(col("CallType") != "Medical Incident"))
    # few_fire_df.show(5, False)

    # fewdis_fire_df = (fire_df
    #                .select("CallType")
    #                .where(col("CallType").isNotNull())
    #                   .distinct()
    #                   .show())

    new_fire_df = (fire_df
                      .select("CallType")
                      .where(col("CallType").isNotNull())
                      .distinct()
                      .show())

    new_fire_df = fire_df.withColumnRenamed("Delay","Delayed")
    # rename_fire_df.show(5)
    (new_fire_df.select("Delayed").show(5,False))




