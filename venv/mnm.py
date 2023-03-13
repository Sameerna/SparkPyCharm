import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *


from pyspark.sql.functions import count


if __name__ =="__main__":
    spark = (SparkSession.builder
             .appName("example")
             .getOrCreate())
    schema = "`author` STRING,`title` STRING,`pages` INT"
    data = [["sam","love for DE",200]]
    # data = [["sam", "love for DE", 200]]

    # rdd = spark.sparkContext.parallelize(data)
    authordf = spark.createDataFrame(data,schema)
    authordf.show()
    print(authordf.printSchema())
    # if len(sys.argv) != 2:
    #     print("usage :mnmcount <file>", file=sys.stderr)
    #     sys.exit(-1)

    # spark = (SparkSession
    #          .builder
    #          .appName("pythonMnMCount")
    #          .getOrCreate())
    # schema = StructType([StructField("author",StringType(), False),
    #                      StructField("title",StringType(), False),
    #                     StructField("pages",IntegerType(), False)])




    # mnmfile = "file:///D://sparkdatset//mnm_dataset.csv"
    #
    # mnm_df = (spark.read.format("csv")
    #           .option("header","true")
    #           .option("inferSchema","true")
    #           .load(mnmfile))
    #
    # count_mnm_df = (mnm_df.select("State","Color","count")
    #                 .groupBy("state","Color")
    #                 .agg(count("Count").alias("Total"))
    #                 .orderBy("Total",ascending=False))
    # count_mnm_df.show(n=100,truncate=False)
    # print("Total Rows = %d"%(count_mnm_df.count()))
    #
    # ca_count_mnm_df = (mnm_df.select("State","Color","Count")
    #                    .where(mnm_df.State == "CA")
    #                    .groupBy("State","Color")
    #                    .agg(count("Count").alias("Total"))
    #                    .orderBy("Total", ascending=False)
    #                    )
    # ca_count_mnm_df.show()
    # spark.stop()

