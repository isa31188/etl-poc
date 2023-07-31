
from pyspark.context import SparkContext
from awsglue.context import GlueContext

import pyspark.sql.types as T
import pyspark.sql.functions as F

import logging

RAW_PATH = 's3://etl-poc-data-raw/2m-Sales-Records.csv'
PROCESSING_PATH = 's3://etl-poc-data-raw/clean-data/'

# Schema enforcing
# Column renaming
# Dropping duplicates

def convert_to_parquet(spark, path_source, path_target):
    
    df = spark.read.format("csv")\
        .option("sep", ",")\
        .option("header", True)\
        .load(path_source)

    logging.error(df.printSchema())

    logging.error("Renaming columns")
    for cname in df.columns:
        df = df.withColumnRenamed(cname, cname.replace(' ', '_').lower())

    logging.error("Changing date column types")
    for cname in df.columns:
        if cname in ["order_date", "ship_date"]:
            df = df.withColumn(cname, F.to_date(F.col(cname)))

    logging.error("Droping duplicates")
    df = df.dropDuplicates()

    logging.error("Saving to s3")
    df.write\
        .format("parquet")\
        .mode("overwrite")\
        .save(path_target)
    
    return
    
    
if __name__ == "__main__":

    sc = SparkContext.getOrCreate()

    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    convert_to_parquet(spark, RAW_PATH, PROCESSING_PATH)
