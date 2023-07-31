
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import pyspark.sql.types as T
import pyspark.sql.functions as F

import logging

RAW_PATH = 's3://etl-poc-data-raw/2m_Sales_Records.csv'
PROCESSING_PATH = 's3://et-poc-data-raw/clean-data/'

# Schema enforcing 
# Column renaming
# Dropping duplicates

def convert_to_parquet(path_source, path_target):
    
    df = spark.read.format("csv")\
        .option("sep", ",")\
        .option("header", True)\
        .load(path_source)

    logging.error(df.printSchema())

    for cname in df.columns:
        df = df.withColumnRenamed(cname, cname.replace(' ', '_').lower())

    
    
if __name__ == "__main__":

    sc = SparkContext.getOrCreate()

    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)

    convert_to_parquet(RAW_PATH, PROCESSING_PATH) 