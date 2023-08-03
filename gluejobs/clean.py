
from pyspark.context import SparkContext
from awsglue.context import GlueContext

import pyspark.sql.types as T
import pyspark.sql.functions as F

import logging

# For input arguments
import sys
from awsglue.utils import getResolvedOptions

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

    logging.error("Correcting column types")
    for cname in df.columns:

        if cname in ["units_sold"]:
            df = df.withColumn(cname, F.col(cname).cast(T.IntegerType()))

        if cname in ["unit_price", "unit_cost", "total_revenue", "total_cost", "total_profit"]:
            df = df.withColumn(cname, F.col(cname).cast(T.DecimalType(10,2)))

        if cname in ["order_date", "ship_date"]:
            df = df.withColumn(cname, 
                               F.coalesce(
                                F.to_date(F.col(cname), 'M/d/yyyy'), 
                                F.to_date(F.col(cname), 'MM/dd/yyyy')))

    logging.error("Droping duplicates")
    df = df.dropDuplicates()

    logging.error("Saving to s3")
    df.coalesce(1)\
        .write\
        .format("parquet")\
        .mode("overwrite")\
        .save(path_target)
    
    return
    
    
if __name__ == "__main__":

    args = getResolvedOptions(sys.argv, ['raw_object_name', 
                                         'source_bucket', 
                                         'target_bucket'])
    
    path_source = f"s3://{args['source_bucket']}/{args['raw_object_name']}"
    path_target = f"s3://{args['target_bucket']}/{args['raw_object_name'].split('.')[0]}" # Remove filename extension

    sc = SparkContext.getOrCreate()

    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    convert_to_parquet(spark, path_source, path_target)
