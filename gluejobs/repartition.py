
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# For input arguments
import sys
from awsglue.utils import getResolvedOptions


def repartition_by(spark, path_source, path_target):
    
    df = spark.read.parquet(path_source)

    df.write \
        .partitionBy("country") \
        .format("parquet") \
        .mode("overwrite") \
        .save(path_target)
    
    
if __name__ == "__main__":

    args = getResolvedOptions(sys.argv, ['raw_object_name', 
                                         'source_bucket', 
                                         'target_bucket'])
    
    object_name = args['raw_object_name'].split('.')[0]

    path_source = f"s3://{args['source_bucket']}/{object_name}"
    path_target = f"s3://{args['target_bucket']}/{object_name}"

    sc = SparkContext.getOrCreate()

    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    repartition_by(spark, path_source, path_target)
