
from pyspark.context import SparkContext
from awsglue.context import GlueContext

SOURCE_PATH = 's3://etl-poc-data-raw/clean-data/'
TARGET_PATH = 's3://etl-poc-data-raw/repart-data/'


def repartition_by(spark, path_source, path_target):
    
    df = spark.read.parquet(path_source)

    df.write \
        .partitionBy("country") \
        .format("parquet") \
        .mode("overwrite") \
        .save(path_target)
    
    
if __name__ == "__main__":

    sc = SparkContext.getOrCreate()

    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    repartition_by(spark, SOURCE_PATH, TARGET_PATH)
