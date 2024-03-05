from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_santosh_git.config.ConfigStore import *
from pl_santosh_git.udfs.UDFs import *

def ds_src_forgit(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("book_id", StringType(), True), StructField("title", StringType(), True), StructField("author", StringType(), True), StructField("category", StringType(), True), StructField("price", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ";")\
        .csv("dbfs:/mnt/demo-datasets/bookstore/books-csv/export_001.csv")
