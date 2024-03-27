from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_usegem02.config.ConfigStore import *
from pl_usegem02.udfs.UDFs import *

def ds_src_input(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("ProductSubcategoryKey", IntegerType(), True), StructField("ProductSubcategoryAlternateKey", IntegerType(), True), StructField("EnglishProductSubcategoryName", StringType(), True), StructField("SpanishProductSubcategoryName", StringType(), True), StructField("FrenchProductSubcategoryName", StringType(), True), StructField("ProductCategoryKey", IntegerType(), True)
        ])
        )\
        .option("header", True)\
        .option("inferSchema", True)\
        .option("sep", ",")\
        .csv("dbfs:/mnt/ipcontainer/DimProductSubCategory.csv")
