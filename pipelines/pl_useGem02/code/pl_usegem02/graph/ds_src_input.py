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
            StructField("id", IntegerType(), True), StructField("CustomerId", IntegerType(), True), StructField("Surname", StringType(), True), StructField("CreditScore", IntegerType(), True), StructField("Geography", StringType(), True), StructField("Gender", StringType(), True), StructField("Age", IntegerType(), True), StructField("Tenure", IntegerType(), True), StructField("Balance", DoubleType(), True), StructField("NumOfProducts", IntegerType(), True), StructField("HasCrCard", IntegerType(), True), StructField("IsActiveMember", IntegerType(), True), StructField("EstimatedSalary", DoubleType(), True)
        ])
        )\
        .option("header", True)\
        .option("inferSchema", True)\
        .option("sep", ",")\
        .csv("dbfs:/mnt/ipcontainer/bank_churn_input.csv")
