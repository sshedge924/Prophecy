from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_santosh_git.config.ConfigStore import *
from pl_santosh_git.udfs.UDFs import *

def ds_tgt_s01(spark: SparkSession, in0: DataFrame):
    in0.write.format("avro").save("dbfs:/mnt/output_data/avro_output")
