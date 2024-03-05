from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pl_santosh_git.config.ConfigStore import *
from pl_santosh_git.udfs.UDFs import *
from prophecy.utils import *
from pl_santosh_git.graph import *

def pipeline(spark: SparkSession) -> None:
    df_ds_src_forgit = ds_src_forgit(spark)
    ds_tgt_s01(spark, df_ds_src_forgit)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pl_santosh_git")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/pl_santosh_git", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/pl_santosh_git")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
