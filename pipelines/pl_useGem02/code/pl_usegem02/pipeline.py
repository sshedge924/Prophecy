from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pl_usegem02.config.ConfigStore import *
from pl_usegem02.udfs.UDFs import *
from prophecy.utils import *
from pl_usegem02.graph import *

def pipeline(spark: SparkSession) -> None:
    df_ds_src_input = ds_src_input(spark)
    df_CustomSynthData_1 = CustomSynthData_1(spark, df_ds_src_input)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pl_useGem02")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/pl_useGem02", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/pl_useGem02")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
