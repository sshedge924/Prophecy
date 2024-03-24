from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *

class CustomSynthData2(ComponentSpec):
    name: str = "CustomSynthData2"
    category: str = "Custom"

    def optimizeCode(self) -> bool:
        # Return whether code optimization is enabled for this component
        return True

    @dataclass(frozen=True)
    class CustomSynthData2Properties(ComponentProperties):
        limit: SInt = SInt("10")

    def dialog(self) -> Dialog:
        # Define the UI dialog structure for the component
        return Dialog("CustomSynthData2").addElement(
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(PortSchemaTabs().importSchema(), "2fr")
                .addColumn(
                ExpressionBox("Limit")
                    .bindPlaceholder("5")
                    .bindProperty("limit")
                    .withFrontEndLanguage(),
                "5fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component[CustomSynthData2Properties]) -> List[Diagnostic]:
        diagnostics = []
        limitDiagMsg = "Limit has to be an integer between [0, (2**31)-1]"
        if component.properties.limit.diagnosticMessages is not None and len(component.properties.limit.diagnosticMessages) > 0:
            for message in component.properties.limit.diagnosticMessages:
                diagnostics.append(Diagnostic("properties.limit", message, SeverityLevelEnum.Error))
        else:
            resolved = component.properties.limit.value
            if resolved <= 0:
                diagnostics.append(Diagnostic("properties.limit", limitDiagMsg, SeverityLevelEnum.Error))
            else:
                pass
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[CustomSynthData2Properties], newState: Component[CustomSynthData2Properties]) -> Component[
        CustomSynthData2Properties]:
        return newState


    class CustomSynthData2Code(ComponentCode):
        def __init__(self, newProps):
            self.props: CustomSynthData2.CustomSynthData2Properties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            # This method contains logic used to generate the spark code from the given inputs.
           
            from pandas import pandas
            import numpy as np

            
            # Create a Spark session
            spark = SparkSession.builder.appName("SyntheticDataGenerator2").getOrCreate()

            df = in0.toPandas()

            #num_cols = df._get_numeric_data().columns

            #df.select_dtypes(include=['float','integer']) # integer  object

            n = self.props.limit.value

            synthetic_data=[]

            # Generate synthetic data
            for i in range(n):
                synthetic_data.append(df.apply(lambda x: x.sample(n=1),axis=0).sum(axis=0))
           
            df_synthetic = spark.createDataFrame(synthetic_data)

            # Return Synthetic DataFrame
            out0 = df_synthetic

            return out0
