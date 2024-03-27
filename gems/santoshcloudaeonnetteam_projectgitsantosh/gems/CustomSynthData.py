from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *



class CustomSynthData(ComponentSpec):
    name: str = "CustomSynthData"
    category: str = "Custom"

    def optimizeCode(self) -> bool:
        # Return whether code optimization is enabled for this component
        return True

    @dataclass(frozen=True)
    class CustomSynthDataProperties(ComponentProperties):
        limit: SInt = SInt("10")

    def dialog(self) -> Dialog:
        # Define the UI dialog structure for the component
        return Dialog("CustomSynthData").addElement(
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

    def validate(self, context: WorkflowContext, component: Component[CustomSynthDataProperties]) -> List[Diagnostic]:
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

    def onChange(self, context: WorkflowContext, oldState: Component[CustomSynthDataProperties], newState: Component[CustomSynthDataProperties]) -> Component[
        CustomSynthDataProperties]:
        return newState


    class CustomSynthDataCode(ComponentCode):
        def __init__(self, newProps):
            self.props: CustomSynthData.CustomSynthDataProperties = newProps

        def apply(self, spark: SparkSession,in0: DataFrame ) -> DataFrame:
            # This method contains logic used to generate the spark code from the given inputs.
            from faker import Faker
            
            # Create a Spark session
            spark = SparkSession.builder.appName("SyntheticDataGenerator").getOrCreate()

            # Create a Faker instance
            fake = Faker()

            # Generate synthetic data
            num_rows = self.props.limit.value
            data = [(fake.name(), fake.address(), fake.random_int(min=1, max = num_rows)) for _ in range(num_rows)]
            columns = ["name", "address", "age"]
            df_synthetic = spark.createDataFrame(data, columns)

            # Return Synthetic DataFrame
            in0 = df_synthetic

            return in0
