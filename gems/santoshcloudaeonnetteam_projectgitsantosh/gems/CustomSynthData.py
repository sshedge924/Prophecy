from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *

from faker import Faker

class CustomSynthData(ComponentSpec):
    name: str = "CustomSynthData"
    category: str = "Custom"

    def optimizeCode(self) -> bool:
        # Return whether code optimization is enabled for this component
        return True

    @dataclass(frozen=True)
    class CustomSynthDataProperties(ComponentProperties):
        # properties for the component with default values
        my_property: SString = SString("default value of my property")

    def dialog(self) -> Dialog:
        # Define the UI dialog structure for the component
        return Dialog("CustomSynthData")

    def validate(self, context: WorkflowContext, component: Component[CustomSynthDataProperties]) -> List[Diagnostic]:
        # Validate the component's state
        return []

    def onChange(self, context: WorkflowContext, oldState: Component[CustomSynthDataProperties], newState: Component[CustomSynthDataProperties]) -> Component[
    CustomSynthDataProperties]:
        # Handle changes in the component's state and return the new state
        return newState


    class CustomSynthDataCode(ComponentCode):
        def __init__(self, newProps):
            self.props: CustomSynthData.CustomSynthDataProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            # This method contains logic used to generate the spark code from the given inputs.


            # Create a Spark session
            spark = SparkSession.builder.appName("SyntheticDataGenerator").getOrCreate()

            # Create a Faker instance
            fake = Faker()

            # Generate synthetic data
            num_rows = 1000
            data = [(fake.name(), fake.address(), fake.random_int(min=1, max = num_rows)) for _ in range(num_rows)]
            columns = ["name", "address", "age"]
            df_synthetic = spark.createDataFrame(data, columns)

            # Return Synthetic DataFrame
            in0 = df_synthetic

            return in0
