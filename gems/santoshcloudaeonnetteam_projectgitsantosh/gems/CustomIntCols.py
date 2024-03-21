from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *


class CustomIntCols(ComponentSpec):
    name: str = "CustomIntCols"
    category: str = "Custom"

    def optimizeCode(self) -> bool:
        # Return whether code optimization is enabled for this component
        return True

    @dataclass(frozen=True)
    class CustomIntColsProperties(ComponentProperties):
        # properties for the component with default values
        my_property: SString = SString("default value of my property")

    def dialog(self) -> Dialog:
        # Define the UI dialog structure for the component
        return Dialog("CustomIntCols")

    def validate(self, context: WorkflowContext, component: Component[CustomIntColsProperties]) -> List[Diagnostic]:
        # Validate the component's state
        return []

    def onChange(self, context: WorkflowContext, oldState: Component[CustomIntColsProperties], newState: Component[CustomIntColsProperties]) -> Component[
    CustomIntColsProperties]:
        # Handle changes in the component's state and return the new state
        return newState


    class CustomIntColsCode(ComponentCode):
        def __init__(self, newProps):
            self.props: CustomIntCols.CustomIntColsProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            # This method contains logic used to generate the spark code from the given inputs.

            # Identify integer columns
            integer_columns = [col_name for col_name, col_type in in0.dtypes if col_type == "int"]

            # Select only integer columns
            in0 = in0.select(*integer_columns)

            return in0
