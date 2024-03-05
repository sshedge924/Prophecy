from prophecy.cb.server.base.MetaComponentBuilderBase import *
from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *
from prophecy.config import ConfigBase


class custGem001(MetaComponentSpec):
    name: str = "custGem001"
    category: str = "CustomSubgraph"

    def optimizeCode(self) -> bool:
        # Return whether code optimization is enabled for this component
        return True

    @dataclass(frozen=True)
    class custGem001Properties(MetaComponentProperties):
        # properties for the component with default values
        my_property: SString = SString("default value of my property")

    def dialog(self) -> Dialog:
        # Define the UI dialog structure for the component
        return (Dialog("custGem001", footer=SubgraphDialogFooter())
            .addElement(
                ColumnsLayout(gap="1rem", height="100%")
                .addColumn(PortSchemaTabs().importSchema(), "1fr")
                .addColumn(
                    Tabs()
                    .addTabPane(
                        TabPane("Settings", "Settings")
                        .addElement(
                            # Add dialog specifications for settings tab
                            element = "Dialog Box"
                        )
                    )
                    .addTabPane(
                        TabPane("Configuration", "Configuration")
                        .addElement(
                            SubgraphConfigurationTabs()
                        )
                    ),
                    "3fr"
                )
            )
         )

    def validate(self, context: WorkflowContext, component: MetaComponent[custGem001Properties]) -> List[Diagnostic]:
        # Validate the component's state
        return []

    def onChange(self, context: WorkflowContext, oldState: MetaComponent[custGem001Properties], newState: MetaComponent[custGem001Properties]) -> MetaComponent[
    custGem001Properties]:
        # Handle changes in the component's state and return the new state
        return newState


    class custGem001Code(MetaComponentCode):
        def __init__(self, newProps, config):
            self.props: custGem001.custGem001Properties = newProps
            self.config: ConfigBase = config

        def apply(self, spark: SparkSession, in0: DataFrame, *inDFs: DataFrame) -> (List[DataFrame]):
            # This method contains logic used to generate the spark code from the given inputs.
            return in0
