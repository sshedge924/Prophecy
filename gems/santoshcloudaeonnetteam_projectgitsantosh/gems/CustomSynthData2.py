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
            self.props: CustomSynthData22.CustomSynthData22Properties = newProps

        def apply(self, spark: SparkSession, in0:DataFrame ) -> DataFrame:
            # This method contains logic used to generate the spark code from the given inputs.
           
            import pandas as pd
            import numpy as np
            import datetime
            #import org.apache.spark.sql.types.{IntegerType,StringType,StructType,StructField}

            from faker import Faker
            import random2
            from random2 import randint
            from faker.providers import DynamicProvider

            # IMPORT SECRETS
            import secrets

            # IMPORT TIMER
            from timeit import default_timer as timer
            from datetime import timedelta
            
            # Create a Spark session
            spark = SparkSession.builder.appName("SyntheticDataGenerator2").getOrCreate()

            #bank_churn_data = in0.toPandas()
            #bank_churn_data = in0.select("*").toPandas()
            
            # bank_churn_data = pd.read_csv("/mnt/ipcontainer/bank_churn_input.csv")

            sample_df = sc.parallelize([
                        (15773898,'Lucchese',	586,	'France',	'Female',23,	2,	0,	2,	0,	1,	160976.75),
                        (15782418,'Nott',	683,	'France',	'Female',46,	2,	0,	1,	1,	0,	72549.27),
                        (15807120,'Knorr',	656,	'France',	'Female',34,	7,	0,	2,	1,	0,	138882.09),
                        (15808905,'ODonnell',	681,	'France',	'Male',	36,	8,	0,	1,	1,	0,	113931.57),
                        (15607314,'Higgins',	752,	'Germany',	'Male',	38,	10,	121263.62,1,	1,	0,	139431),
                        (15672704,'Pearson',	593,	'France',	'Female',22,	9,	0,	2,	0,	0,	51907.72),
                        (15647838,'Onyemere',	682,	'Spain',	'Male',	45,	4,	0,	2,	1,	1,	157878.67),
                        (15775307,'Hargreaves',	539,	'Spain',	'Female',47,	8,	0,	2,	1,	1,	126784.29),
                        (15653937,'Hsueh',	845,	'France',	'Female',47,	3,	111096.91,1,	1,	0,	94978.1)
                                                ]
                        ).toDF(["CustomerID","Surname","CreditScore","Geography","Gender","Age","Tenure","Balance","NumOfProducts","HasCrCard","IsActiveMember","EstimatedSalary"])

            bank_churn_data = sample_df.toPandas()

            geog = bank_churn_data["Geography"].unique()
            gend = bank_churn_data['Gender'].unique()
            prod = bank_churn_data['NumOfProducts'].unique()
            cc   = bank_churn_data['HasCrCard'].unique()
            acti = bank_churn_data['IsActiveMember'].unique()

                        
            #CREATE FILE HEADER FROM THE LIST OF INPUT COLUMNS
            file_hdr = bank_churn_data.columns.tolist()

            #CREATE AN EMPTY DATAFRAME WITH THE HEADER
            df = pd.DataFrame(columns=file_hdr)

            # SET FAKER INPUT LOCALE
            fake = Faker('en_GB')

            def x(a, b, N):
                return np.random.randint(a, b, N)

            def y(a, N):
                return np.random.choice(a, N)

            def sur(N):
                arr = np.array([fake.last_name() for _ in range(N)], dtype=object)
                return arr

            def geo(N):
                arr = np.array([secrets.choice(geog) for _ in range(N)], dtype=object)
                return arr

            def gen(N):
                arr = np.array([secrets.choice(gend) for _ in range(N)], dtype=object)
                return arr

            N = component.properties.limit.value  # THIS IS THE DATAFRAME ROWCOUNT THAT NEEDS TO BE CREATED

            # start = timer()

            id = np.arange(start=1, stop=N+1, step=1)

            # CREATE DATAFRAME WITH COLUMN id AND FILL IT WITH AN THE ARRAY id
            df = pd.DataFrame({'id': id})

            # KEEPING ADDED SYNTHETICALLY FILLED COLUMNS TO THE DATAFRAME
            df['CustomerID']      = np.vectorize(x)(1000000, 9999999,N)
            df['Surname']         = np.vectorize(sur)(N)  #df.apply(lambda x: fake.last_name(), axis=1)
            df['CreditScore']     = np.vectorize(x)(350, 850, N)
            df['Geography']       = np.vectorize(geo)(N)  #df.apply(lambda x: secrets.choice(geog), axis=1)
            df['Gender']          = np.vectorize(gen)(N)  #df.apply(lambda x: secrets.choice(gend), axis=1)
            df['Age']             = np.vectorize(x)(18, 100, N)
            df['Tenure']          = np.vectorize(x)(1, 11, N)
            df['Balance']         = np.vectorize(y)(300000, N)
            df['NumOfProducts']   = np.vectorize(x)(1,4,N)  #df.apply(lambda x: secrets.choice(prod), axis=1)
            df['HasCrCard']       = np.vectorize(y)(2,N)
            df['IsActiveMember']  = np.vectorize(y)(2,N)
            df['EstimatedSalary'] = np.vectorize(x)(10,200000,N)

            df_synthetic = df[["id","CustomerID","Surname","CreditScore","Geography","Gender","Age","Tenure","Balance","NumOfProducts","HasCrCard","IsActiveMember","EstimatedSalary"]]     
            
            
            # end = timer()
            # print(f"Time taken to generate {N} recs: ",timedelta(seconds=end-start))            

            # Return Synthetic DataFrame
            #out0 = df_synthetic
            # Pandas to Spark
            data = [1,15773898,'Lucchese',	586,	'France',	'Female',23,	2,	0,	2,	0,	1,	160976.75]
            columns = ["id","CustomerID","Surname","CreditScore","Geography","Gender","Age","Tenure","Balance","NumOfProducts","HasCrCard","IsActiveMember","EstimatedSalary"]

            columns = StructType(Array(
                            StructField("id",IntegerType,true),
                            StructField("CustomerID",IntegerType,true),
                            StructField("Surname",StringType,true),
                            StructField("CreditScore", IntegerType, true),
                            StructField("Geography", StringType, true),
                            StructField("Gender", StringType, true),
                            StructField("Age",IntegerType,true),
                            StructField("Tenure",IntegerType,true),
                            StructField("Balance",IntegerType,true),
                            StructField("NumOfProducts", IntegerType, true),
                            StructField("HasCrCard", IntegerType, true),
                            StructField("IsActiveMember", IntegerType, true),
                            StructField("EstimatedSalary", IntegerType, true)
                        ))


            in0 = spark.createDataFrame(data=data, schema=columns)

            return in0
