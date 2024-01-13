import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1705128465950 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity-project3-20240112",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1705128465950",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1705128434416 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity-project3-20240112",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1705128434416",
)

# Script generated for node SQL Query
SqlQuery813 = """
select * from S INNER JOIN A on S.sensorreadingtime = A.timestamp;
"""
SQLQuery_node1705128920541 = sparkSqlQuery(
    glueContext,
    query=SqlQuery813,
    mapping={
        "S": StepTrainerTrusted_node1705128434416,
        "A": AccelerometerTrusted_node1705128465950,
    },
    transformation_ctx="SQLQuery_node1705128920541",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1705128693112 = glueContext.getSink(
    path="s3://udacity-project3-20240112/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node1705128693112",
)
MachineLearningCurated_node1705128693112.setCatalogInfo(
    catalogDatabase="udacity-project3-20240112",
    catalogTableName="machine_learning_curated",
)
MachineLearningCurated_node1705128693112.setFormat("json")
MachineLearningCurated_node1705128693112.writeFrame(SQLQuery_node1705128920541)
job.commit()
