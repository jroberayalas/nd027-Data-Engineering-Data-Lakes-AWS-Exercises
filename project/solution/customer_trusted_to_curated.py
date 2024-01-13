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

# Script generated for node Customer Trusted
CustomerTrusted_node1705128434416 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity-project3-20240112",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1705128434416",
)

# Script generated for node SQL Query
SqlQuery661 = """
select distinct C.* from C JOIN A on C.email = A.user;

"""
SQLQuery_node1705128920541 = sparkSqlQuery(
    glueContext,
    query=SqlQuery661,
    mapping={
        "C": CustomerTrusted_node1705128434416,
        "A": AccelerometerTrusted_node1705128465950,
    },
    transformation_ctx="SQLQuery_node1705128920541",
)

# Script generated for node Customer Curated
CustomerCurated_node1705128693112 = glueContext.getSink(
    path="s3://udacity-project3-20240112/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1705128693112",
)
CustomerCurated_node1705128693112.setCatalogInfo(
    catalogDatabase="udacity-project3-20240112", catalogTableName="customer_curated"
)
CustomerCurated_node1705128693112.setFormat("json")
CustomerCurated_node1705128693112.writeFrame(SQLQuery_node1705128920541)
job.commit()
