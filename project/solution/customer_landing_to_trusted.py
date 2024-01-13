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

# Script generated for node Customer Landing
CustomerLanding_node1705124376547 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity-project3-20240112",
    table_name="customer_landing",
    transformation_ctx="CustomerLanding_node1705124376547",
)

# Script generated for node Filtering
SqlQuery722 = """
select * from myDataSource where shareWithResearchAsOfDate != 0;
"""
Filtering_node1705125187329 = sparkSqlQuery(
    glueContext,
    query=SqlQuery722,
    mapping={"myDataSource": CustomerLanding_node1705124376547},
    transformation_ctx="Filtering_node1705125187329",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1705117701017 = glueContext.getSink(
    path="s3://udacity-project3-20240112/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1705117701017",
)
CustomerTrusted_node1705117701017.setCatalogInfo(
    catalogDatabase="udacity-project3-20240112", catalogTableName="customer_trusted"
)
CustomerTrusted_node1705117701017.setFormat("json")
CustomerTrusted_node1705117701017.writeFrame(Filtering_node1705125187329)
job.commit()
