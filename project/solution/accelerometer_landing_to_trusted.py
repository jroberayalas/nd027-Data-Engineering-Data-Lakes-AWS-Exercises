import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1705127434800 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity-project3-20240112",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1705127434800",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1705127439233 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity-project3-20240112",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1705127439233",
)

# Script generated for node Join
Join_node1705127510504 = Join.apply(
    frame1=CustomerTrusted_node1705127439233,
    frame2=AccelerometerLanding_node1705127434800,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1705127510504",
)

# Script generated for node Drop Fields
DropFields_node1705127582740 = DropFields.apply(
    frame=Join_node1705127510504,
    paths=[
        "phone",
        "lastUpdateDate",
        "email",
        "shareWithFriendsAsOfDate",
        "customerName",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "birthDay",
        "serialNumber",
    ],
    transformation_ctx="DropFields_node1705127582740",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1705127769916 = glueContext.getSink(
    path="s3://udacity-project3-20240112/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1705127769916",
)
AccelerometerTrusted_node1705127769916.setCatalogInfo(
    catalogDatabase="udacity-project3-20240112",
    catalogTableName="accelerometer_trusted",
)
AccelerometerTrusted_node1705127769916.setFormat("json")
AccelerometerTrusted_node1705127769916.writeFrame(DropFields_node1705127582740)
job.commit()
