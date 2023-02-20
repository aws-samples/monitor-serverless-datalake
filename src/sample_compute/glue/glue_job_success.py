from awsglue.transforms import Join
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import boto3
import sys
from awsglue.utils import getResolvedOptions

glueContext = GlueContext(SparkContext.getOrCreate())

args = getResolvedOptions(sys.argv, ["landing_s3", "processed_s3"])
landing_s3 = args["landing_s3"]
processed_s3 = args["processed_s3"]

# catalog: database and table names
db_name = "legislators"
tbl_persons = "persons_json"
tbl_membership = "memberships_json"
tbl_organization = "organizations_json"

s3_output_prefix = "us-legislators/output-dir"

base_s3_output_path = f"s3://{processed_s3}/{s3_output_prefix}"

# Clean-up previous results
s3 = boto3.resource("s3")
bucket = s3.Bucket(processed_s3)
bucket.objects.filter(Prefix=f"{s3_output_prefix}/").delete()
bucket.put_object(Bucket=processed_s3, Body="", Key=f"{s3_output_prefix}/")

# """
output_history_dir = f"{base_s3_output_path}/legislator_history"

# Create dynamic frames from the source tables
persons = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name=tbl_persons)
memberships = glueContext.create_dynamic_frame.from_catalog(
    database=db_name, table_name=tbl_membership
)
orgs = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name=tbl_organization)


# Keep the fields we need and rename some.
orgs = (
    orgs.drop_fields(["other_names", "identifiers"])
    .rename_field("id", "org_id")
    .rename_field("name", "org_name")
)

# Join the frames to create history
l_history = Join.apply(
    orgs, Join.apply(persons, memberships, "id", "person_id"), "org_id", "organization_id"
).drop_fields(["person_id", "org_id"])

# ---- Write out the history ----

# Write out the dynamic frame into parquet in "legislator_history" directory
print("Writing to /legislator_history ...")
glueContext.write_dynamic_frame.from_options(
    frame=l_history,
    connection_type="s3",
    connection_options={"path": output_history_dir},
    format="parquet",
)
