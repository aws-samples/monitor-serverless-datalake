"""Serverless datalake monitoring constructs"""
# pylint: disable=unused-variable, too-many-locals
import os
from aws_cdk import (
    aws_glue as glue,
    aws_iam as iam,
    aws_s3_assets as s3assets, Stack,
)
from constructs import Construct

import config as cf

GLUE_LOG_POLICY = iam.PolicyStatement(
    actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
    effect=iam.Effect.ALLOW,
    resources=["arn:aws:logs:*:*:/aws-glue/*"],
    sid="LogsAccess",
)

S3_LANDING_BUCKET_RESOURCES = [
    f"arn:aws:s3:::{cf.S3_LANDING_BUCKET}",
]

S3_LEGISLATOR_RESOURCES = [
    f"arn:aws:s3:::{cf.S3_LANDING_BUCKET}/{cf.LEGISLATORS_PATH}/*",
    f"arn:aws:s3:::{cf.S3_LANDING_BUCKET}/{cf.LEGISLATORS_PATH}_$folder$",
]

S3_PROCESSED_BUCKET_RESOURCES = [
    f"arn:aws:s3:::{cf.S3_PROCESSED_BUCKET}",
]

S3_LEGISLATOR_PROCESSED = [
    f"arn:aws:s3:::{cf.S3_PROCESSED_BUCKET}/{cf.PROCESSED_PATH}/*",
    f"arn:aws:s3:::{cf.S3_PROCESSED_BUCKET}/{cf.PROCESSED_PATH}_$folder$",
]

S3_LANDING_READ_POLICY = iam.PolicyStatement(
    actions=["s3:GetObject*", "s3:GetBucket*", "s3:List*", "s3:Head*"],
    effect=iam.Effect.ALLOW,
    resources=S3_LANDING_BUCKET_RESOURCES + S3_LEGISLATOR_RESOURCES,
)

S3_LANDING_NO_READ_POLICY = iam.PolicyStatement(
    actions=["s3:GetObject*", "s3:GetBucket*", "s3:List*", "s3:Head*"],
    effect=iam.Effect.DENY,
    resources=S3_LANDING_BUCKET_RESOURCES,
)

S3_PROCESSED_WRITE_POLICY = iam.PolicyStatement(
    actions=[
        "s3:PutObject*",
        "s3:Abort*",
        "s3:DeleteObject*",
        "s3:GetObject*",
        "s3:GetBucket*",
        "s3:List*",
        "s3:Head*",
    ],
    effect=iam.Effect.ALLOW,
    resources=S3_PROCESSED_BUCKET_RESOURCES + S3_LEGISLATOR_PROCESSED,
)

GLUE_DB_POLICY = iam.PolicyStatement(
    actions=[
        "glue:CreateTable",
        "glue:CreatePartition",
        "glue:UpdatePartition",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:DeletePartition",
        "glue:BatchCreatePartition",
        "glue:Get*",
        "glue:BatchGet*",
        "athena:StartQueryExecution",
        "athena:StopQueryExecution",
        "athena:GetDataCatalog",
        "athena:GetQueryResults",
        "athena:GetQueryExecution",
    ],
    effect=iam.Effect.ALLOW,
    resources=[
        f"arn:aws:glue:{cf.REGION}:{cf.ACCOUNT}:catalog",
        f"arn:aws:glue:{cf.REGION}:{cf.ACCOUNT}:database/{cf.LEGISLATOR_DB}",
        f"arn:aws:glue:{cf.REGION}:{cf.ACCOUNT}:table/{cf.LEGISLATOR_DB}/*",
        f"arn:aws:athena:{cf.REGION}:{cf.ACCOUNT}:datacatalog/AwsDataCatalog",
    ],
)


class DataLakeGlueIngestionStack(Stack):
    """Construct containing resources for Data Lake Glue Crawler & Glue Job for ingestion"""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        """Create construct"""
        super().__init__(scope, construct_id, **kwargs)

        # Glue Database

        glue_legislators_db = glue.CfnDatabase(
            self,
            id=f"{cf.LEGISLATOR_DB}-db",
            catalog_id=cf.ACCOUNT,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=cf.LEGISLATOR_DB,
                description="Legislator Database",
                location_uri=f"s3://{cf.S3_LANDING_BUCKET}/{cf.LEGISLATOR_DB}",
            ),
        )

        # Legislator Glue Crawler
        legislators_glue_crawler_name = "glue-crawler-success"
        legislators_glue_crawler_role_name = f"{legislators_glue_crawler_name}_glue_crawler_role"

        legislators_glue_crawler_role = iam.Role(
            self,
            legislators_glue_crawler_role_name.lower(),
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            role_name=legislators_glue_crawler_role_name,
        )

        legislators_glue_crawler_role.add_to_policy(GLUE_LOG_POLICY)
        legislators_glue_crawler_role.add_to_policy(S3_LANDING_READ_POLICY)
        legislators_glue_crawler_role.add_to_policy(GLUE_DB_POLICY)

        legislators_glue_crawler = glue.CfnCrawler(
            self,
            id=legislators_glue_crawler_name,
            role=legislators_glue_crawler_role.role_arn,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{cf.S3_LANDING_BUCKET}/{cf.LEGISLATORS_PATH}",
                        exclusions=None,
                    )
                ]
            ),
            # schedule=glue.CfnCrawler.ScheduleProperty(schedule_expression=schedule),
            database_name=cf.LEGISLATOR_DB,
            description=f"Glue Crawler for Legislators dataset created in {cf.LEGISLATOR_DB} database",
            name=legislators_glue_crawler_name,
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior="DEPRECATE_IN_DATABASE", update_behavior="UPDATE_IN_DATABASE"
            ),
        )

        # Glue crawler that will fail due to permission
        legislators_fail_glue_crawler_name = "glue-crawler-fail"
        legislators_fail_glue_crawler_role_name = (
            f"{legislators_fail_glue_crawler_name}_glue_crawler_role"
        )

        legislators_fail_glue_crawler_role = iam.Role(
            self,
            legislators_fail_glue_crawler_role_name.lower(),
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            role_name=legislators_fail_glue_crawler_role_name,
        )

        legislators_fail_glue_crawler_role.add_to_policy(GLUE_LOG_POLICY)
        legislators_fail_glue_crawler_role.add_to_policy(S3_LANDING_NO_READ_POLICY)
        legislators_fail_glue_crawler_role.add_to_policy(GLUE_DB_POLICY)

        legislators_fail_glue_crawler = glue.CfnCrawler(
            self,
            id=f"{legislators_fail_glue_crawler_name}_fail",
            role=legislators_fail_glue_crawler_role.role_arn,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{cf.S3_LANDING_BUCKET}/{cf.LEGISLATORS_PATH}",
                        exclusions=None,
                    )
                ]
            ),
            database_name=f"{cf.LEGISLATOR_DB}-database",
            description=f"Glue Crawler for Legislators dataset created in {cf.LEGISLATOR_DB} database",
            name=legislators_fail_glue_crawler_name,
        )

        # Glue Job - Success
        legislators_process_glue_job_name = "glue-job-success"
        legislators_process_glue_job_role_name = (
            f"{legislators_process_glue_job_name}-glue-job-role"
        )
        legislators_process_job_role = iam.Role(
            self,
            legislators_process_glue_job_role_name.lower(),
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            role_name=legislators_process_glue_job_role_name,
        )

        legislators_process_job_role.add_to_policy(GLUE_LOG_POLICY)
        legislators_process_job_role.add_to_policy(S3_LANDING_READ_POLICY)
        legislators_process_job_role.add_to_policy(S3_PROCESSED_WRITE_POLICY)
        legislators_process_job_role.add_to_policy(GLUE_DB_POLICY)
        legislators_process_glue_job_script_path = os.path.join(
            cf.SRC_DIR_PATH,
            "sample_compute/glue",
            "glue_job_success.py",
        )
        legislators_process_glue_job_script = s3assets.Asset(
            self,
            id=f"{legislators_process_glue_job_name}_script",
            path=legislators_process_glue_job_script_path,
        )

        legislators_process_glue_job_script.grant_read(legislators_process_job_role)

        legislators_process_glue_job = glue.CfnJob(
            self,
            id=f"{legislators_process_glue_job_name}_id",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=legislators_process_glue_job_script.s3_object_url,
            ),
            role=legislators_process_job_role.role_arn,
            description="Glue Job for processing US Legislators data",
            glue_version=cf.GLUE_VERSION,
            name=legislators_process_glue_job_name,
            default_arguments={
                "--landing_s3": cf.S3_LANDING_BUCKET,
                "--processed_s3": cf.S3_PROCESSED_BUCKET,
            },
            number_of_workers=2,
            timeout=10,
            worker_type="G.1X",
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=15
            ),
        )

        # Glue Job - Failure

        sample_failure_glue_job_name = "glue-job-fail"
        sample_failure_glue_job_role_name = (
            f"{sample_failure_glue_job_name}-glue-job_role"
        )
        sample_failure_glue_job_role = iam.Role(
            self,
            sample_failure_glue_job_role_name.lower(),
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            role_name=sample_failure_glue_job_role_name,
        )

        sample_failure_glue_job_role.add_to_policy(GLUE_LOG_POLICY)
        sample_failure_glue_job_script_path = os.path.join(
            cf.SRC_DIR_PATH,
            "sample_compute/glue",
            "glue_job_fail.py",
        )
        sample_failure_glue_job_script = s3assets.Asset(
            self,
            id=f"{sample_failure_glue_job_name}_script",
            path=sample_failure_glue_job_script_path,
        )

        sample_failure_glue_job_script.grant_read(sample_failure_glue_job_role)

        sample_failure_glue_job = glue.CfnJob(
            self,
            id=f"{sample_failure_glue_job_name}_id",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=sample_failure_glue_job_script.s3_object_url,
            ),
            role=sample_failure_glue_job_role.role_arn,
            description="Glue Job for processing US Legislators data",
            glue_version=cf.GLUE_VERSION,
            name=sample_failure_glue_job_name,
            number_of_workers=2,
            timeout=10,
            worker_type="G.1X",
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=15
            ),
        )
