"""Serverless datalake monitoring constructs"""
import json
# pylint: disable=unused-variable, too-many-locals
import os
import subprocess as sp

from aws_cdk import (
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_event_source,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_glue as glue,
    aws_sns as sns,
    aws_secretsmanager as secrets, Stack, Duration
)
from constructs import Construct

import config as cf
from common.utils import select_artifacts

MONITOR_LAMBDA_ASSETS = {"zips": "*.zip", "lib_dir": "libs", "monitor_lambda": "handler.py"}


class DataLakeMonitoringStack(Stack):
    """Construct containing resources for Data Lake monitoring"""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        """Create construct"""
        super().__init__(scope, construct_id, **kwargs)

        lambda_timeout_seconds = 180
        path_monitoring_lambda = os.path.join(cf.PATH_SRC, "datalake_monitoring")
        monitor_db = cf.MONITOR_DB
        monitor_table = cf.MONITOR_TABLE

        # Secret for Monitoring
        secrets_kv = {
            f"{cf.SLACK_WEBHOOK_SECRET_NAME}": "to-be-updated"
        }

        monitoring_secret = secrets.Secret(
            self,
            id="monitoring-secret",
            secret_name=cf.MONITOR_SECRET_MANAGER,
            secret_string_beta1=secrets.SecretStringValueBeta1.from_unsafe_plaintext(f"{json.dumps(secrets_kv)}")
        )

        # Create SNS Topic to fork out subscriptions
        self.dl_monitor_sns_topic = sns.Topic(
            self,
            id="dl-monitor-sns",
            display_name="datalake-monitor-sns",
            topic_name=cf.MONITOR_SNS_TOPIC,
        )

        # Lambda layer - aws-wrangler
        sp.call(["make", "bundle"], cwd=path_monitoring_lambda)

        wrangler_layer = lambda_.LayerVersion(
            self,
            id="aws-wrangler",
            code=lambda_.Code.from_asset(
                os.path.join(path_monitoring_lambda, f"libs/{cf.WRANGLER_ASSET}")
            ),
        )

        sp.call(["make", "clean"], cwd=path_monitoring_lambda)

        monitoring_lambda = lambda_.Function(
            self,
            id="datalake-monitoring-lambda",
            handler="handler.handler",
            runtime=lambda_.Runtime.PYTHON_3_10,
            code=lambda_.Code.from_asset(
                path=path_monitoring_lambda,
                exclude=select_artifacts(
                    artifacts=MONITOR_LAMBDA_ASSETS, keep_artifact="monitor_lambda"
                ),
            ),
            function_name="datalake-monitoring-lambda",
            environment={
                "REGION": cf.REGION,
                "SECRET_MGR": cf.MONITOR_SECRET_MANAGER,
                "MONITOR_S3": cf.S3_MONITOR_BUCKET,
                "MONITOR_DATABASE": cf.MONITOR_DB,
                "MONITOR_TABLE": cf.MONITOR_TABLE,
            },
            layers=[wrangler_layer],
            memory_size=128,
            timeout=Duration.seconds(lambda_timeout_seconds),
        )

        monitoring_secret.grant_read(monitoring_lambda)

        # Create an SNS event source for Lambda
        sns_event_source = lambda_event_source.SnsEventSource(self.dl_monitor_sns_topic)

        # Add SNS event source to the Lambda function
        monitoring_lambda.add_event_source(sns_event_source)

        # Glue Events to SNS

        # Create Event Rule to capture Glue Job
        glue_job_rule = events.Rule(
            self,
            id="serverless-event-rule-capture-glue-state",
            description="For any Glue Job or Crawler, capture if it was a failure or success",
            rule_name="glue-monitor-rule",
            enabled=True,
            event_pattern=events.EventPattern(
                detail_type=["Glue Job State Change", "Glue Crawler State Change"],
                detail={
                    "state": ["TIMEOUT", "FAILED", "SUCCEEDED", "STOPPED", "Failed", "Succeeded"]
                },
            ),
            # targets=[targets.SqsQueue(self.central_state_queue)],
            targets=[targets.SnsTopic(self.dl_monitor_sns_topic)],
        )

        # SLACK NOTIFICATION

        # Secret for Monitoring
        # monitor_secret = secrets.Secret.from_secret_name(
        monitor_secret = secrets.Secret.from_secret_name_v2(
            self, id="monitor-stack-secret", secret_name=cf.SLACK_WEBHOOK_SECRET_NAME
        )

        '''
        Monitoring : Database and Table creation
        Create DB, Table
        '''

        # Monitoring Database
        monitor_db = glue.CfnDatabase(
            self,
            id="monitor-db",
            catalog_id=cf.ACCOUNT,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=cf.MONITOR_DB,
                description="Monitoring DB",
                location_uri=f"s3://{cf.S3_MONITOR_BUCKET}/{cf.MONITOR_DB}/",
            ),
        )

        # Monitoring Table
        service_metrics_columns = [
            {"name": column_name, "type": "string", "comment": ""}
            for column_name in [
                "error_message",
                "event_type",
                "exception_details",
                "retry_attempts",
                "service_name",
                "service_request_id",
                "service_type",
                "timestamp",
            ]
        ]

        service_metrics_table = glue.CfnTable.TableInputProperty(
            description="Monitor Table Attributes",
            name=cf.MONITOR_TABLE,
            parameters={
                "classification": "json",
                "has_encrypted_data": "false",
                "projection.enabled": "true",
                "projection.exported_on.type": "date",
                "projection.exported_on.range": "2021-07-01,2021-12-31",
                "projection.exported_on.format": "yyyy-MM-dd",
                "projection.exported_on.interval": "1",
                "projection.exported_on.interval.unit": "DAYS",
            },
            partition_keys=[{"name": "exported_on", "type": "date", "comment": "Day of event"}],
            storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                columns=service_metrics_columns,
                input_format="org.apache.hadoop.mapred.TextInputFormat",
                output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                compressed=True,
                location=f"s3://{cf.S3_MONITOR_BUCKET}/{cf.MONITOR_DB}/{cf.MONITOR_TABLE}/",
                serde_info=glue.CfnTable.SerdeInfoProperty(
                    serialization_library="org.openx.data.jsonserde.JsonSerDe"
                ),
            ),
            table_type="EXTERNAL_TABLE",
        )

        # Create Glue Table
        monitor_table = glue.CfnTable(
            self,
            id="monitor-table",
            catalog_id=cf.ACCOUNT,
            database_name=cf.MONITOR_DB,
            table_input=service_metrics_table,
        )

        monitor_table.add_depends_on(monitor_db)

        # Policy for Lambda to create or replace view. Also update Glue and Athena artifacts
        monitoring_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "athena:UpdateDataCatalog",
                    "athena:GetDataCatalog",
                    "glue:DeleteTable",
                    "glue:CreateTable",
                    "glue:GetDatabases",
                    "glue:GetSchema",
                    "glue:GetTable",
                ],
                resources=[
                    f"arn:aws:glue:{cf.REGION}:{cf.ACCOUNT}:database/{cf.MONITOR_DB}",
                    f"arn:aws:glue:{cf.REGION}:{cf.ACCOUNT}:schema/{cf.MONITOR_DB}",
                    f"arn:aws:glue:{cf.REGION}:{cf.ACCOUNT}:table/{cf.MONITOR_DB}/*",
                    f"arn:aws:glue:{cf.REGION}:{cf.ACCOUNT}:catalog",
                    f"arn:aws:athena:{cf.REGION}:{cf.ACCOUNT}:datacatalog/AwsDataCatalog",
                    f"arn:aws:athena:{cf.REGION}:{cf.ACCOUNT}:workgroup/primary",
                ],
            )
        )

        # S3 Access
        monitoring_lambda.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:PutObject*",
                    "s3:Abort*",
                    "s3:DeleteObject*",
                    "s3:GetObject*",
                    "s3:GetBucket*",
                    "s3:List*",
                    "s3:Head*"
                ],
                resources=[
                    f"arn:aws:s3:::{cf.S3_MONITOR_BUCKET}",
                    f"arn:aws:s3:::{cf.S3_MONITOR_BUCKET}/{cf.MONITOR_DB}",
                    f"arn:aws:s3:::{cf.S3_MONITOR_BUCKET}/{cf.MONITOR_DB}/*"
                ],
            )
        )

        # Database and Tables Access
        monitoring_lambda.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "glue:CreateTable",
                    "glue:CreatePartition",
                    "glue:UpdatePartition",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:DeletePartition",
                    "glue:BatchCreatePartition",
                    "glue:Get*",
                    "glue:BatchGet*"
                ],
                resources=[
                    f"arn:aws:glue:{cf.REGION}:{cf.ACCOUNT}:catalog",
                    f"arn:aws:glue:{cf.REGION}:{cf.ACCOUNT}:database/{cf.MONITOR_DB}",
                    f"arn:aws:glue:{cf.REGION}:{cf.ACCOUNT}:table/{cf.MONITOR_DB}/{cf.MONITOR_TABLE}",
                    f"arn:aws:glue:{cf.REGION}:{cf.ACCOUNT}:schema/{cf.MONITOR_DB}",
                    f"arn:aws:glue:{cf.REGION}:{cf.ACCOUNT}:catalog",
                    f"arn:aws:athena:{cf.REGION}:{cf.ACCOUNT}:datacatalog/AwsDataCatalog",
                    f"arn:aws:athena:{cf.REGION}:{cf.ACCOUNT}:workgroup/primary",
                ],
            )
        )
