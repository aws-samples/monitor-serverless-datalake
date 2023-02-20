"""CDK App - Serverless Data Lake : Monitoring"""
# pylint: disable=unused-variable
from aws_cdk import App

from base_stacks.environment_stack import DataLakeEnvironmentStack
from base_stacks.monitoring_stack import DataLakeMonitoringStack
from ingestion_stack.lambda_stack import DataLakeLambdaIngestionStack
from ingestion_stack.glue_stack import DataLakeGlueIngestionStack

import config as cf

app = App()

# Base / Common Stacks
env_stack = DataLakeEnvironmentStack(app, construct_id="env-stack", env=cf.CDK_ENV)

# Monitoring Stack
monitor_stack = DataLakeMonitoringStack(app, construct_id="monitor-stack", env=cf.CDK_ENV)
monitor_stack.add_dependency(env_stack)

# Ingestion Stack : Lambda
lambda_ingestion_stack = DataLakeLambdaIngestionStack(
    app,
    construct_id="ingestion-lambda-stack",
    monitor_sns=monitor_stack.dl_monitor_sns_topic,
    env=cf.CDK_ENV,
)
lambda_ingestion_stack.add_dependency(monitor_stack)

# Ingestion Stack : Glue Crawler and Glue Job

glue_ingestion_stack = DataLakeGlueIngestionStack(
    app,
    construct_id="ingestion-glue-stack",
    env=cf.CDK_ENV,
)

glue_ingestion_stack.add_dependency(monitor_stack)

app.synth(skip_validation=False)
