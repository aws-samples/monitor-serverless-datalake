"""CDK App - Serverless Data Lake : Monitoring"""
# pylint: disable=unused-variable
from aws_cdk import core

from base_stacks.environment_stack import DataLakeEnvironmentStack
from base_stacks.monitoring_stack import DataLakeMonitoringStack

import config as cf

app = core.App()

# Base / Common Stacks
env_stack = DataLakeEnvironmentStack(app, construct_id="env-stack", env=cf.CDK_ENV)

# Monitoring Stack
monitor_stack = DataLakeMonitoringStack(app, construct_id="monitor-stack", env=cf.CDK_ENV)
monitor_stack.add_dependency(env_stack)
app.synth(skip_validation=False)
