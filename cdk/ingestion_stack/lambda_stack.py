"""Serverless datalake monitoring constructs"""
# pylint: disable=unused-variable, too-many-locals
import os

from aws_cdk import (
    aws_lambda as lambda_,
    aws_sns as sns,
    aws_lambda_destinations as lambda_dest,
    aws_sns_subscriptions as subscriptions,
    aws_events as events,
    aws_events_targets as targets, Stack, Duration,
)
from constructs import Construct

import config as cf

from common.utils import select_artifacts

SAMPLE_LAMBDA_ASSETS = {
    "zips": "*.zip",
    "lib_dir": "libs",
    "lambda_fail": "lambda_fail.py",
    "lambda_success": "lambda_success.py",
}


class DataLakeLambdaIngestionStack(Stack):
    """Construct containing resources for Data Lake Lambda for ingestion"""

    def __init__(
            self, scope: Construct, construct_id: str, monitor_sns: sns.ITopic, **kwargs
    ) -> None:
        """Create construct"""
        super().__init__(scope, construct_id, **kwargs)

        lambda_timeout_seconds = 180
        ingestion_lambda = os.path.join(cf.PATH_SRC, "sample_compute/lambda")

        lambda_names = ["lambda_fail", "lambda_success"]

        for lambda_name in lambda_names:
            lambda_function = lambda_.Function(
                self,
                id=lambda_name,
                handler=f"{lambda_name}.handler",
                runtime=lambda_.Runtime.PYTHON_3_8,
                code=lambda_.Code.from_asset(
                    path=ingestion_lambda,
                    exclude=select_artifacts(
                        artifacts=SAMPLE_LAMBDA_ASSETS, keep_artifact=lambda_name
                    ),
                ),
                function_name=lambda_name.replace("_", "-"),
                memory_size=128,
                timeout=Duration.seconds(lambda_timeout_seconds),
                on_success=lambda_dest.SnsDestination(monitor_sns),
                on_failure=lambda_dest.SnsDestination(monitor_sns),
            )

            monitor_sns.grant_publish(lambda_function)

            events.Rule(
                self,
                id=f"event-rule-{lambda_name}-id",
                rule_name=f"event-rule-{lambda_name}",
                enabled=False,
                schedule=events.Schedule.rate(Duration.minutes(5)),
                targets=[targets.LambdaFunction(handler=lambda_function)],
            )
