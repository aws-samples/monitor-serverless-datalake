import json
import logging
from typing import Dict, Union

import boto3

LOGGER = logging.getLogger(__name__)


def get_lambda_name_from_arn(arn: str) -> str:
    """Fetch Lambda function name from ARN"""
    arn_type = "function"
    return arn[arn.find(f"{arn_type}:") + len(arn_type) + 1 : len(arn)][
        : arn[arn.find(f"{arn_type}:") + len(arn_type) + 1 : len(arn)].find(":")
    ]


def get_secret(
    secret_id: str, region_name: str = "us-west-2", is_json: bool = True
) -> Union[str, Dict[str, str]]:
    """
    Gets credentials from secretsmanager
    """

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_id)
    secret = json.loads(response["SecretString"]) if is_json else response["SecretString"]
    LOGGER.info("Retrieved Secret for %s", secret_id)
    return secret