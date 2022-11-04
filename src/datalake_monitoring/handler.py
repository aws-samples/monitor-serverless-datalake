""" Subscriber for SNS to monitor data lake ETLs and persist to Athena """

import logging
import traceback
from datetime import datetime
from json.decoder import JSONDecodeError
from typing import Dict
import requests

import boto3
import os
import json
import awswrangler as wr
import pandas as pd

import config as cf
from commons.ddb_lambda_item import SUCCESS_ITEM as LAMBDA_SUCCESS_TEMPLATE
from commons.ddb_lambda_item import FAILURE_ITEM as LAMBDA_FAILURE_TEMPLATE

from commons.ddb_glue_job_item import SUCCESS_ITEM as GLUE_JOB_SUCCESS_TEMPLATE
from commons.ddb_glue_job_item import FAILURE_ITEM as GLUE_JOB_FAILURE_TEMPLATE

from commons.ddb_glue_crawler_item import SUCCESS_ITEM as GLUE_CRAWLER_SUCCESS_TEMPLATE
from commons.ddb_glue_crawler_item import FAILURE_ITEM as GLUE_CRAWLER_FAILURE_TEMPLATE

from commons.utils import get_lambda_name_from_arn, get_secret

from functools import reduce
from operator import getitem

FAILURE_RESPONSE = {
    "statusCode": 400,
    "body": json.dumps("FAILURE: Data lake event persist to Athena has failed"),
}
SUCCESS_RESPONSE = {
    "statusCode": 200,
    "body": json.dumps("SUCCESS: Data lake event persisted to Athena"),
}

EVENT_TYPE_SUCCESS = "succeeded"
EVENT_TYPE_FAIL = "failed"


def handler(event, context):
    """Handler that takes data from SNS,
    computes the item based on the event message and persists to Athena"""
    try:
        log = logging.getLogger()
        log.setLevel(logging.INFO)
        log.info(event)
        ps = ProcessEvent(event=event, context=context, cf=cf, log=log)
        return ps.execute()

    except Exception:
        print(traceback.format_exc())
        return FAILURE_RESPONSE

class ProcessEvent(object):
    def __init__(self, event, context, cf, log):
        self.log = log
        self.event = event
        self.context = context
        self.cf = cf
        self.item = {}
        self.region = cf.REGION
        self.item_template = ""
        self.body = {}

        self.message = {
            "service": "",
            "service_name": "",
            "service_id": "",
            "exception_details": "",
            "time_stamp": "",
        }
        self.slack_webhook = get_secret(cf.SECRET_MGR)["slack_webhook"]

    def execute(self) -> dict:
        """The driver program that orchestrates processing and storing events"""
        try:
            self.log.info(
                f"Processing event messages from SNS with batch size of {len(self.event['Records'])}"
            )
            boto3.setup_default_session(profile_name=os.getenv("AWS_PROFILE"))

            for record in self.event["Records"]:
                self.body = json.loads(record["Sns"]["Message"])
                self.identify_event_source()
                self.get_item_template()
                self.generic_compose_item()

                if self.item["event_type"] not in ("success", "succeeded"):
                    self.add_remedy_details()

                from pprint import pprint as pp
                pp(self.item)
                self.put_item_athena()

                # Notify
                if self.item["event_type"].lower() == "failed":
                    self.notify()

                self.item.clear()

            return SUCCESS_RESPONSE

        except Exception:
            self.log.error(traceback.format_exc())
            return FAILURE_RESPONSE

    def compose_message(self):
        """Compose the message"""
        self.message["service"] = self.item["service_type"]
        self.message["service_name"] = self.item["service_name"]
        self.message["time_stamp"] = self.item["timestamp"]
        self.message["service_id"] = self.item["service_request_id"]

        if "exception_details" in self.item:
            exception_details = self.item["exception_details"]
        else:
            exception_details = self.item["error_message"]
        self.message["exception_details"] = exception_details

    def notify_slack(self) -> int:
        """Send message to Slack channel"""
        r = requests.post(
            url=self.slack_webhook,
            data=json.dumps(self.message),
            headers={"Content-Type": "application/json"},
        )
        return r.status_code

    def notify(self):
        self.compose_message()
        self.notify_slack()

    def identify_event_source(self) -> None:
        """Identify source (lambda, glue_job, etc.) and event type (failure, success, etc.)"""
        # Lambda
        try:
            if self.body["requestContext"]["functionArn"].find("lambda") != -1:
                self.item["service_type"] = "lambda"
                if self.body["requestContext"]["condition"].lower() == "success":
                    self.item["event_type"] = EVENT_TYPE_SUCCESS
                else:
                    self.item["event_type"] = EVENT_TYPE_FAIL
        except KeyError:
            pass

        # Glue Job
        try:
            if self.body["detail-type"].lower().find("glue job") != -1:
                self.item["service_type"] = "glue_job"
                self.item["event_type"] = self.body["detail"]["state"].lower()
        except KeyError:
            pass

        # Glue Crawler Job
        try:
            if self.body["detail-type"].lower().find("glue crawler") != -1:
                self.item["service_type"] = "glue_crawler"
                self.item["event_type"] = self.body["detail"]["state"].lower()
        except KeyError:
            pass

        try:
            if self.item["service_type"] == "" or self.item["event_type"] == "":
                raise Exception(f"Invalid SNS Event message : {self.body}")
        except KeyError:
            raise Exception(f"Invalid SNS Event message : {self.body}")

    def get_item_template(self) -> None:
        """Get the item template based on service_type and event_type"""
        job_succeeded = self.item["event_type"].lower() == EVENT_TYPE_SUCCESS
        is_lambda = self.item["service_type"].lower() == "lambda"
        is_glue_job = self.item["service_type"].lower() == "glue_job"
        is_glue_crawler_job = self.item["service_type"].lower() == "glue_crawler"

        if is_lambda and job_succeeded:
            self.item_template = LAMBDA_SUCCESS_TEMPLATE
        elif is_lambda and not job_succeeded:
            self.item_template = LAMBDA_FAILURE_TEMPLATE
        elif is_glue_job and not job_succeeded:
            self.item_template = GLUE_JOB_FAILURE_TEMPLATE
        elif is_glue_job and job_succeeded:
            self.item_template = GLUE_JOB_SUCCESS_TEMPLATE
        elif is_glue_crawler_job and not job_succeeded:
            self.item_template = GLUE_CRAWLER_SUCCESS_TEMPLATE
        elif is_glue_crawler_job and job_succeeded:
            self.item_template = GLUE_CRAWLER_FAILURE_TEMPLATE

    def generic_compose_item(self) -> None:
        """Compose the common 5 attributes generic to any item"""
        for kk in self.item_template:
            if len(self.item_template[kk]) > 0:
                if self.item["service_type"] in ("glue_job", "glue_crawler"):
                    self.item[kk] = reduce(getitem, self.item_template[kk], self.body)
                elif self.item["service_type"] == "lambda":
                    if kk.lower() == "service_name":
                        self.item[kk] = get_lambda_name_from_arn(
                            self.body["requestContext"]["functionArn"]
                        )
                    self.item[kk] = reduce(getitem, self.item_template[kk], self.body)

    def add_remedy_details(self) -> None:
        """Add Remedy details for event_type that has not executed"""
        if self.item["service_type"] == "lambda":
            self.compose_item_lambda_failure()
        if self.item["service_type"] == "glue_job":
            self.compose_item_glue_job_failure()
        if self.item["service_type"] == "glue_crawler":
            self.compose_item_glue_crawler_failure()

    def compose_item_lambda_failure(self) -> None:
        """Add remedy attributes to the item for Lambda failure event"""
        self.item["error_message"] = self.body["responsePayload"]["errorMessage"]
        try:
            self.item["exception_details"] = json.loads(
                self.body["responsePayload"]["errorMessage"]
            )["Exception"]["error_message"]
        except JSONDecodeError:
            self.item["exception_details"] = self.body["responsePayload"]["stackTrace"][0]

    def compose_item_glue_job_failure(self) -> None:
        """Add remedy attributes to the item for Glue Job failure"""
        self.item["error_message"] = self.body["detail"]["message"]
        self.item["service_run_id"] = self.body["detail"]["jobRunId"]

    def compose_item_glue_crawler_failure(self) -> None:
        """Add crawler error message reported"""
        self.item["error_message"] = self.body["detail"]["errorMessage"]

    def get_athena_types(self, df: pd.DataFrame) -> Dict[str, str]:
        """Assigns Glue data types for data from panda dataframe """
        return {
            col.lower(): "string"
            for col, _ in df.dtypes.iteritems()
        }

    def put_item_athena(self) -> None:
        """put_item to s3 and create Athena table"""
        export_date = datetime.today().strftime("%Y%m%d")
        table_s3_path = f"s3://{self.cf.MONITOR_S3}/{self.cf.MONITOR_DATABASE}/{self.cf.MONITOR_TABLE}"
        item_df = pd.DataFrame(self.item, index=[0])
        item_df["exported_on"] = export_date
        table_partition = ["exported_on"]
        column_types = self.get_athena_types(item_df)
        wr.s3.to_parquet(
            df=item_df,
            path=table_s3_path,
            dataset=True,
            table=self.cf.MONITOR_TABLE,
            database=self.cf.MONITOR_DATABASE,
            partition_cols=table_partition,
            dtype=column_types,
            compression="snappy",
            description="todo",
            mode="append",
        )

