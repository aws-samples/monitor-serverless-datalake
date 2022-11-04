"""Config file"""
import os
REGION = os.environ.get("REGION", "us-west-2")
MONITOR_S3 = os.environ.get("MONITOR_S3", "monitor-datalake")
MONITOR_DATABASE = os.environ.get("MONITOR_DATABASE", "monitor")
MONITOR_TABLE = os.environ.get("MONITOR_TABLE", "monitor")

SECRET_MGR = os.environ.get("MONITORING_NOTIFY_SLACK_WEBHOOK", "datalake-monitoring")

ENABLE_SLACK_NOTIFICATION = False  # To set to True, set up Slack workflow and update above secrets. Refer Readme.
