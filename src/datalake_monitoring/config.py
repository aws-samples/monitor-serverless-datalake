"""Config file"""
import os
ACCOUNT = os.environ.get("ACCOUNT", "123")
REGION = os.environ.get("REGION", "us-west-2")
MONITOR_S3 = os.environ.get("MONITOR_S3", f"{ACCOUNT}-{REGION}-monitor")
MONITOR_DATABASE = os.environ.get("MONITOR_DATABASE", "monitor")
MONITOR_TABLE = os.environ.get("MONITOR_TABLE", "monitor")

SECRET_MGR = os.environ.get("MONITORING_NOTIFY_SLACK_WEBHOOK", "datalake-monitoring")
