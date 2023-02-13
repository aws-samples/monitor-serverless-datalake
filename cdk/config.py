"""Constants file for resource naming and env values"""
import os

REGION = ""  # To be updated
ACCOUNT = ""  # To be updated
SM_VPC_CIDR = ""  # To be updated

SM_VPC_NAME = "dl-vpc"

CDK_ENV = {"account": ACCOUNT, "region": REGION}

# SECRETS
MONITOR_SECRET_MANAGER = "datalake-monitoring"

# S3 Buckets
S3_LANDING_BUCKET = f"{ACCOUNT}-{REGION}-landing"
S3_PROCESSED_BUCKET = f"{ACCOUNT}-{REGION}-processed"
S3_MONITOR_BUCKET = f"{ACCOUNT}-{REGION}-monitor"

# SNS
MONITOR_SNS_TOPIC = "dl-monitor-sns"

# ATHENA
MONITOR_DB = "monitor"
MONITOR_TABLE = "monitor"
LEGISLATOR_DB = "legislators"

# NOTIFICATION - SLACK
SLACK_WEBHOOK_SECRET_NAME = "slack_webhook"

# REPO PATHS
PATH_CDK = os.path.dirname(os.path.abspath(__file__))
PATH_ROOT = os.path.dirname(PATH_CDK)
PATH_SRC = os.path.join(PATH_ROOT, 'src')

WRANGLER_ASSET = "awswrangler-layer-2.6.0-py3.8.zip"  # this is referenced in src/datalake_monitoring/Makefile

# Crawler source data
LEGISLATORS_PATH = "legislators"

# Processed output path
PROCESSED_PATH = "us-legislators/output-dir"

# SOURCE PATH
CDK_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR_PATH = os.path.dirname(CDK_DIR_PATH)
SRC_DIR_PATH = os.path.join(ROOT_DIR_PATH, 'src')
