"""Constants file for resource naming and env values"""
import os

REGION = ""
ACCOUNT = ""
SM_VPC_CIDR = "10.32.0.0/18"

SM_VPC_NAME = "dl-vpc"

CDK_ENV = {"account": ACCOUNT, "region": REGION}

# SECRETS
MONITOR_SECRET_MANAGER = "datalake-monitoring"

# S3 Buckets
S3_MONITOR_BUCKET = f"{ACCOUNT}-{REGION}-monitor"

# SNS
MONITOR_SNS_TOPIC = "dl-monitor-sns"

# ATHENA
MONITOR_DB = "monitor"
MONITOR_TABLE = "monitor"

# NOTIFICATION - SLACK
SLACK_WEBHOOK_SECRET_NAME = "datalake-monitoring"

# REPO PATHS
PATH_CDK = os.path.dirname(os.path.abspath(__file__))
PATH_ROOT = os.path.dirname(PATH_CDK)
PATH_SRC = os.path.join(PATH_ROOT, 'src')

WRANGLER_ASSET = "awswrangler-layer-2.6.0-py3.8.zip" # this is referenced in src/datalake_monitoring/Makefile

# SOURCE PATH
CDK_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR_PATH = os.path.dirname(CDK_DIR_PATH)
SRC_DIR_PATH = os.path.join(ROOT_DIR_PATH, 'src')

