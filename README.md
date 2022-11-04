# Monitor ETL Jobs in Serverless Data Lake

## Overview
This repository serves as a launch pad for monitoring serverless data lakes in AWS. The objective is to provide a plug and play mechanism for monitoring enterprise scale data lakes. Data lakes starts small and rapidly explodes with adoption. With growing adoption, the data pipelines also grows in number and complexity. It is pivotal to ensure that the data pipeline executes as per SLA and failures be mitigated. 

The solution provides mechanisms for the following, 
    1. Capture state changes across all tasks in the data lake
    2. Quickly notify operations of failures as they happen
    3. Measure service reliability across data lake – to identify opportunities for performance optimization 

## Architecture

The solution highlighted in Orange box, illustrates with serverless services like AWS Lambda & AWS Glue ( These are not included). The design can be easily extended to other services like AWS Fargate.

![state_process lambda](src/assets/architecture.png)

The solution has the following components,
1. Storage : Amazon S3 
Keeping it simple, the following S3 buckets are created ,
   - `monitor` bucket : Houses the `monitor` database 
2. Monitoring:
The monitoring stack has the following components
   - `Amazon EventBridge Rules` : Captures success & failures in data pipeline
   - `Amazon SNS`: The messages are published to SNS
   - `Monitor Database`: The overall states of the data pipeline are captured here
   - `Slack Webhooks`: Slack is used to notify important events to stakeholders. Slack webhooks to be set up as described in [here](https://slack.com/help/articles/360053571454-Set-up-a-workflow-in-Slack)
     - Refer to the below template for the structure of the notification message
     
     ![message-text](src/assets/slack-message-template.png)
     - The slack webhook has to be updated to the AWS Secretes `datalake-monitoring` as shown below,
     
     ![message-text](src/assets/webhook.png)
   - `Monitoring Lambda`: `datalake-monitoring-lambda` acts on the messages from SNS in the following ways,
      - Persist state of the data pipeline execution
      - Notify stakeholders via IM on failure
3. Prerequisites 
    - All Lambda functions (functioning as data pipeline) should be configured with destinations to the SNS topic 

## Deployment
The Cloud Deployment Kit(CDK) has been provided to deploy the solution. Follow the below steps,
1. Using Python 3.8,
    - `cd <path to pyproject.toml>`
    - `poetry install`
2. Stack deployment :
    - Update `ACCOUNT` & `REGION` in `./cdk/config.py` 
    - cd cdk
    - cdk deploy --all  --profile <profile_name>
