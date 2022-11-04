# Serverless Data Lake Monitoring : Cloud Formation

## Overview
This section of the repository has the CDK code that provisions the cloud infrastructure for serverless data lake in setting up, 
    - Environment stack
    - Monitoring stack

### Serverless Data Lake: Environment Stack 
The environment stack provisions the following,
    1. Environment that hosts the data lake,
        - Networking(VPC, Subnets, Security Groups, etc) 
        - S3 bucket - `monitor` bucket

## Serverless Data Lake: Monitoring Stack
The monitoring stack provisions the following,
    1. SNS
    2. Monitoring Lambda
    3. Monitor database
    4. Secrets to hold instant messaging application's webhooks

## Deployment
For manual deployment, follow the below steps,
1. Using Python 3.8,
    - `poetry install`
2. Stack deployment :
    - Update `ACCOUNT` & `REGION` in `./cdk/config.py` 
    - cd cdk
    - `cdk deploy --all  --profile <profile_name>`

