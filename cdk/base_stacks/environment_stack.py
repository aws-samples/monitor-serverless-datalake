"""Base environment to manage AWS resources for the serverless datalake"""
from aws_cdk import aws_ec2 as ec2, aws_ssm as ssm, aws_s3 as s3, Stack, RemovalPolicy, Duration
from constructs import Construct
import config as cf


class DataLakeEnvironmentStack(Stack):
    """
    Stack to provision the environment following for the stateless data lake,
        - Networking
        - S3 resources
        -
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Networking
        self.vpc = ec2.Vpc(
            self,
            cf.SM_VPC_NAME,
            ip_addresses=ec2.IpAddresses.cidr(cf.SM_VPC_CIDR),
            max_azs=1,
            enable_dns_support=True,
            enable_dns_hostnames=True,
            nat_gateway_provider=ec2.NatProvider.gateway(),
            nat_gateways=1,
        )

        vpc_id_parameter = ssm.StringParameter(
            self,
            id="vpc-id-ssm",
            parameter_name=f"/{cf.SM_VPC_NAME}/vpc-id",
            string_value=self.vpc.vpc_id,
        )

        public_subnet_ids_parameter = ssm.StringListParameter(
            self,
            id="public-subnet-ids-ssm",
            string_list_value=[subnet.subnet_id for subnet in self.vpc.public_subnets],
            description="Public subnet ids for VPC in network-stack",
            parameter_name=f"/{cf.SM_VPC_NAME}/public-subnet-ids",
        )

        private_subnet_ids_parameter = ssm.StringListParameter(
            self,
            id="private-subnet-ids-ssm",
            string_list_value=[subnet.subnet_id for subnet in self.vpc.private_subnets],
            description="Private subnet ids for VPC in network-stack",
            parameter_name=f"/{cf.SM_VPC_NAME}/private-subnet-ids",
        )

        self.sg_outbound_only = ec2.SecurityGroup(
            self,
            id="base-sg-outbound",
            vpc=self.vpc,
            allow_all_outbound=True,
            description=f"{cf.SM_VPC_NAME} Outbound Only SG",
            security_group_name="base-sg-outbound",
        )

        sg_parameter = ssm.StringParameter(
            self,
            id="base-sg-outbound-ssm",
            parameter_name=f"/{cf.SM_VPC_NAME}/base-sg-outbound",
            string_value=self.sg_outbound_only.security_group_id,
        )

        # S3

        landing_bucket = s3.Bucket(
            self,
            id=f"s3-{cf.S3_LANDING_BUCKET}",
            bucket_name=cf.S3_LANDING_BUCKET,
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
            versioned=True,
            enforce_ssl=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(0),
                        )
                    ],
                    noncurrent_version_transitions=[
                        s3.NoncurrentVersionTransition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(0),
                        )
                    ],
                )
            ],
        )

        processed_bucket = s3.Bucket(
            self,
            id=f"s3-{cf.S3_PROCESSED_BUCKET}",
            bucket_name=cf.S3_PROCESSED_BUCKET,
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
            versioned=True,
            enforce_ssl=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(0),
                        )
                    ],
                    noncurrent_version_transitions=[
                        s3.NoncurrentVersionTransition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(0),
                        )
                    ],
                )
            ],
        )

        monitor_bucket = s3.Bucket(
            self,
            id=f"s3-{cf.S3_MONITOR_BUCKET}",
            bucket_name=cf.S3_MONITOR_BUCKET,
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
            versioned=True,
            enforce_ssl=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(0),
                        )
                    ],
                    noncurrent_version_transitions=[
                        s3.NoncurrentVersionTransition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(0),
                        )
                    ],
                )
            ],
        )
