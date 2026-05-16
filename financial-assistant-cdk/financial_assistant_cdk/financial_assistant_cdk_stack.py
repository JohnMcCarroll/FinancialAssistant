from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_ec2 as ec2,
    aws_iam as iam,
    RemovalPolicy,
    CfnOutput,
    aws_glue as glue,
    aws_lambda as _lambda,
    Duration,
    aws_opensearchservice as opensearch,
    aws_sqs as sqs,
    aws_lambda_event_sources as sources,
)
from constructs import Construct


class FinancialAssistantCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create VPC for project
        self.vpc = ec2.Vpc(self, "AssistantVPC",
            max_azs=2,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                name="Public",
                subnet_type=ec2.SubnetType.PUBLIC,)
            ]
        )

        # Create the S3 Data Lake
        self.bucket = s3.Bucket(self, "FinancialDataLake",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        self.export_value(self.bucket.bucket_name, name="DataLakeName")

        


        # Create the SQS Queue
        # We add a visibility timeout to ensure the Lambda has time to finish 
        # before SQS tries to give the message to someone else.
        sec_queue = sqs.Queue(
            self, "SecDownloadQueue",
            visibility_timeout=Duration.minutes(60) 
        )

        # Define the data ingestion Lambda
        sec_ingest_lambda = _lambda.Function(
            self, "SecIngestHandler",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="ingestion_lambda.handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.minutes(10),
            memory_size=512,
            # # Rate limiting
            # reserved_concurrent_executions=1, 
            environment={
                "BUCKET_NAME": self.bucket.bucket_name,
                "QUEUE_URL": sec_queue.queue_url
            }
        )

        # Add the SQS trigger to ingestion lambda
        sec_ingest_lambda.add_event_source(
            sources.SqsEventSource(
                sec_queue,
                batch_size=1, # Only process one ticker/year message at a time
                max_concurrency=2,
            )
        )

        # Grant the Lambda permission to read from the queue
        sec_queue.grant_consume_messages(sec_ingest_lambda)





        # Create a role for AWS Glue
        self.glue_role = iam.Role(self, "GlueIngestionRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com")
        )

        # Give role access to S3 and Bedrock
        self.bucket.grant_read_write(self.glue_role)
        self.glue_role.add_to_policy(iam.PolicyStatement(
            actions=["bedrock:InvokeModel"],
            resources=["*"] #TODO: specific model scope
        ))

        # Give Glue the base read/write permissions
        self.glue_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
        )

        # Setup a basic access policy for OpenSearch access
        access_policy = iam.PolicyStatement(
            actions=["es:*"],
            resources=[f"arn:aws:es:{self.region}:{self.account}:domain/financialassistantdomain/*"],
            principals=[iam.AccountPrincipal(self.account)],
        )

        # 2. Define the "managed" OpenSearch Domain
        self.search_domain = opensearch.Domain(self, "FinancialAssistantDomain",
            version=opensearch.EngineVersion.OPENSEARCH_2_11,
            capacity=opensearch.CapacityConfig(
                data_node_instance_type="t3.small.search",
                data_nodes=1,
                master_nodes=0,
                multi_az_with_standby_enabled=False
            ),
            ebs=opensearch.EbsOptions(
                volume_size=10,
                volume_type=ec2.EbsDeviceVolumeType.GP3
            ),
            # Security Settings
            enforce_https=True,
            node_to_node_encryption=True,
            encryption_at_rest=opensearch.EncryptionAtRestOptions(enabled=True),
            removal_policy=RemovalPolicy.DESTROY,
            access_policies=[access_policy],
            zone_awareness=opensearch.ZoneAwarenessConfig(enabled=False),
        )

        CfnOutput(self, "OpenSearchEndpoint", value=self.search_domain.domain_endpoint)

        # Define the Chunk and Embed AWS Glue job
        self.ingestion_job = glue.CfnJob(self, "SEC-Ingestion-Job",
            name="SEC-Ingestion-and-Embedding",
            role=self.glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="pythonshell",
                python_version="3.9",
                script_location=f"s3://{self.bucket.bucket_name}/scripts/glue_ingestion.py"
            ),
            default_arguments={
                "--OpenSearchEndpoint": self.search_domain.domain_endpoint,
                "--BUCKET_NAME": self.bucket.bucket_name,
                "--ticker": "AAPL", #TODO: remove hardcoded ticker
                "--additional-python-modules": "sec-edgar-downloader==5.0.2,boto3>=1.34.0,botocore>=1.34.0,beautifulsoup4,requests,numpy<2.0.0,langchain-text-splitters"
            },
            max_capacity=0.0625,
            glue_version="3.0",
        )

        # Define lambda layer for needed dependencies
        opensearch_layer = _lambda.LayerVersion(self, "OpenSearchLayer",
            code=_lambda.Code.from_asset("lambda_layer"), 
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11],
            description="Manual layer for OpenSearch and Auth"
        )

        # Define the user Query Lambda function
        self.query_lambda = _lambda.Function(self, "QueryHandler",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="query_lambda.handler",
            code=_lambda.Code.from_asset("lambda"),
            layers=[opensearch_layer],
            timeout=Duration.seconds(30),
            environment={
                "OpenSearchEndpoint": self.search_domain.domain_endpoint,
                "COLLECTION_NAME": "aapl_financials" # TODO: remove hardcoded collection name
            }
        )

        # Grant Lambda permission to use Bedrock
        self.query_lambda.add_to_role_policy(iam.PolicyStatement(
            actions=["bedrock:InvokeModel"],
            resources=["*"]
        ))

        # Create Lambda URL
        fn_url = self.query_lambda.add_function_url(
            auth_type=_lambda.FunctionUrlAuthType.NONE, # TODO: add authentication
            cors=_lambda.FunctionUrlCorsOptions(
                allowed_origins=["*"],
                allowed_methods=[_lambda.HttpMethod.GET, _lambda.HttpMethod.POST],
                allowed_headers=["content-type", "authorization"],
                max_age=Duration.days(1)
            )
        )

        # Output needed asset info to terminal
        CfnOutput(self, "QueryUrl", value=fn_url.url)
        CfnOutput(self, "GlueJobName", value=self.ingestion_job.name)

        # Grant access
        self.search_domain.grant_read_write(self.glue_role)
        self.search_domain.grant_read_write(self.query_lambda)
