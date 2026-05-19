from aws_cdk import (
    Stack,
    RemovalPolicy,
    CfnOutput,
    Duration,
    Size,
    aws_s3 as s3,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_glue as glue,
    aws_lambda as _lambda,
    aws_opensearchservice as opensearch,
    aws_sqs as sqs,
    aws_lambda_event_sources as sources,
    aws_s3_notifications as s3n,
)
from constructs import Construct


class FinancialAssistantCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        ### DEFINE ROLES

        # Create a role for AWS Glue
        self.glue_role = iam.Role(self, "GlueIngestionRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        )

        ### DEFINE ASSETS

        # Create VPC for project
        self.vpc = ec2.Vpc(self, "AssistantVPC",
            max_azs=2,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                name="Public",
                subnet_type=ec2.SubnetType.PUBLIC,)
            ]
        )

        # Create the S3 Bucket Data Lake
        self.bucket = s3.Bucket(self, "FinancialDataLake",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # Create the Ingestion SQS Queue
        self.sec_ingestion_queue = sqs.Queue(
            self, "SecDownloadQueue",
            visibility_timeout=Duration.minutes(15),     # Fault tolerant message dequeuing
            retention_period=Duration.days(7)
        )

        # Define lambda layer for needed dependencies
        self.lambda_layer = _lambda.LayerVersion(self, "OpenSearchLayer",
            code=_lambda.Code.from_asset("lambda_layer"), 
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11],
            description="Manual layer for OpenSearch, Auth, and SEC Downloads"
        )

        # Define the data ingestion Lambda
        self.sec_ingestion_lambda = _lambda.Function(
            self, "SecIngestionHandler",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="ingestion_lambda.handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.minutes(10),
            memory_size=512,
            ephemeral_storage_size=Size.mebibytes(2048), # 2GB storage to handle multiple 10-k downloads
            environment={
                "BUCKET_NAME": self.bucket.bucket_name,
                "QUEUE_URL": self.sec_ingestion_queue.queue_url
            },
            layers=[self.lambda_layer]
        )

        # Define the "managed" OpenSearch Domain (vector database)
        self.opensearch_domain = opensearch.Domain(self, "FinancialAssistantDomain",
            version=opensearch.EngineVersion.OPENSEARCH_2_11,
            capacity=opensearch.CapacityConfig(
                data_node_instance_type="t3.small.search", # TODO: switch to more powerful instance type if needed
                data_nodes=1,
                master_nodes=0,
                multi_az_with_standby_enabled=False
            ),
            ebs=opensearch.EbsOptions(
                volume_size=100,        # TODO: manually scale with data requirements OR switch to serverless
                volume_type=ec2.EbsDeviceVolumeType.GP3
            ),
            # Security Settings
            enforce_https=True,
            node_to_node_encryption=True,
            encryption_at_rest=opensearch.EncryptionAtRestOptions(enabled=True),
            removal_policy=RemovalPolicy.DESTROY,
            # access_policies=[opensearch_access_policy],
            access_policies=[],
            zone_awareness=opensearch.ZoneAwarenessConfig(enabled=False),
        )

        # Create the dedicated Processing SQS Queue
        self.processing_queue = sqs.Queue(
            self, "SecIngestionQueue",
            visibility_timeout=Duration.minutes(15),    # fault tolerant dequeuing
            retention_period=Duration.days(7)
        )

        # Define the Chunk and Embed AWS Glue job
        self.ingestion_job = glue.CfnJob(self, "SEC-Processing-Job",
            name="SEC-Clean-Chunk-and-Embed",
            role=self.glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="gluestreaming",
                python_version="3",
                script_location=f"s3://{self.bucket.bucket_name}/scripts/clean_chunk_embed_glue.py" # TODO: remove hardcoding
            ),
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=6, # TODO: increase to max bedrock API can handle
            default_arguments={
                # TODO: switch from streaming glue option - unneeded after sqs batch conversion?
                "--job-bookmark-option": "job-bookmark-disable",  # Streaming relies on Spark checkpoints instead
                # Environment variables
                "--CHECKPOINT_DIR": f"s3://{self.bucket.bucket_name}/glue_checkpoints/sec_stream",
                "--OPENSEARCH_ENDPOINT": self.opensearch_domain.domain_endpoint,
                "--BUCKET_NAME": self.bucket.bucket_name,
                "--PROCESSING_QUEUE_URL": self.processing_queue.queue_url,
                "--additional-python-modules": "boto3>=1.34.0,botocore>=1.34.0,beautifulsoup4==4.12.3,lxml==5.1.0,langchain-text-splitters==0.2.0,opensearch-py==2.5.0,requests-aws4auth==1.2.3",
            }
        )

        # Define the User Query Lambda function
        self.query_lambda = _lambda.Function(self, "QueryHandler",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="query_lambda.handler",
            code=_lambda.Code.from_asset("lambda"),
            layers=[self.lambda_layer],
            timeout=Duration.seconds(30),
            environment={
                "OpenSearchEndpoint": self.opensearch_domain.domain_endpoint,
                "COLLECTION_NAME": "financial_docs" # TODO: remove hardcoded collection name
            }
        )

        ### DEFINE CONNECTIONS

        # Add the trigger connecting ingestion SQS to ingestion lambda
        self.sec_ingestion_lambda.add_event_source(
            sources.SqsEventSource(
                self.sec_ingestion_queue,
                batch_size=1, # Only process one ticker/year message at a time
                max_concurrency=8,  # number of lambda functions requesting from SEC database (API rate limit = 10 calls/sec)
            )
        )

        # Configure S3 to send notifications to Processing SQS on new file creation inside 'raw/'
        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(self.processing_queue),
            s3.NotificationKeyFilter(prefix="raw/", suffix=".txt") # TODO: brittle to non .txt files
        )

        # Create User Query Lambda URL (connection to frontend website)
        fn_url = self.query_lambda.add_function_url(
            auth_type=_lambda.FunctionUrlAuthType.NONE, # TODO: add authentication
            cors=_lambda.FunctionUrlCorsOptions(
                allowed_origins=["*"],
                allowed_methods=[_lambda.HttpMethod.GET, _lambda.HttpMethod.POST],
                allowed_headers=["content-type", "authorization"],
                max_age=Duration.days(1)    # max allowed is 1 day
            )
        )

        ### GRANT PERMISSIONS (PROVIDE POLICIES TO ROLES)

        # Grant ingestion Lambda permission to read from SQS queue + write to S3
        self.sec_ingestion_queue.grant_consume_messages(self.sec_ingestion_lambda)
        self.bucket.grant_put(self.sec_ingestion_lambda)

        # Give role access to S3 and Bedrock and OpenSearch
        self.bucket.grant_read_write(self.glue_role)
        self.glue_role.add_to_policy(iam.PolicyStatement(
            actions=["bedrock:InvokeModel"],
            resources=["*"] #TODO: specific model scope
        ))

        # Setup a basic access policy for OpenSearch access
        opensearch_access_policy = iam.PolicyStatement(
            actions=["es:*"],
            resources=[f"arn:aws:es:{self.region}:{self.account}:domain/financialassistantdomain/*"],
            principals=[iam.AccountPrincipal(self.account)],
        )
        self.opensearch_domain.add_access_policies(opensearch_access_policy)

        # Grant the Glue Job Role permissions to read and delete from the SQS queue
        glue_sqs_policy = iam.PolicyStatement(
            actions=[
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes",
                "sqs:ChangeMessageVisibility"
            ],
            resources=[self.processing_queue.queue_arn]
        )
        self.glue_role.add_to_policy(glue_sqs_policy)

        # Grant User Query Lambda permission to use Bedrock (AWS LLMs)
        self.query_lambda.add_to_role_policy(iam.PolicyStatement(
            actions=["bedrock:InvokeModel"],
            resources=["*"]
        ))

        # Grant access to vector database
        self.opensearch_domain.grant_read_write(self.glue_role)
        self.opensearch_domain.grant_read_write(self.query_lambda)

        ### OUTPUT ASSET INFORMATION

        CfnOutput(self, "DataLakeName", value=self.bucket.bucket_name)
        CfnOutput(self, "IngestionSQSUrl", value=self.sec_ingestion_queue.queue_url)
        CfnOutput(self, "OpenSearchEndpoint", value=self.opensearch_domain.domain_endpoint)
        CfnOutput(self, "QueryUrl", value=fn_url.url)
        CfnOutput(self, "GlueJobName", value=self.ingestion_job.name)
