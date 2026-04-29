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
)
from constructs import Construct

class FinancialAssistantCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1. Create small VPC for ChromaDB instance
        self.vpc = ec2.Vpc(self, "AssistantVPC",
            max_azs=2,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                name="Public",
                subnet_type=ec2.SubnetType.PUBLIC,)
            ]
        )

        # 2. Create the S3 Data Lake
        self.bucket = s3.Bucket(self, "FinancialDataLake",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # 3. Create a Role for AWS Glue
        self.glue_role = iam.Role(self, "GlueIngestionRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com")
        )

        # Give role access to S3 and Bedrock
        self.bucket.grant_read_write(self.glue_role)
        self.glue_role.add_to_policy(iam.PolicyStatement(
            actions=["bedrock:InvokeModel"],
            resources=["*"] #TODO: specific model scope
        ))

        # This gives Glue the base permissions to write logs and talk to the network
        self.glue_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
        )

        # Output the Bucket Name to your terminal after deployment
        self.export_value(self.bucket.bucket_name, name="DataLakeName")

        # 4. Security Group for ChromaDB
        # This acts as a virtual firewall
        self.chroma_sg = ec2.SecurityGroup(self, "ChromaSG",
            vpc=self.vpc,
            allow_all_outbound=True,
            description="Allow access to ChromaDB"
        )

        # For an MVP, we are opening port 8000 to the world. 
        # TODO: restrict this to my own IP
        self.chroma_sg.add_ingress_rule(
            ec2.Peer.any_ipv4(), 
            ec2.Port.tcp(8000), 
            "Allow ChromaDB API access"
        )

        # TODO: Paid tier
        # # 5. Define the EC2 Instance
        # # t3.medium has 4GB RAM, which is the sweet spot for ChromaDB
        # instance = ec2.Instance(self, "ChromaInstance",
        #     instance_type=ec2.InstanceType("t3.medium"),
        #     machine_image=ec2.MachineImage.latest_amazon_linux2023(),
        #     vpc=self.vpc,
        #     vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
        #     security_group=self.chroma_sg
        # )

        # # 6. User Data: The "Setup Script"
        # # This installs Docker and starts ChromaDB automatically
        # user_data_script = [
        #     "yum update -y",
        #     "yum install -y docker",
        #     "systemctl start docker",
        #     "systemctl enable docker",
        #     "docker run -d -p 8000:8000 chromadb/chroma"
        # ]

        # Free tier
        # 5. Define the EC2 Instance
        # Change from t3.medium to t3.micro
        instance = ec2.Instance(self, "ChromaInstance",
            instance_type=ec2.InstanceType("t3.micro"), # <--- This is the fix
            machine_image=ec2.MachineImage.latest_amazon_linux2023(),
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            security_group=self.chroma_sg
        )

        # 6. User Data: The "Setup Script"
        user_data_script = [
            "yum update -y",
            "yum install -y docker",
            # Add 2GB of Swap space for stability
            "dd if=/dev/zero of=/swapfile bs=128M count=16",
            "chmod 600 /swapfile",
            "mkswap /swapfile",
            "swapon /swapfile",
            "echo '/swapfile swap swap defaults 0 0' >> /etc/fstab",
            # Start Docker
            "systemctl start docker",
            "systemctl enable docker",
            "docker run -d -p 8000:8000 chromadb/chroma"
        ]
        
        for line in user_data_script:
            instance.add_user_data(line)

        # Output the Public IP so we can connect to it later
        CfnOutput(self, "ChromaPublicIP", value=instance.instance_public_ip)

        # 7. Define the Glue Job
        # We use a 'Python Shell' job for the MVP to keep it simple and cheap
        ingestion_job = glue.CfnJob(self, "SEC-Ingestion-Job",
            name="SEC-Ingestion-and-Embedding",
            role=self.glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="pythonshell",
                python_version="3.9",
                script_location=f"s3://{self.bucket.bucket_name}/scripts/glue_ingestion.py"
            ),
            default_arguments={
                "--CHROMA_IP": instance.instance_public_ip,
                "--BUCKET_NAME": self.bucket.bucket_name,
                "--additional-python-modules": "chromadb-client,boto3>=1.34.0,botocore>=1.34.0,beautifulsoup4"
            },
            max_capacity=0.0625 # This is the smallest/cheapest unit for Python Shell
        )

        # 8. Define the Query Lambda
        query_lambda = _lambda.Function(self, "QueryHandler",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="query_lambda.handler",
            code=_lambda.Code.from_asset("lambda"), # This points to a 'lambda' folder in your project
            timeout=Duration.seconds(30),
            environment={
                "CHROMA_IP": instance.instance_public_ip,
                "COLLECTION_NAME": "aapl_financials"
            }
        )

        # Grant Lambda permission to talk to Bedrock
        query_lambda.add_to_role_policy(iam.PolicyStatement(
            actions=["bedrock:InvokeModel"],
            resources=["*"]
        ))

        # Create a Public URL for testing
        fn_url = query_lambda.add_function_url(
            auth_type=_lambda.FunctionUrlAuthType.NONE # TODO: For MVP only!
        )

        CfnOutput(self, "QueryUrl", value=fn_url.url)
