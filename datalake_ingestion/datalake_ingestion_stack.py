import json
import logging
import os

import aws_cdk as cdk
from aws_cdk import Duration, Stack
from aws_cdk import aws_dms as dms
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_glue_alpha as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_redshift as redshift
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3_deployment
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subs
from aws_cdk import aws_stepfunctions as sf
from aws_cdk import aws_stepfunctions_tasks as tasks
from aws_cdk import aws_lakeformation as lakeformation
from constructs import Construct

logger = logging.getLogger()
logger.setLevel(logging.INFO)

path_analytics_jobs = "./artifacts/analytics/jobs/"
path_analytics_jars = "./artifacts/analytics/jars/"
path_to_bucket_stage_jobs_scripts = "stage/relational/jobs/"
path_to_bucket_raw_jobs_scripts = "raw/relational/jobs/"
path_lambdas = "./artifacts/raw/general/lambda/"
path_layers = "./artifacts/layers/"
path_stage_jobs = "./artifacts/stage/jobs/"
path_raw_jobs = './artifacts/raw/general/glue/'

def add_enviroment(lambda_function: _lambda, values_and_keys: dict):
    try:
        for value in values_and_keys:
            lambda_function.add_environment(value, values_and_keys[value])
    except Exception as e:
        logger.error("Exception : {}".format(e))

def get_path_since_directory_by_string(path, directory, string):
    for subdir, dirs, files in os.walk(path):
        for file in files:
            if string in file:
                full_path = os.path.join(subdir, file)
                return full_path[full_path.index(directory):]
    raise Exception(f"No {string} string found in filenames in path: {path}")

class DatalakeIngestionStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, props, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        landing_bucket = s3.Bucket(
            self, f"aje-{props['Environment']}-landing-bucket")

        raw_bucket = s3.Bucket(
            self, f"aje-{props['Environment']}-raw-bucket")

        stage_bucket = s3.Bucket(
            self, f"aje-{props['Environment']}-stage-bucket")

        analytics_bucket = s3.Bucket(
            self, f"aje-{props['Environment']}-analytics-bucket")

        artifacts_bucket = s3.Bucket(
            self, f"aje-{props['Environment']}-artifacts-bucket")

        # VPC for lambdas

        vpc_for_delta_loads = ec2.Vpc.from_vpc_attributes(self,
                                                          id=f"aje-{props['Environment']}-vpc",
                                                          availability_zones=[props['DELTA_LOADS_AVAILABILITY_ZONE']],
                                                          vpc_id=props['DELTA_LOADS_VPC_ID']
                                                          )

        delta_loads_subnet = ec2.Subnet.from_subnet_attributes(self,
                                                               id=f"aje-{props['Environment']}-subnet_for_delta_loads",
                                                               subnet_id=props['DELTA_LOADS_SUBNET_ID'],
                                                               availability_zone=props['DELTA_LOADS_AVAILABILITY_ZONE']
                                                               )

        # vpc security groups

        security_group_for_glue = ec2.SecurityGroup.from_security_group_id(self,
                                                    f"aje-{props['Environment']}-security-group-for-glue",
                                                    props['DELTA_LOADS_SECURITY_GROUP']
                                                    )

        lambda_security_group = ec2.SecurityGroup(self, f"aje-{props['Environment']}-security-group-for-lambda",
                                                  vpc=vpc_for_delta_loads, allow_all_outbound=True)
        lambda_security_group.allow_all_outbound

        # glue connection for jobs
        conection_for_glue = glue.Connection(self, f"aje-{props['Environment']}-conection-to-data-bases",
                                             type=glue.ConnectionType.NETWORK,
                                             security_groups=[security_group_for_glue],
                                             subnet=delta_loads_subnet
                                             )

        # replace with access for all bds

        lambda_security_group.add_ingress_rule(ec2.Peer.any_ipv4(), ec2.Port.all_traffic())
        lambda_security_group.add_ingress_rule(ec2.Peer.any_ipv4(), ec2.Port.tcp(443), 'allow HTTPS traffic')

        failed_topic = sns.Topic(self, f"aje-{props['Environment']}-email-failed-tables-topic", display_name="Failed Tables Notification Topic")
        
        email_addresses = props['MAILS_TO_ADVISE'].split(',')
        for email in email_addresses:
            failed_topic.add_subscription(subs.EmailSubscription(email))

        success_topic = sns.Topic(self, f"aje-{props['Environment']}-email-succeded-endpoint-topic", display_name="Success Endpoint Notification Topic")
        
        email_addresses = props['MAILS_TO_ADVISE'].split(',')
        for email in email_addresses:
            success_topic.add_subscription(subs.EmailSubscription(email))

        sns_policies = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["sns:Publish"],
            resources=[failed_topic.topic_arn, success_topic.topic_arn]
        )
        
        s3_deployment.BucketDeployment(
            self,
            f"aje-{props['Environment']}-raw-jobs",
            sources=[
                s3_deployment.Source.asset(path_raw_jobs)
            ],
            destination_bucket=artifacts_bucket,
            destination_key_prefix=path_to_bucket_raw_jobs_scripts
        )

        s3_deployment.BucketDeployment(
            self,
            f"aje-{props['Environment']}-stage-jobs",
            sources=[
                s3_deployment.Source.asset(path_stage_jobs)
            ],
            destination_bucket=artifacts_bucket,
            destination_key_prefix=path_to_bucket_stage_jobs_scripts
        )

        subnet_oracle = props['ORACLE_REPLICATION_SUBNET_IDENTIFIER']
        security_groups_oracle = props['ORACLE_VPC_SECURITY_GROUP_IDENTIFIER']
        subnet_mysql = props['MYSQL_REPLICATION_SUBNET_IDENTIFIER']
        security_groups_mysql = props['MYSQL_VPC_SECURITY_GROUP_IDENTIFIER']
        subnet_mssql = props['MSSQL_REPLICATION_SUBNET_IDENTIFIER']
        security_groups_mssql = props['MSSQL_VPC_SECURITY_GROUP_IDENTIFIER']

        # dynamo
        etl_configuration_table = dynamodb.Table(
            self,
            f"aje-{props['Environment']}-raw-{props['EXPORT_DATA_DYNAMODBTABLE']}",
            partition_key=dynamodb.Attribute(
                name="TARGET_TABLE_NAME",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )

        databases_credentials_table = dynamodb.Table(
            self,
            f"aje-{props['Environment']}-raw-{props['CREDENTIALS_DYNAMODBTABLE']}",
            partition_key=dynamodb.Attribute(
                name="ENDPOINT_NAME",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )

        stage_columns_especification_table = dynamodb.Table(
            self,
            f"aje-{props['Environment']}-stage-columns-specifications",
            partition_key=dynamodb.Attribute(
                name="TARGET_TABLE_NAME",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="COLUMN_NAME",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )

        logs_table = dynamodb.Table(
            self,
            f"aje-{props['Environment']}-logs",
            partition_key=dynamodb.Attribute(
                name="PROCESS_ID",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="DATE_SYSTEM",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )
        
        # policies for access to s3
        policies_access_to_s3_raw = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["s3:ListBucket",
                     "s3:ListBucket",
                     "s3:GetObject",
                     "s3:PutObject",
                     "s3:ReplicateObject",
                     "s3:DeleteObject",
                     "s3:GetBucketLocation",
                     "s3:ListBucket",
                     "s3:ListAllMyBuckets",
                     "s3:GetBucketAcl",],
            resources=[raw_bucket.bucket_arn, raw_bucket.bucket_arn + "/*"]
        )

        step_function_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["states:StartExecution"],
            resources=[props['DOM_STEP_NAME']]
        )

        policies_access_to_s3_stage = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["s3:ListBucket",
                     "s3:ListBucket",
                     "s3:GetObject",
                     "s3:PutObject",
                     "s3:ReplicateObject",
                     "s3:DeleteObject",
                     "s3:GetBucketLocation",
                     "s3:ListBucket",
                     "s3:ListAllMyBuckets",
                     "s3:GetBucketAcl",],
            resources=[stage_bucket.bucket_arn, stage_bucket.bucket_arn + "/*", artifacts_bucket.bucket_arn, artifacts_bucket.bucket_arn + "/*"]
        )

        policies_glue_logs = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "logs:CreateLogGroup",
                "logs:PutLogEvents",
                "logs:CreateLogStream",
            ],
            resources=["arn:aws:logs:*:*:/aws-glue/*"]
        )

        policies_glue_for_crawler = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["glue:GetCrawler",
                     "glue:GetDatabase",
                     "glue:UpdateCrawler",
                     "glue:GetJob",
                     "glue:CreateDatabase",
                     "glue:CreateCrawler",
                     "glue:StartCrawler",
                     "lakeformation:GrantPermissions",
                     "lakeformation:AddLFTagsToResource",
                     "iam:PassRole"
                     ],
            # allows on everything, because the resources doesn't exist yet
            resources=["*"]
        )

        # policies for ec2
        policies_query_to_ec2 = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["ec2:DescribeVpcs",
                     "ec2:DescribeNetworkInterfaces",
                     "ec2:DescribeInternetGateways",
                     "ec2:DescribeAvailabilityZones",
                     "ec2:DescribeSubnets",
                     "ec2:DescribeSecurityGroups",
                     "ec2:ModifyNetworkInterfaceAttribute",
                     "ec2:CreateNetworkInterface",
                     "ec2:DeleteNetworkInterface"],
            resources=[etl_configuration_table.table_arn, databases_credentials_table.table_arn, stage_columns_especification_table.table_arn, logs_table.table_arn]
        )

        policies_dynamo_stage = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["dynamodb:Scan",
                     "dynamodb:GetItem",
                     "dynamodb:UpdateItem",
                     "dynamodb:Query",
                     "dynamodb:PutItem"],
            resources=[etl_configuration_table.table_arn, databases_credentials_table.table_arn, stage_columns_especification_table.table_arn, logs_table.table_arn]
        )

        # Crawler role for stage
        role_crawler = iam.Role(self, f"aje-{props['Environment']}-stage-role-crawler",
                                assumed_by=iam.ServicePrincipal(
                                    "glue.amazonaws.com"),
                                inline_policies={
                                    'AccessStageS3': iam.PolicyDocument(
                                        statements=[iam.PolicyStatement(
                                            actions=["s3:PutObject",
                                                     "s3:GetObject"],
                                            resources=[stage_bucket.bucket_arn, stage_bucket.bucket_arn + "/*"]
                                        )]
                                    )},
                                managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole')]
                                )

        # policies
        role_access_to_stage_raw_s3 = iam.Role(self, f"aje-{props['Environment']}-access-to-stage-raw-s3-role",
                                               assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
                                               )
        role_access_to_s3_for_endpoints = iam.Role(self, f"aje-{props['Environment']}-access-to-S3-role",
                                                   assumed_by=iam.ServicePrincipal("dms.amazonaws.com"),
                                                   )
        role_access_to_stage_raw_bucket = iam.Role(self, f"aje-{props['Environment']}-access-to-stage-s3-role",
                                                   assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
                                                   )
        role_glue_access_to_vpc = iam.Role(self, f"aje-{props['Environment']}-access-to-glue-connection",
                                           assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
                                           )
        role_access_to_s3_for_endpoints.add_to_policy(policies_access_to_s3_raw)
        role_access_to_s3_for_endpoints.add_to_policy(policies_query_to_ec2)

        role_access_to_stage_raw_bucket.add_to_policy(policies_access_to_s3_raw)
        role_access_to_stage_raw_bucket.add_to_policy(policies_access_to_s3_stage)
        role_access_to_stage_raw_bucket.add_to_policy(policies_dynamo_stage)

        role_access_to_stage_raw_s3.add_to_policy(policies_access_to_s3_raw)
        role_access_to_stage_raw_s3.add_to_policy(policies_access_to_s3_stage)
        role_access_to_stage_raw_s3.add_to_policy(policies_dynamo_stage)
        role_access_to_stage_raw_s3.add_to_policy(policies_glue_for_crawler)
        role_access_to_stage_raw_s3.add_to_policy(policies_glue_logs)
        role_access_to_stage_raw_s3.add_to_policy(sns_policies)
        role_access_to_stage_raw_s3.add_to_policy(step_function_policy)

        # Endpoints
        s3_settings_oracle = dms.CfnEndpoint.S3SettingsProperty(
            bucket_name=raw_bucket.bucket_name,
            bucket_folder="temp/ORACLE",
            service_access_role_arn=role_access_to_s3_for_endpoints.role_arn,
            add_column_name=True,
        )
        oracle_target_EP = dms.CfnEndpoint(self, f"aje-{props['Environment']}-raw-Oracle-Target-EndPoint",
                                           endpoint_type="target",
                                           engine_name="s3",
                                           s3_settings=s3_settings_oracle,
                                           )

        s3_settings_mysql = dms.CfnEndpoint.S3SettingsProperty(
            bucket_name=raw_bucket.bucket_name,
            bucket_folder="temp/MYSQL",
            service_access_role_arn=role_access_to_s3_for_endpoints.role_arn,
            add_column_name=True,
        )
        mysql_target_EP = dms.CfnEndpoint(self, f"aje-{props['Environment']}-raw-MySQL-Target-EndPoint",
                                          endpoint_type="target",
                                          engine_name="s3",
                                          s3_settings=s3_settings_mysql
                                          )

        s3_settings_sql = dms.CfnEndpoint.S3SettingsProperty(
            bucket_name=raw_bucket.bucket_name,
            bucket_folder="temp/MSSQL",
            service_access_role_arn=role_access_to_s3_for_endpoints.role_arn,
            add_column_name=True,
        )
        mssql_target_EP = dms.CfnEndpoint(self, f"aje-{props['Environment']}-raw-MSSQL-Target-EndPoint",
                                          endpoint_type="target",
                                          engine_name="s3",
                                          s3_settings=s3_settings_sql
                                          )

        # policies for glue
        policies_network = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["glue:GetConnection",
                     "ec2:DescribeSubnets",
                     "ec2:DescribeSecurityGroups",
                     "ec2:DescribeVpcEndpoints",
                     "ec2:DescribeRouteTables",
                     "ec2:CreateNetworkInterface",
                     "ec2:CreateTags",
                     "ec2:DeleteNetworkInterface",
                     "ec2:DescribeNetworkInterfaces",
                     "ec2:DescribeSecurityGroups",
                     "ec2:DescribeSubnets",
                     "ec2:DescribeVpcAttribute",],
            resources=["*"]
        )

        policies_glue_logs = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "logs:CreateLogGroup",
                "logs:PutLogEvents",
                "logs:CreateLogStream",
            ],
            resources=["arn:aws:logs:*:*:/aws-glue/*"]
        )

        # secrets
        Secret = secretsmanager.Secret(self, f"aje-{props['Environment']}-raw-secret-passwords",
                                       generate_secret_string=secretsmanager.SecretStringGenerator(
                                           secret_string_template=json.dumps({"test_password_identifier": "test_password"}),
                                           generate_string_key="password"
                                       )
                                       )

        policies_secrets = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["secretsmanager:GetResourcePolicy",
                     "secretsmanager:GetSecretValue",
                     "secretsmanager:DescribeSecret",
                     "secretsmanager:ListSecretVersionIds",
                     "secretsmanager:*"],
            resources=[Secret.secret_arn]
        )

        role_glue_access_to_vpc.add_to_policy(policies_network)
        role_glue_access_to_vpc.add_to_policy(policies_dynamo_stage)
        role_glue_access_to_vpc.add_to_policy(policies_access_to_s3_raw)
        role_glue_access_to_vpc.add_to_policy(policies_access_to_s3_stage)
        role_glue_access_to_vpc.add_to_policy(policies_glue_logs)
        role_glue_access_to_vpc.add_to_policy(policies_secrets)
        role_glue_access_to_vpc.add_to_policy(sns_policies)

        role_access_to_stage_raw_bucket.add_to_policy(policies_glue_logs)
        role_access_to_stage_raw_bucket.add_to_policy(sns_policies)

        # job for raw
        raw_job = glue.Job(self, f"aje-{props['Environment']}-dms-load-by-query-raw-job",
                           executable=glue.JobExecutable.python_etl(
                               glue_version=glue.GlueVersion.V4_0,
                               python_version=glue.PythonVersion.THREE,
                               script=glue.Code.from_asset(path_raw_jobs + "load_with_query.py")
                           ),
                           connections=[conection_for_glue],
                           default_arguments={
                               '--S3_RAW_PREFIX': f"s3://{raw_bucket.bucket_name}/",
                               '--DYNAMO_CONFIG_TABLE': etl_configuration_table.table_name,
                               '--DYNAMO_ENDPOINT_TABLE': databases_credentials_table.table_name,
                               '--DYNAMO_LOGS_TABLE': logs_table.table_name,
                               '--enable-continuous-log-filter': "true",
                               '--datalake-formats': "delta",
                               '--PROJECT_NAME' : props['PROJECT_NAME'],
                               '--TOPIC_ARN': failed_topic.topic_arn,
                               '--SECRETS_NAME': Secret.secret_name,
                               '--TABLE_NAME': "NONE",
                               '--THREADS_FOR_INCREMENTAL_LOADS': "6",
                               '--SECRETS_REGION' : props['DELTA_LOADS_AVAILABILITY_ZONE']
                           },
                           worker_type=glue.WorkerType.G_1_X,
                           worker_count=2,
                           continuous_logging=glue.ContinuousLoggingProps(enabled=True),
                           enable_profiling_metrics=True,
                           timeout=Duration.hours(3),
                           max_concurrent_runs=200,
                           role=role_glue_access_to_vpc
                           )

        # job for stage
        stage_job = glue.Job(self, f"aje-{props['Environment']}-dms-load-stage-job",
                             executable=glue.JobExecutable.python_etl(
                                 glue_version=glue.GlueVersion.V4_0,
                                 python_version=glue.PythonVersion.THREE,
                                 script=glue.Code.from_asset(path_stage_jobs + "aje-stage-dms-load.py")
                             ),
                             default_arguments={
                                 '--S3_RAW_PREFIX': f"s3://{raw_bucket.bucket_name}/",
                                 '--S3_STAGE_PREFIX': f"s3://{stage_bucket.bucket_name}/",
                                 '--TOPIC_ARN': failed_topic.topic_arn,
                                 '--PROJECT_NAME' : props['PROJECT_NAME'],
                                 '--DYNAMO_CONFIG_TABLE': etl_configuration_table.table_name,
                                 '--DYNAMO_ENDPOINT_TABLE': databases_credentials_table.table_name,
                                 '--DYNAMO_STAGE_COLUMNS': stage_columns_especification_table.table_name,
                                 '--DYNAMO_LOGS_TABLE': logs_table.table_name,
                                 '--enable-continuous-log-filter': "true",
                                 '--TABLE_NAME': "NONE",
                                 '--datalake-formats': "delta",
                             },
                             worker_type=glue.WorkerType.G_1_X,
                             worker_count=2,
                             continuous_logging=glue.ContinuousLoggingProps(enabled=True),
                             enable_profiling_metrics=True,
                             timeout=Duration.hours(2),
                             max_concurrent_runs=1000,
                             role=role_access_to_stage_raw_bucket
                             )

        # job for crawlers
        crawler_job = glue.Job(self, f"aje-{props['Environment']}-crawlers-job",
                               executable=glue.JobExecutable.python_etl(
                                   glue_version=glue.GlueVersion.V4_0,
                                   python_version=glue.PythonVersion.THREE,
                                   script=glue.Code.from_bucket(artifacts_bucket, path_to_bucket_stage_jobs_scripts + "crawlers_job.py"),
                               ),
                               default_arguments={
                                   '--S3_STAGE_PREFIX': f"s3://{stage_bucket.bucket_name}/",
                                   '--DYNAMO_CONFIG_TABLE': etl_configuration_table.table_name,
                                   '--DYNAMO_ENDPOINT_TABLE': databases_credentials_table.table_name,
                                   '--PROJECT_NAME' : props['PROJECT_NAME'],
                                   '--enable-continuous-log-filter': "true",
                                   '--datalake-formats': "delta",
                                   '--additional-python-modules': 'boto3==1.26.42',
                                   '--ARN_ROLE_CRAWLER': role_crawler.role_arn,
                                   '--DOM_STEP_NAME' : props['DOM_STEP_NAME']
                               },
                               worker_type=glue.WorkerType.G_1_X,
                               worker_count=4,
                               continuous_logging=glue.ContinuousLoggingProps(enabled=True),
                               enable_profiling_metrics=True,
                               timeout=Duration.hours(2),
                               max_concurrent_runs=10,
                               role=role_access_to_stage_raw_s3
                               )

        # job for analytics
        glue_connection_to_on_premise = glue.Connection.from_connection_name(
            self,
            f"aje-{props['Environment']}-analytics-delta_lake_glue_connection",
            connection_name="Connection_to_on_premise"
        )

        # layers for lambdas
        mysql_lambda_layer = _lambda.LayerVersion(self, "MySQL-library-Layer",
                                                  code=_lambda.AssetCode(path_layers + "pymysql"),
                                                  compatible_runtimes=[_lambda.Runtime.PYTHON_3_7])

        cx_oracle_lambda_layer = _lambda.LayerVersion(self, "Oracle-library-Layer",
                                                      code=_lambda.AssetCode(path_layers + "cx_oracle"),
                                                      compatible_runtimes=[_lambda.Runtime.PYTHON_3_7])

        mssql_lambda_layer = _lambda.LayerVersion(self, "MsSQL-library-Layer",
                                                  code=_lambda.AssetCode(path_layers + "mssql"),
                                                  compatible_runtimes=[_lambda.Runtime.PYTHON_3_7])

        # create lambdas
        _lambdaprepare_for_task_creation = _lambda.Function(self, f"aje-{props['Environment']}-raw-PrepareForTaskCreation",
                                                            runtime=_lambda.Runtime.PYTHON_3_7,
                                                            handler="PrepareForTaskCreation.lambda_handler",
                                                            code=_lambda.Code.from_asset(path_lambdas + "PrepareForTaskCreation"),
                                                            timeout=Duration.minutes(15))

        _lambdaprepare_for_task_execution = _lambda.Function(self, f"aje-{props['Environment']}-raw-PrepareForTaskExecution",
                                                             runtime=_lambda.Runtime.PYTHON_3_7,
                                                             handler="PrepareForTaskExecution.lambda_handler",
                                                             code=_lambda.Code.from_asset(path_lambdas + "PrepareForTaskExecution"),
                                                             timeout=Duration.minutes(15))

        _lambdacheck_task_creation = _lambda.Function(self, f"aje-{props['Environment']}-raw-CheckTaskCreation",
                                                      runtime=_lambda.Runtime.PYTHON_3_7,
                                                      handler="CheckTaskCreation.lambda_handler",
                                                      code=_lambda.Code.from_asset(path_lambdas + "CheckTaskCreation"),
                                                      timeout=Duration.minutes(15))

        _lambdacheck_dms_replication_task_status = _lambda.Function(self, f"aje-{props['Environment']}-raw-CheckDMSReplicationTaskStatus",
                                                                    runtime=_lambda.Runtime.PYTHON_3_7,
                                                                    handler="CheckDMSReplicationTaskStatus.lambda_handler",
                                                                    code=_lambda.Code.from_asset(path_lambdas + "CheckDMSReplicationTaskStatus"),
                                                                    timeout=Duration.minutes(15))

        _lambdacreate_replication_instance = _lambda.Function(self, f"aje-{props['Environment']}-raw-CreateReplicationInstance",
                                                              runtime=_lambda.Runtime.PYTHON_3_7,
                                                              handler="CreateReplicationInstance.lambda_handler",
                                                              code=_lambda.Code.from_asset(path_lambdas + 'CreateReplicationInstance'),
                                                              timeout=Duration.minutes(15))

        _lambdacreate_replication_task = _lambda.Function(self, f"aje-{props['Environment']}-raw-CreateReplicationTaskForTable",
                                                          runtime=_lambda.Runtime.PYTHON_3_7,
                                                          handler="CreateReplicationTaskForTable.lambda_handler",
                                                          code=_lambda.Code.from_asset(path_lambdas + 'CreateReplicationTaskForTable'),
                                                          timeout=Duration.minutes(15))

        _lambdadelete_dms_replication_task = _lambda.Function(self, f"aje-{props['Environment']}-raw-DeleteDMSReplicationTask",
                                                              runtime=_lambda.Runtime.PYTHON_3_7,
                                                              handler="DeleteDMSReplicationTask.lambda_handler",
                                                              code=_lambda.Code.from_asset(path_lambdas + 'DeleteDMSReplicationTask'),
                                                              timeout=Duration.minutes(15))

        _lambdadelete_replication_instance = _lambda.Function(self, f"aje-{props['Environment']}-raw-DeleteReplicationInstance",
                                                              runtime=_lambda.Runtime.PYTHON_3_7,
                                                              handler="DeleteReplicationInstance.lambda_handler",
                                                              code=_lambda.Code.from_asset(path_lambdas + 'DeleteReplicationInstance'),
                                                              timeout=Duration.minutes(15))

        _lambdasupervise_dms_creation = _lambda.Function(self, f"aje-{props['Environment']}-raw-SuperviseDMSCreation",
                                                         runtime=_lambda.Runtime.PYTHON_3_7,
                                                         handler="etlSuperviseDMSCreation.lambda_handler",
                                                         code=_lambda.Code.from_asset(path_lambdas + 'etlSuperviseDMSCreation'),
                                                         timeout=Duration.minutes(15))

        _lambdastart_replication_task = _lambda.Function(self, f"aje-{props['Environment']}-raw-StartReplicationTask",
                                                         runtime=_lambda.Runtime.PYTHON_3_7,
                                                         handler="StartReplicationTask.lambda_handler",
                                                         code=_lambda.Code.from_asset(path_lambdas + 'StartReplicationTask'),
                                                         timeout=Duration.minutes(15))

        _lambdastart_workflow = _lambda.Function(self, f"aje-{props['Environment']}-raw-StartCDCWorkflow",
                                                 runtime=_lambda.Runtime.PYTHON_3_7,
                                                 handler="StartWorkFlow.lambda_handler",
                                                 code=_lambda.Code.from_asset(path_lambdas + 'StartWorkFlow'),
                                                 timeout=Duration.minutes(15))

        _lambdatest_replication_instance = _lambda.Function(self, f"aje-{props['Environment']}-raw-TestReplicationInstanceConn",
                                                            runtime=_lambda.Runtime.PYTHON_3_7,
                                                            handler="TestReplicationInstance.lambda_handler",
                                                            code=_lambda.Code.from_asset(path_lambdas + 'TestReplicationInstance'),
                                                            timeout=Duration.minutes(15))

        _lambdaget_endpoint_for_crawler = _lambda.Function(self, f"aje-{props['Environment']}-get-endpoint-for-crawler",
                                                           runtime=_lambda.Runtime.PYTHON_3_7,
                                                           handler="prepareForCrawler.lambda_handler",
                                                           code=_lambda.Code.from_asset(path_lambdas + 'prepareForCrawler'),
                                                           layers=[cx_oracle_lambda_layer],
                                                           timeout=Duration.minutes(5))

        _lambdaupdate_start_value_mysql = _lambda.Function(self, f"aje-{props['Environment']}-raw-UpdateLoadStartValue-MYSQL",
                                                           runtime=_lambda.Runtime.PYTHON_3_7,
                                                           handler="UpdateLoadStartValue-MySQL.lambda_handler",
                                                           code=_lambda.Code.from_asset(path_lambdas + 'UpdateLoadStartValue-MySQL'),
                                                           layers=[mysql_lambda_layer],
                                                           timeout=Duration.minutes(15),
                                                           vpc=vpc_for_delta_loads,
                                                           vpc_subnets=ec2.SubnetSelection(
                                                               subnets=[delta_loads_subnet]
                                                           ),
                                                           security_groups=[lambda_security_group])

        _lambdaupdate_start_value_oracle = _lambda.Function(self, f"aje-{props['Environment']}-raw-UpdateLoadStartValue-ORACLE",
                                                            runtime=_lambda.Runtime.PYTHON_3_7,
                                                            handler="UpdateLoadStartValue-ORACLE.lambda_handler",
                                                            code=_lambda.Code.from_asset(path_lambdas + 'UpdateLoadStartValue-ORACLE'),
                                                            layers=[cx_oracle_lambda_layer],
                                                            timeout=Duration.minutes(15),
                                                            vpc=vpc_for_delta_loads,
                                                            vpc_subnets=ec2.SubnetSelection(
                                                                subnets=[delta_loads_subnet]
                                                            ),
                                                            security_groups=[lambda_security_group])

        _lambdaupdate_start_value_mssql = _lambda.Function(self, f"aje-{props['Environment']}-raw-UpdateLoadStartValue-MSSQL",
                                                           runtime=_lambda.Runtime.PYTHON_3_7,
                                                           handler="UpdateLoadStartValue-MSSQL.lambda_handler",
                                                           code=_lambda.Code.from_asset(path_lambdas + 'UpdateLoadStartValue-MSSQL'),
                                                           layers=[mssql_lambda_layer],
                                                           timeout=Duration.minutes(15),
                                                           vpc=vpc_for_delta_loads,
                                                           vpc_subnets=ec2.SubnetSelection(
                                                               subnets=[delta_loads_subnet]
                                                           ),
                                                           security_groups=[lambda_security_group])

        _lambdamigrate_data = _lambda.Function(self, f"aje-{props['Environment']}-raw-MigrateData",
                                               runtime=_lambda.Runtime.PYTHON_3_7,
                                               handler="MigrateData.lambda_handler",
                                               code=_lambda.Code.from_asset(path_lambdas + 'MigrateData'),
                                               timeout=Duration.minutes(15))

        # event bridge to start the load

        event_bridge = events.Rule(self, "start load rule",
                                   schedule=events.Schedule.cron(minute="30", hour="5"),
                                   )
        event_bridge.add_target(targets.LambdaFunction(_lambdastart_workflow, retry_attempts=1, event = events.RuleTargetInput.from_object({"PROCESS_ID_TO_LOAD" : props["PROCESS_ID_TO_LOAD"], "COUNTRIES_TO_LOAD" : props["COUNTRIES_TO_LOAD"]})))
            
        # fails controls
        add_instance_arn_pass = sf.Pass(self, "add void arn",
                                        result_path="$.replication_instance_arn",
                                        result=sf.Result.from_string("null")
                                        )

        add_error_prepare_task_pass = sf.Pass(self, "add prepare task error",
                                              result_path="$.error",
                                              result=sf.Result.from_string("error while preparing the migration task")
                                              )

        migration_by_glue_fail = sf.Wait(self, "migrate by query Failed",
                                         time=sf.WaitTime.duration(Duration.seconds(10))
                                         )

        record_failed = tasks.SnsPublish(self, "Record Failed",
                                         message=sf.TaskInput.from_object(
                                             {
                                                 "message": "failed preparing load",
                                                 "error.$": "$.error",
                                             }),
                                         topic=failed_topic,
                                         result_path=sf.JsonPath.DISCARD
                                         )

        no_task_created = sf.Wait(self, "No task Created",
                                  time=sf.WaitTime.duration(Duration.seconds(10))
                                  )

        finnish_task_creation = sf.Wait(self, "Finnish task creation",
                                        time=sf.WaitTime.duration(Duration.seconds(10))
                                        )
        job_raw_failed_step = sf.Pass(self, "error raw job",
                                      )

        # steps from step function
        prepare_for_task_creation_step = tasks.LambdaInvoke(self, "Prepare for Task Creation",
                                                            lambda_function=_lambdaprepare_for_task_creation,
                                                            retry_on_service_exceptions=False,
                                                            payload_response_only=True
                                                            )

        prepare_for_task_execution_step = tasks.LambdaInvoke(self, "Prepare for Task execution",
                                                             lambda_function=_lambdaprepare_for_task_execution,
                                                             retry_on_service_exceptions=False,
                                                             payload_response_only=True
                                                             )

        check_task_creation_step = tasks.LambdaInvoke(self, "Check task creation",
                                                      lambda_function=_lambdacheck_task_creation,
                                                      retry_on_service_exceptions=False,
                                                      payload_response_only=True
                                                      )

        create_instance_step = tasks.LambdaInvoke(self, "Create Replication Instance",
                                                  lambda_function=_lambdacreate_replication_instance,
                                                  retry_on_service_exceptions=False,
                                                  payload_response_only=True
                                                  )

        delete_on_first_fail_step = tasks.LambdaInvoke(self, "delete Replication Instance on failed creation",
                                                       lambda_function=_lambdadelete_replication_instance,
                                                       retry_on_service_exceptions=False,
                                                       payload_response_only=True
                                                       )

        second_chance_step = tasks.LambdaInvoke(self, "Replication Instance Connection second chance",
                                                lambda_function=_lambdasupervise_dms_creation,
                                                retry_on_service_exceptions=False,
                                                payload_response_only=True
                                                )

        wait_replication_instance = sf.Wait(self, "Wait for Testing Replication Instance Connection",
                                            time=sf.WaitTime.duration(Duration.seconds(30))
                                            )

        get_endpoint_for_crawler_step = tasks.LambdaInvoke(self, "Get Endpoint",
                                                           lambda_function=_lambdaget_endpoint_for_crawler,
                                                           retry_on_service_exceptions=False,
                                                           payload_response_only=True
                                                           )

        update_value_mysql_step = tasks.LambdaInvoke(self, "Update start value for table MySQL",
                                                     lambda_function=_lambdaupdate_start_value_mysql,
                                                     retry_on_service_exceptions=False,
                                                     payload_response_only=True
                                                     )

        update_value_oracle_step = tasks.LambdaInvoke(self, "Update start value for table Oracle",
                                                      lambda_function=_lambdaupdate_start_value_oracle,
                                                      retry_on_service_exceptions=False,
                                                      payload_response_only=True
                                                      )

        update_value_sql_step = tasks.LambdaInvoke(self, "Update start value for table SQL",
                                                   lambda_function=_lambdaupdate_start_value_mssql,
                                                   retry_on_service_exceptions=False,
                                                   payload_response_only=True
                                                   )

        create_dms_task_step = tasks.LambdaInvoke(self, "Create DMS Task for table",
                                                  lambda_function=_lambdacreate_replication_task,
                                                  retry_on_service_exceptions=False,
                                                  payload_response_only=True
                                                  )

        wait_dms_task_creation = sf.Wait(self, "Wait for DMS Task Creation",
                                         time=sf.WaitTime.duration(Duration.seconds(30))
                                         )

        start_dms_task_step = tasks.LambdaInvoke(self, "Start DMS Task",
                                                 lambda_function=_lambdastart_replication_task,
                                                 retry_on_service_exceptions=False,
                                                 payload_response_only=True
                                                 )

        wait_for_dms = sf.Wait(self, "Wait for DMS",
                               time=sf.WaitTime.duration(Duration.seconds(30))
                               )

        check_dms_task_status_step = tasks.LambdaInvoke(self, "Check DMS Task Status",
                                                        lambda_function=_lambdacheck_dms_replication_task_status,
                                                        retry_on_service_exceptions=False,
                                                        payload_response_only=True
                                                        )

        delete_task_on_failure_step = tasks.LambdaInvoke(self, "Delete Task on Failure",
                                                         lambda_function=_lambdadelete_dms_replication_task,
                                                         retry_on_service_exceptions=False,
                                                         payload_response_only=True
                                                         )

        delete_task_step = tasks.LambdaInvoke(self, "Delete replication task on success",
                                              lambda_function=_lambdadelete_dms_replication_task,
                                              retry_on_service_exceptions=False,
                                              payload_response_only=True
                                              )

        wait_for_delete_task = sf.Wait(self, "Wait for Delete DMS Task",
                                       time=sf.WaitTime.duration(Duration.seconds(600))
                                       )

        delete_replication_instance_step = tasks.LambdaInvoke(self, "Delete Replication Instance",
                                                              lambda_function=_lambdadelete_replication_instance,
                                                              retry_on_service_exceptions=False,
                                                              payload_response_only=True
                                                              )

        migrate_data_step = tasks.LambdaInvoke(self, "Migrate Data",
                                               lambda_function=_lambdamigrate_data,
                                               retry_on_service_exceptions=False,
                                               payload_response_only=True
                                               )

        migrate_data_step.add_retry(backoff_rate=5, max_attempts=10, errors=["Lambda.TooManyRequestsException"])

        wait_for_delete_task_on_failure = sf.Wait(self, "Wait for delete task on failure",
                                                  time=sf.WaitTime.duration(Duration.seconds(300))
                                                  )

        job_raw_step = tasks.GlueStartJobRun(self, "raw job",
                                             glue_job_name=raw_job.job_name,
                                             integration_pattern=sf.IntegrationPattern.RUN_JOB,
                                             arguments=sf.TaskInput.from_object({
                                                 "--TABLE_NAME.$": "$.dynamodb_key.table"
                                             }),
                                             result_path='$.glue_result'
                                             )
        job_raw_step.add_retry(backoff_rate=5, max_attempts=10, errors=["Glue.AWSGlueException", "Glue.ConcurrentRunsExceededException"])
        job_raw_step.add_catch(errors=['States.TaskFailed'], handler=job_raw_failed_step)

        job_stage_step = tasks.GlueStartJobRun(self, "stage job",
                                               glue_job_name=stage_job.job_name,
                                               integration_pattern=sf.IntegrationPattern.RUN_JOB,
                                               arguments=sf.TaskInput.from_object({
                                                   "--TABLE_NAME.$": "$.dynamodb_key"
                                               }),
                                               result_path='DISCARD'
                                               )
        job_stage_step.add_retry(backoff_rate=5, max_attempts=10, errors=["Glue.AWSGlueException", "Glue.ConcurrentRunsExceededException"])

        job_stage_by_glue_step = tasks.GlueStartJobRun(self, "stage job by glue",
                                                       glue_job_name=stage_job.job_name,
                                                       integration_pattern=sf.IntegrationPattern.RUN_JOB,
                                                       arguments=sf.TaskInput.from_object({
                                                           "--TABLE_NAME.$": "$.dynamodb_key.table"
                                                       }),
                                                       result_path='DISCARD'
                                                       )
        job_stage_by_glue_step.add_retry(backoff_rate=5, max_attempts=10, errors=["Glue.AWSGlueException", "Glue.ConcurrentRunsExceededException"])

        job_crawlers_step = tasks.GlueStartJobRun(self, "crawler job",
                                                  glue_job_name=crawler_job.job_name,
                                                  integration_pattern=sf.IntegrationPattern.RUN_JOB,
                                                  arguments=sf.TaskInput.from_object({
                                                      "--INPUT_ENDPOINT.$": "$.endpoint",
                                                      "--PROCESS_ID.$" : "$.process_id"
                                                  }),
                                                  result_path='DISCARD'
                                                  )
        job_crawlers_step.add_retry(backoff_rate=5, max_attempts=10)

        # parallel steps

        bd_type_map = sf.Map(self, "Map State",
                             parameters={
                                 "dynamodb_key.$": "$$.Map.Item.Value",
                                 "replication_instance_arn.$": "$.replication_instance_arn",
                                 "bd_type.$": "$.bd_type",
                                 "process.$":"$.process"
                             },
                             items_path=sf.JsonPath.string_at("$.table_names"),
                             result_path=sf.JsonPath.DISCARD
                             )

        needs_query_map = sf.Map(self, "Needs Query?",
                                 parameters={
                                     "dynamodb_key.$": "$$.Map.Item.Value",
                                     "replication_instance_arn.$": "$.replication_instance_arn",
                                     "process.$":"$.process"
                                 },
                                 items_path=sf.JsonPath.string_at("$.dynamodb_key"),
                                 max_concurrency = int(props['MAX_TABLES_AT_A_TIME_GLUE'])
                                 )

        create_task_per_table_map = sf.Map(self, "Create taks per table",
                                           parameters={
                                               "dynamodb_key.$": "$$.Map.Item.Value",
                                               "replication_instance_arn.$": "$.replication_instance_arn"
                                           },
                                           items_path=sf.JsonPath.string_at("$.dynamodb_key.table")
                                           )

        migrate_tables_map = sf.Map(self, "migrate data map",
                                    parameters={
                                        "dynamodb_key.$": "$$.Map.Item.Value",
                                        "replication_instance_arn.$": "$.replication_instance_arn"
                                    },
                                    items_path=sf.JsonPath.string_at("$.dynamodb_key")
                                    )

        # all choices from state machine
        needs_instance_choice = sf.Choice(self, 'Needs Replication Instance?')
        needs_instance_choice.when(sf.Condition.boolean_equals("$.needs_dms", True), create_instance_step)
        needs_instance_choice.otherwise(add_instance_arn_pass)

        replication_instance_choice = sf.Choice(self, 'Replication Instance Creation Status?')
        replication_instance_choice.when(sf.Condition.string_equals("$.create_status", "FAILED"), second_chance_step)
        replication_instance_choice.otherwise(bd_type_map)

        second_chance_choice = sf.Choice(self, 'Replication Instance Creation Status? second chance')
        second_chance_choice.when(sf.Condition.string_equals("$.create_status", "SUCCESS"), bd_type_map)
        second_chance_choice.when(sf.Condition.and_(sf.Condition.string_equals("$.create_status", "FAILED"), sf.Condition.number_less_than_equals("$.dms_creation_try", 3)), delete_on_first_fail_step)
        second_chance_choice.otherwise(delete_replication_instance_step)

        BD_type_choice = sf.Choice(self, "select BD Type")
        BD_type_choice.when(sf.Condition.string_equals("$.bd_type", "MYSQL"), update_value_mysql_step)
        BD_type_choice.when(sf.Condition.string_equals("$.bd_type", "MSSQL"), update_value_sql_step)
        BD_type_choice.when(sf.Condition.string_equals("$.bd_type", "ORACLE"), update_value_oracle_step)
        BD_type_choice.otherwise(update_value_oracle_step)

        start_value_updated_choice = sf.Choice(self, "Start Value Updated?")
        start_value_updated_choice.when(sf.Condition.string_equals("$.result", "SUCCEEDED"), prepare_for_task_creation_step)
        start_value_updated_choice.otherwise(record_failed)

        prepare_for_task_creation_choice = sf.Choice(self, "Preparation ready for task creation?")
        prepare_for_task_creation_choice.when(sf.Condition.string_equals("$.result", "SUCCEEDED"), needs_query_map)
        prepare_for_task_creation_choice.otherwise(add_error_prepare_task_pass)

        migrate_succesful_choice = sf.Choice(self, "import by dms succeded?")
        migrate_succesful_choice.when(sf.Condition.string_equals("$.result", "SUCCESS"), job_stage_step)
        migrate_succesful_choice.otherwise(migration_by_glue_fail)

        needs_glue_choice = sf.Choice(self, "needs glue?")
        needs_glue_choice.when(sf.Condition.string_equals("$.dynamodb_key.type", "needs_glue"), job_raw_step)
        needs_glue_choice.otherwise(create_task_per_table_map)

        dms_task_created_choice = sf.Choice(self, "DMS Task created?")
        dms_task_created_choice.when(sf.Condition.string_equals("$.result", "CREATING"), wait_dms_task_creation)
        dms_task_created_choice.when(sf.Condition.or_(sf.Condition.string_equals("$.result", "SUCCESS"), sf.Condition.string_equals("$.result", "FAILED")), finnish_task_creation)
        dms_task_created_choice.otherwise(finnish_task_creation)

        dms_task_empty_choice = sf.Choice(self, "DMS Task empty?")
        dms_task_empty_choice.when(sf.Condition.string_equals("$.result", "FAILED"), no_task_created)
        dms_task_empty_choice.when(sf.Condition.string_equals("$.result", "SUCCESS"), start_dms_task_step)
        dms_task_empty_choice.otherwise(no_task_created)

        dms_task_complete_choice = sf.Choice(self, "Task Complete?")
        dms_task_complete_choice.when(sf.Condition.string_equals("$.load_status ", "CREATING"), wait_for_dms)
        dms_task_complete_choice.when(sf.Condition.and_(sf.Condition.string_equals("$.result", "PENDING"), sf.Condition.string_equals("$.load_status ", "LOADED")), start_dms_task_step)
        dms_task_complete_choice.when(sf.Condition.and_(sf.Condition.string_equals("$.result", "SUCCESS"), sf.Condition.string_equals("$.load_status ", "LOADED")), delete_task_step)
        dms_task_complete_choice.otherwise(delete_task_step)

        definition = needs_instance_choice
        create_instance_step.next(wait_replication_instance)
        wait_replication_instance.next(replication_instance_choice)
        second_chance_step.next(second_chance_choice)
        delete_on_first_fail_step.next(create_instance_step)
        add_error_prepare_task_pass.next(record_failed)
        add_instance_arn_pass.next(bd_type_map)
        update_value_mysql_step.next(start_value_updated_choice)
        update_value_sql_step.next(start_value_updated_choice)
        update_value_oracle_step.next(start_value_updated_choice)

        bd_type_map.iterator(BD_type_choice).next(delete_replication_instance_step)
        create_task_per_table_map.iterator(create_dms_task_step).next(prepare_for_task_execution_step)

        prepare_for_task_creation_step.next(prepare_for_task_creation_choice)
        needs_query_map.iterator(needs_glue_choice)
        needs_query_map.next(get_endpoint_for_crawler_step)
        get_endpoint_for_crawler_step.next(job_crawlers_step)

        create_dms_task_step.next(wait_dms_task_creation)
        wait_dms_task_creation.next(check_task_creation_step)
        check_task_creation_step.next(dms_task_created_choice)

        job_raw_step.next(job_stage_by_glue_step)

        prepare_for_task_execution_step.next(dms_task_empty_choice)
        start_dms_task_step.next(wait_for_dms)
        wait_for_dms.next(check_dms_task_status_step)
        check_dms_task_status_step.next(dms_task_complete_choice)

        delete_task_on_failure_step.next(wait_for_delete_task_on_failure)

        migrate_tables_map.iterator(migrate_data_step)
        migrate_data_step.next(migrate_succesful_choice)

        delete_task_step.next(migrate_tables_map)
        migrate_tables_map.next(wait_for_delete_task)

        update_value_oracle_step.add_catch(
            record_failed,
            result_path="$.error",
        )

        update_value_mysql_step.add_catch(
            record_failed,
            result_path="$.error"
        )

        update_value_sql_step.add_catch(
            record_failed,
            result_path="$.error"
        )

        state_function = sf.StateMachine(self, f"aje-{props['Environment']}-raw-State-Machine-DeltaLoad",
                                         definition=definition,
                                         )

        # policies for step function
        policies_glue_fpr_step = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["glue:StartJobRun",
                     "glue:GetJobRun",
                     "glue:GetJobRuns",
                     "glue:BatchStopJobRun"],
            resources=[raw_job.job_arn, stage_job.job_arn]
        )

        state_function.add_to_role_policy(policies_glue_fpr_step)

        # policies for lambdas
        policies_dynamo = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["dynamodb:Scan",
                     "dynamodb:GetItem",
                     "dynamodb:UpdateItem",
                     "dynamodb:Query"],
            resources=[etl_configuration_table.table_arn, databases_credentials_table.table_arn]
        )

        policies_state_function = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["states:StartExecution"],
            resources=[state_function.state_machine_arn]
        )

        policies_secrets = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["secretsmanager:GetResourcePolicy",
                     "secretsmanager:GetSecretValue",
                     "secretsmanager:DescribeSecret",
                     "secretsmanager:ListSecretVersionIds",
                     "secretsmanager:*"],
            resources=[Secret.secret_arn]
        )

        policies_dms = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["dms:DescribeReplicationInstanceTaskLogs",
                     "dms:DescribeReplicationTasks",
                     "dms:CreateReplicationTask",
                     "dms:DeleteReplicationTask",
                     "dms:StartReplicationTask",
                     "dms:CreateReplicationInstance",
                     "dms:RebootReplicationInstance",
                     "dms:DeleteReplicationInstance",
                     "dms:TestConnection",
                     "dms:DescribeReplicationInstances",
                     "dms:ModifyReplicationInstance",
                     "dms:DescribeConnections",
                     "dms:AddTagsToResource",
                     "dms:DescribeOrderableReplicationInstances",
                     "dms:ModifyReplicationSubnetGroup"],
            resources=["*"]
        )

        policies_state_function_all = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["states:StartExecution"],
            resources=["*"]
        )

        policies_ec2 = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["ec2:DeleteNetworkInterface",
                     "ec2:DescribeVpcs",
                     "ec2:DescribeInternetGateways",
                     "ec2:DescribeNetworkInterfaces",
                     "ec2:DescribeSecurityGroups",
                     "ec2:DescribeAvailabilityZones",
                     "ec2:DescribeSubnets",
                     "ec2:ModifyNetworkInterfaceAttribute",
                     "ec2:CreateNetworkInterface",
                     ],
            resources=["*"]
        )

        policies_s3 = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "S3:GetObject",
                "S3:PutObject",
                "S3:ListBucket",
                "s3:DeleteObject",
            ],
            resources=[raw_bucket.bucket_arn, raw_bucket.bucket_arn + "/*"]
        )

        _lambdastart_workflow.add_to_role_policy(policies_dynamo)
        _lambdastart_workflow.add_to_role_policy(policies_state_function)

        _lambdacreate_replication_task.add_to_role_policy(policies_dynamo)
        _lambdacreate_replication_task.add_to_role_policy(policies_dms)
        _lambdacreate_replication_task.add_to_role_policy(sns_policies)

        _lambdacheck_task_creation.add_to_role_policy(policies_dms)
        _lambdacheck_task_creation.add_to_role_policy(sns_policies)

        _lambdaprepare_for_task_creation.add_to_role_policy(policies_dynamo)

        _lambdacheck_dms_replication_task_status.add_to_role_policy(policies_dms)
        _lambdacheck_dms_replication_task_status.add_to_role_policy(sns_policies)

        _lambdacreate_replication_instance.add_to_role_policy(policies_dms)

        _lambdadelete_dms_replication_task.add_to_role_policy(policies_dms)

        _lambdadelete_replication_instance.add_to_role_policy(policies_dms)

        _lambdastart_replication_task.add_to_role_policy(policies_dms)

        _lambdatest_replication_instance.add_to_role_policy(policies_dms)
        _lambdatest_replication_instance.add_to_role_policy(policies_dynamo)

        _lambdasupervise_dms_creation.add_to_role_policy(policies_dms)

        _lambdamigrate_data.add_to_role_policy(policies_dynamo)
        _lambdamigrate_data.add_to_role_policy(policies_s3)
        _lambdamigrate_data.add_to_role_policy(sns_policies)

        _lambdaget_endpoint_for_crawler.add_to_role_policy(policies_dynamo)
        _lambdaget_endpoint_for_crawler.add_to_role_policy(sns_policies)

        _lambdaupdate_start_value_mysql.add_to_role_policy(policies_dynamo)
        _lambdaupdate_start_value_mysql.add_to_role_policy(policies_dms)
        _lambdaupdate_start_value_mysql.add_to_role_policy(policies_secrets)
        _lambdaupdate_start_value_mysql.add_to_role_policy(policies_ec2)
        _lambdaupdate_start_value_mysql.add_to_role_policy(sns_policies)

        _lambdaupdate_start_value_oracle.add_to_role_policy(policies_dynamo)
        _lambdaupdate_start_value_oracle.add_to_role_policy(policies_dms)
        _lambdaupdate_start_value_oracle.add_to_role_policy(policies_secrets)
        _lambdaupdate_start_value_oracle.add_to_role_policy(policies_ec2)
        _lambdaupdate_start_value_oracle.add_to_role_policy(sns_policies)

        _lambdaupdate_start_value_mssql.add_to_role_policy(policies_dynamo)
        _lambdaupdate_start_value_mssql.add_to_role_policy(policies_dms)
        _lambdaupdate_start_value_mssql.add_to_role_policy(policies_secrets)
        _lambdaupdate_start_value_mssql.add_to_role_policy(policies_ec2)
        _lambdaupdate_start_value_mssql.add_to_role_policy(sns_policies)

        # Enviroment for lambdas
        create_replication_instance_environment = {
            "REPLICATION_INSTANCE_ENGINE_VERSION": props['REPLICATION_INSTANCE_ENGINE_VERSION'],
            "REPLICATION_INSTANCE_IDENTIFIER": props['REPLICATION_INSTANCE_IDENTIFIER'],
            "ORACLE_REPLICATION_SUBNET_IDENTIFIER": subnet_oracle,
            "ORACLE_VPC_SECURITY_GROUP_IDENTIFIER": security_groups_oracle,
            "MYSQL_REPLICATION_SUBNET_IDENTIFIER": subnet_mysql,
            "MYSQL_VPC_SECURITY_GROUP_IDENTIFIER": security_groups_mysql,
            "MSSQL_REPLICATION_SUBNET_IDENTIFIER": subnet_mssql,
            "MSSQL_VPC_SECURITY_GROUP_IDENTIFIER": security_groups_mssql,
        }

        prepare_for_task_creation_environment = {
            "TABLES_PER_TASK": props['TABLES_PER_TASK'],
            "DYNAMO_DB_TABLE": etl_configuration_table.table_name
        }

        create_replication_task_table_enviroment = {
            "DYNAMO_DB_TABLE": etl_configuration_table.table_name,
            "DYNAMO_CREDENTIALS_TABLE": databases_credentials_table.table_name,
            "TARGET_ENDPOINT_ARN_ORACLE": oracle_target_EP.ref,
            "TARGET_ENDPOINT_ARN_MYSQL": mysql_target_EP.ref,
            "TARGET_ENDPOINT_ARN_MSSQL": mssql_target_EP.ref,
            "WORKERS_PER_TASK": props['WORKERS_PER_TASK'],
            'TOPIC_ARN': failed_topic.topic_arn,
        }

        check_task_creation_environment = {
            'TOPIC_ARN': failed_topic.topic_arn,
        }

        migrate_data_environment = {
            "DYNAMO_DB_TABLE": etl_configuration_table.table_name,
            "DYNAMO_CREDENTIALS_TABLE": databases_credentials_table.table_name,
            "S3_BUCKET_TARGET": raw_bucket.bucket_name,
            "S3_BUCKET_SOURCE": raw_bucket.bucket_name,
            'TOPIC_ARN': failed_topic.topic_arn,
            'PROJECT_NAME' : props['PROJECT_NAME']
        }

        start_workflow_environment = {
            "DYNAMO_DB_TABLE": etl_configuration_table.table_name,
            "DYNAMO_CREDENTIALS_TABLE": databases_credentials_table.table_name,
            "REPLICATION_INSTANCE_CLASS": props['REPLICATION_INSTANCE_CLASS'],
            "STATE_MACHINE_ARN": state_function.state_machine_arn
        }

        test_replication_instance_environment = {
            "DYNAMO_DB_TABLE": etl_configuration_table.table_name,
            "DYNAMO_CREDENTIALS_TABLE": databases_credentials_table.table_name,
            "ETL_S3_ENDPOINT_ARN": oracle_target_EP.ref
        }

        get_endpoint_for_crawler_environment = {
            "DYNAMO_DB_TABLE": etl_configuration_table.table_name,
            'TOPIC_ARN': success_topic.topic_arn,
        }

        update_start_value_mysql_environment = {
            "DYNAMO_DB_TABLE": etl_configuration_table.table_name,
            "DYNAMO_CREDENTIALS_TABLE": databases_credentials_table.table_name,
            "SECRETS_NAME": Secret.secret_name,
            "TOPIC_ARN": failed_topic.topic_arn,
            "SECRETS_REGION" : props['DELTA_LOADS_AVAILABILITY_ZONE']
        }

        update_start_value_mssql_environment = {
            "DYNAMO_DB_TABLE": etl_configuration_table.table_name,
            "DYNAMO_CREDENTIALS_TABLE": databases_credentials_table.table_name,
            "SECRETS_NAME": Secret.secret_name,
            "TOPIC_ARN": failed_topic.topic_arn,
            "SECRETS_REGION" : props['DELTA_LOADS_AVAILABILITY_ZONE']
        }

        update_start_value_oracle_environment = {
            "DYNAMO_DB_TABLE": etl_configuration_table.table_name,
            "DYNAMO_CREDENTIALS_TABLE": databases_credentials_table.table_name,
            "SECRETS_NAME": Secret.secret_name,
            "TOPIC_ARN": failed_topic.topic_arn,
            "SECRETS_REGION" : props['DELTA_LOADS_AVAILABILITY_ZONE']
        }

        add_enviroment(_lambdacreate_replication_instance, create_replication_instance_environment)
        add_enviroment(_lambdaprepare_for_task_creation, prepare_for_task_creation_environment)
        add_enviroment(_lambdacheck_task_creation, check_task_creation_environment)
        add_enviroment(_lambdacreate_replication_task, create_replication_task_table_enviroment)
        add_enviroment(_lambdastart_workflow, start_workflow_environment)
        add_enviroment(_lambdaget_endpoint_for_crawler, get_endpoint_for_crawler_environment)
        add_enviroment(_lambdatest_replication_instance, test_replication_instance_environment)
        add_enviroment(_lambdaupdate_start_value_mysql, update_start_value_mysql_environment)
        add_enviroment(_lambdaupdate_start_value_mssql, update_start_value_mssql_environment)
        add_enviroment(_lambdamigrate_data, migrate_data_environment)
        add_enviroment(_lambdaupdate_start_value_oracle, update_start_value_oracle_environment)

        self.landing_bucket = landing_bucket
        self.raw_bucket = raw_bucket
        self.stage_bucket = stage_bucket
        self.artifacts_bucket = artifacts_bucket
        self.failed_topic = failed_topic
        self.success_topic = success_topic
        self.logs_table = logs_table
        self.config_table = etl_configuration_table
        self.crawler_role_arn = role_crawler.role_arn
        self.columns_table = stage_columns_especification_table
        