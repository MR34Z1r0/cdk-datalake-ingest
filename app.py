#!/usr/bin/env python3
import os

import aws_cdk as cdk
from datalake_ingestion.datalake_ingestion_stack import DatalakeIngestionStack
from salesforce_ingestion.salesforce_ingestion_stack import SalesforceIngestionStack
from dotenv import load_dotenv

load_dotenv('.env')

props = {
    'AwsAccount': os.getenv('AWS_ACCOUNT'),
    'TUXPAS_USER_ROLE' : os.getenv('TUXPAS_USER_ROLE'),
    'SalesforceS3Prefix': os.getenv('SALESFORCE_S3_PREFIX'),
    'SalesforceConectionName': os.getenv('SALESFORCE_CONECTION_NAME'),

    'EXPORT_DATA_DYNAMODBTABLE': os.getenv('EXPORT_DATA_DYNAMODBTABLE'),
    'CREDENTIALS_DYNAMODBTABLE': os.getenv('CREDENTIALS_DYNAMODBTABLE'),
    'REPLICATION_INSTANCE_CLASS': os.getenv('REPLICATION_INSTANCE_CLASS'),
    'REPLICATION_INSTANCE_ENGINE_VERSION': os.getenv('REPLICATION_INSTANCE_ENGINE_VERSION'),
    'REPLICATION_INSTANCE_IDENTIFIER': os.getenv('REPLICATION_INSTANCE_IDENTIFIER'),
    'TABLES_PER_TASK': os.getenv('TABLES_PER_TASK'),
    'WORKERS_PER_TASK': os.getenv('WORKERS_PER_TASK'),
    'PROCESS_ID_TO_LOAD': os.getenv('PROCESS_ID_TO_LOAD'),
    'COUNTRIES_TO_LOAD': os.getenv('COUNTRIES_TO_LOAD'),
    'MAX_TABLES_AT_A_TIME_GLUE' : os.getenv('MAX_TABLES_AT_A_TIME_GLUE'),
    'PROJECT_NAME': os.getenv('PROJECT_NAME'),
    'DOM_STEP_NAME' : os.getenv('DOM_STEP_NAME'),

    'ORACLE_REPLICATION_SUBNET_IDENTIFIER': os.getenv('ORACLE_REPLICATION_SUBNET_IDENTIFIER'),
    'ORACLE_VPC_SECURITY_GROUP_IDENTIFIER': os.getenv('ORACLE_VPC_SECURITY_GROUP_IDENTIFIER'),

    'MYSQL_REPLICATION_SUBNET_IDENTIFIER': os.getenv('MYSQL_REPLICATION_SUBNET_IDENTIFIER'),
    'MYSQL_VPC_SECURITY_GROUP_IDENTIFIER': os.getenv('MYSQL_VPC_SECURITY_GROUP_IDENTIFIER'),

    'MSSQL_REPLICATION_SUBNET_IDENTIFIER': os.getenv('MSSQL_REPLICATION_SUBNET_IDENTIFIER'),
    'MSSQL_VPC_SECURITY_GROUP_IDENTIFIER': os.getenv('MSSQL_VPC_SECURITY_GROUP_IDENTIFIER'),
    'Environment': os.getenv('ENVIRONMENT'),  # Prod or Dev
    'DELTA_LOADS_SECURITY_GROUP': os.getenv('DELTA_LOADS_SECURITY_GROUP'),
    'DELTA_LOADS_SUBNET_ID': os.getenv('DELTA_LOADS_SUBNET_ID'),
    'DELTA_LOADS_AVAILABILITY_ZONE': os.getenv('DELTA_LOADS_AVAILABILITY_ZONE'),
    'DELTA_LOADS_VPC_ID': os.getenv('DELTA_LOADS_VPC_ID'),
    'MAILS_TO_ADVISE': os.getenv('MAILS_TO_ADVISE'),
    'MAIL_SALESFORCE_LOG': os.getenv('MAIL_SALESFORCE_LOG')
}

app = cdk.App()
data_lake_ingestion = DatalakeIngestionStack(
    app,
    "datalakeIngestion",
    env=cdk.Environment(account=props["AwsAccount"], region="us-east-2"),
    props=props
)

salesforce_ingestion = SalesforceIngestionStack(
    app,
    "salesforceIngestion",
    env=cdk.Environment(account=props["AwsAccount"], region="us-east-2"),
    props=props,
    s3_landing_bucket=data_lake_ingestion.landing_bucket,
    s3_raw_bucket=data_lake_ingestion.raw_bucket,
    s3_stage_bucket=data_lake_ingestion.stage_bucket,
    s3_artifacts_bucket=data_lake_ingestion.artifacts_bucket,
    sns_failed_topic=data_lake_ingestion.failed_topic,
    sns_success_topic=data_lake_ingestion.success_topic,
    dynamodb_logs_table=data_lake_ingestion.logs_table,
    dynamodb_config_table=data_lake_ingestion.config_table,
    columns_table=data_lake_ingestion.columns_table,
    crawler_role_arn=data_lake_ingestion.crawler_role_arn
)

app.synth()
