import datetime as dt
import logging
import os
import sys
import time
import json

import boto3
import pytz
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from py4j.protocol import Py4JJavaError
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.functions import current_timestamp
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("IngestionStageDynamicsCrm")
logger.setLevel(os.environ.get("LOGGING", logging.DEBUG))

TZ_LIMA = pytz.timezone('America/Lima')
YEARS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%Y')
MONTHS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%m')
DAYS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%d')

# @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'S3_STAGE_PREFIX', 'DYNAMO_CONFIG_TABLE', 'DYNAMO_ENDPOINT_TABLE', 'INPUT_ENDPOINT', 'PROCESS_ID', 'ARN_ROLE_CRAWLER', 'PROJECT_NAME', 'DOM_STEP_NAME'])

spark = SparkSession \
    .builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .getOrCreate()

dynamodb = boto3.resource('dynamodb')
client_glue = boto3.client('glue')
client_lakeformation = boto3.client('lakeformation')
dynamo_config_table = args['DYNAMO_CONFIG_TABLE']
dynamo_endpoint_table = args['DYNAMO_ENDPOINT_TABLE']

config_table_metadata = dynamodb.Table(dynamo_config_table)
endpoint_table_metadata = dynamodb.Table(dynamo_endpoint_table)

s3_target = args['S3_STAGE_PREFIX']
arn_role_crawler = args['ARN_ROLE_CRAWLER']
job_name = args['JOB_NAME']
endpoint_name = args['INPUT_ENDPOINT']
endpoint_data = endpoint_table_metadata.get_item(Key={'ENDPOINT_NAME': endpoint_name})['Item']

if endpoint_data['BD_TYPE'] == 'mssql':
    bd_type = 'sqlserver'
else:
    bd_type = endpoint_data['BD_TYPE']

data_catalog_database_name = f"{args['PROJECT_NAME']}_{bd_type}_{endpoint_name}_stage"
data_catalog_crawler_name = data_catalog_database_name+ "_crawler"

def start_step_execution(input_json, state_machine_arn):
    try:
        logger.info('Try initialize dom workflow')
        client = boto3.client('stepfunctions')
        response = client.start_execution(
            stateMachineArn=state_machine_arn,
            input=input_json
        )
        return response
    except Exception as e:
        logger.error(e)

def create_database_data_catalog(database_data_catalog_name):
    try:
        client_glue.create_database(
            DatabaseInput={
                'Name': database_data_catalog_name}
        )
    except Exception as e:
        logger.error(e)


def get_database_data_catalog(database_data_catalog_name):
    try:
        client_glue.get_database(
            Name=database_data_catalog_name
        )
        logger.debug("Successfully get database")
        return True
    except Exception as e:
        logger.error(e)
        return False


def get_job_arn_role(job_name):
    try:
        return client_glue.get_job(
            JobName=job_name
        )['Job']['Role']
    except Exception as e:
        logger.error(e)


def grant_permissions_to_database_lakeformation(job_role_arn_name, database_data_catalog_name):
    client_lakeformation.grant_permissions(
        Principal={
            'DataLakePrincipalIdentifier': job_role_arn_name
        },
        Resource={
            'Database': {
                'Name': database_data_catalog_name
            },
        },
        Permissions=[
            'ALL',
        ],
        PermissionsWithGrantOption=[
            'ALL',
        ]
    )


def grant_permissions_lf_tag_lakeformation(job_role_arn_name):
    """Once defined by the console in lakeformation the role in Data lake administrators and the LF tags
        we proceed to assign the LF-tag permissions to the Role"""
    client_lakeformation.grant_permissions(
        Principal={
            'DataLakePrincipalIdentifier': job_role_arn_name
        },
        Resource={
            'LFTag': {
                'TagKey': 'Level',
                'TagValues': [
                    'Stage',
                ]
            },
        },
        Permissions=[
            'ASSOCIATE',
        ],
        PermissionsWithGrantOption=[
            'ASSOCIATE',
        ]
    )


def add_lf_tags_to_database_lakeformation(database_data_catalog_name):
    """Once the role has the LF-tag, we assign the same LF-tag to the database resources"""
    client_lakeformation.add_lf_tags_to_resource(
        Resource={
            'Database': {
                'Name': database_data_catalog_name
            },
        },
        LFTags=[
            {
                'TagKey': 'Level',
                'TagValues': [
                    'Stage',
                ]
            },
        ]
    )


def create_crawler(total_list):
    try:
        tables = []
        for table in total_list:
            
            table_data = config_table_metadata.get_item(Key={'TARGET_TABLE_NAME': table})['Item']
            endpoint_data = endpoint_table_metadata.get_item(Key={'ENDPOINT_NAME': table_data['ENDPOINT']})['Item']
            
            if endpoint_data['BD_TYPE'] == 'mssql':
                bd_type = 'sqlserver'
            else:
                bd_type = endpoint_data['BD_TYPE']
                
            data_source = {
                'DeltaTables': [f"{s3_target}{args['PROJECT_NAME']}/{bd_type}/{table_data['ENDPOINT']}/{table_data['STAGE_TABLE_NAME']}/"],
                'ConnectionName': '',
                'CreateNativeDeltaTable': True
            }
            tables.append(data_source)

        client_glue.create_crawler(
            Name=data_catalog_crawler_name,
            Role=arn_role_crawler,
            DatabaseName=data_catalog_database_name,
            Targets={
                'DeltaTargets': tables
            }
        )
        logger.debug("Successfully created crawler")
    except Exception as e:
        logger.error(e)


def edit_crawler(total_list):
    try:
        tables = []
        for table in total_list:
            
            table_data = config_table_metadata.get_item(Key={'TARGET_TABLE_NAME': table})['Item']
            endpoint_data = endpoint_table_metadata.get_item(Key={'ENDPOINT_NAME': table_data['ENDPOINT']})['Item']
            
            if endpoint_data['BD_TYPE'] == 'mssql':
                bd_type = 'sqlserver'
            else:
                bd_type = endpoint_data['BD_TYPE']
                
            data_source = {
                'DeltaTables': [f"{s3_target}{args['PROJECT_NAME']}/{bd_type}/{table_data['ENDPOINT']}/{table_data['STAGE_TABLE_NAME']}/"],
                'ConnectionName': '',
                'CreateNativeDeltaTable': True
            }
            tables.append(data_source)

        client_glue.update_crawler(
            Name=data_catalog_crawler_name,
            Role=arn_role_crawler,
            DatabaseName=data_catalog_database_name,
            Targets={
                'DeltaTargets': tables
            }
        )
        logger.debug("Successfully created crawler")
    except Exception as e:
        logger.error(e)


def get_crawler(crawler_name):
    try:
        client_glue.get_crawler(
            Name=crawler_name
        )
        logger.debug("Successfully get crawler")
        return True
    except Exception as e:
        logger.error(e)
        return False


def start_crawler(crawler_name):
    try:
        client_glue.start_crawler(
            Name=crawler_name
        )
        logger.debug("Successfully started crawler")
    except Exception as e:
        logger.error(e)


def update_attribute_value_dynamodb(row_key_field_name, row_key, attribute_name, attribute_value, table_name):
    logger.info('update dynamoDb Metadata : {} ,{},{},{},{}'.format(row_key_field_name, row_key, attribute_name, attribute_value, table_name))
    dynamo_table = dynamodb.Table(table_name)
    response = dynamo_table.update_item(
        Key={row_key_field_name: row_key},
        AttributeUpdates={
            attribute_name: {
                'Value': attribute_value,
                'Action': 'PUT'
            }
        }
    )


def get_dynamo_crawler_status_for_endpoint(endpoint_name):
    total_list = []
    empty_table = []
    for stage_output in config_table_metadata.scan()['Items']:
        try:
            if 'ENDPOINT' in stage_output.keys() and stage_output['ENDPOINT'] == endpoint_name:
                if not 'CRAWLER' in stage_output.keys() or not stage_output['CRAWLER']:
                    empty_table.append(stage_output['TARGET_TABLE_NAME'])
                total_list.append(stage_output['TARGET_TABLE_NAME'])

        except Exception as e:
            logger.error(f"problems with table {stage_output['TARGET_TABLE_NAME']}")
            logger.error(e)

    for table in empty_table:
        update_attribute_value_dynamodb('TARGET_TABLE_NAME', table, 'CRAWLER', True, dynamo_config_table)
        logger.debug(f"added to the crawler {table}")

    return total_list, empty_table


try:
    params = json.dumps({
        'COD_PAIS':args['INPUT_ENDPOINT'][:2],
        'PROCESS_ID' : args['PROCESS_ID'],
        "S3_PATH_DOM": "s3://datalakeingestion-ajedevanalyticsbucket02c90b73-1m6bqj7txp3gt/athenea/dominio/comercial/validaciones"
    })
    start_step_execution(params, args['DOM_STEP_NAME'])
    total_list, empty_table = get_dynamo_crawler_status_for_endpoint(endpoint_name)
    if get_crawler(data_catalog_crawler_name):
        if len(empty_table) > 0:
            edit_crawler(total_list)
        logger.debug("There is a crawler created")
        start_crawler(data_catalog_crawler_name)
    else:
        logger.debug("We proceed to check if there is a data catalog database")
        if get_database_data_catalog(data_catalog_database_name):
            logger.debug("the crawler does not exist, we proceed to its creation")
            create_crawler(total_list)
            logger.debug("We proceed to start the crawler")
            start_crawler(data_catalog_crawler_name)
        else:
            logger.debug("Proceed to obtain the job role name")
            job_role_arn_name = get_job_arn_role(job_name)
            logger.debug("We proceed to assign LF-Tag permissions")
            grant_permissions_lf_tag_lakeformation(job_role_arn_name)
            logger.debug("There is no database, proceed to create the data catalog database")
            create_database_data_catalog(data_catalog_database_name)
            logger.debug("We proceed to add the necessary permissions in lakeformation on the data catalog database")
            add_lf_tags_to_database_lakeformation(data_catalog_database_name)
            grant_permissions_to_database_lakeformation(job_role_arn_name, data_catalog_database_name)
            logger.debug("the crawler does not exist, we proceed to its creation")
            create_crawler(total_list)
            logger.debug("We proceed to start the crawler")
            start_crawler(data_catalog_crawler_name)
            
except Exception as e:
    logger.error("error while creating crawler")
    logger.error(e)
