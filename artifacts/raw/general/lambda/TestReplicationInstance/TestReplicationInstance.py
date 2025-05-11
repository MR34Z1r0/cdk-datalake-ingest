import logging
import os
import time

import boto3
import botocore

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def check_connection(replication_instance_arn, endpoint_arn):
    client = boto3.client('dms')
    response = client.test_connection(ReplicationInstanceArn=replication_instance_arn, EndpointArn=endpoint_arn)
    return response['Connection']['Status']


def describe_connection(replication_instance_arn, endpoint_arn):
    client = boto3.client('dms')
    response = client.describe_connections(Filters=[{'Name': 'replication-instance-arn', 'Values': [replication_instance_arn]}, {'Name': 'endpoint-arn', 'Values': [endpoint_arn]}])
    return response['Connections'][0]


def read_table_metamata(table_name, table_key, key_value):
    logger.info('read_table_meta_data : {} , {}, {}'.format(table_name, table_key, key_value))
    dynamodb = boto3.resource('dynamodb')
    dynamo_table = dynamodb.Table(table_name)
    response = dynamo_table.get_item(Key={table_key: key_value})
    if 'Item' in response:
        item = response['Item']
        return item
    else:
        logger.warning('Metadata Not Found')


def test_endpoint(replication_instance_arn, db_endpoint_arn):
    logger.info('Testing DB EndPoint: ' + str(db_endpoint_arn))
    if db_endpoint_arn != "legacy_glue" and db_endpoint_arn != "ec2":
        status = check_connection(replication_instance_arn, db_endpoint_arn)
        while (status.lower() in ['testing', 'successful', 'failed', 'deleting']):
            time.sleep(20)
            connections = describe_connection(replication_instance_arn, db_endpoint_arn)
            status = connections['Status']
            if(status.lower() == 'successful'):
                create_status = 'SUCCESS'
                break
            if(status.lower() in ["failed", "deleting"]):
                create_status = 'FAILED'
                break
        logger.info('End Point Connection Status {}'.format(create_status))
        return create_status
    else:
        return 'SUCCESS'


def lambda_handler(event, context):
    try:
        table_data = os.getenv('DYNAMO_DB_TABLE').strip()
        table_credentials = os.getenv('DYNAMO_CREDENTIALS_TABLE').strip()
        replication_instance_arn = event['replication_instance_arn']
        s3_endpoint_arn = os.environ['ETL_S3_ENDPOINT_ARN']
        for table in event['table_names']:
            tables_data = read_table_metamata(table_data, 'TARGET_TABLE_NAME', table[0])
            table_endpoint = tables_data['ENDPOINT']
            table_endpoint_arn = read_table_metamata(table_credentials, 'ENDPOINT_NAME', table_endpoint)['ENDPOINT_ARN']
            test = test_endpoint(replication_instance_arn, table_endpoint_arn)
            if test == 'FAILED':
                logger.error("fail test to " + str(table_endpoint_arn) + " End Point")
                event['result'] = "FAILED"
                return event

        test = test_endpoint(replication_instance_arn, s3_endpoint_arn)
        if test == 'FAILED':
            logger.error("fail test to test S3 End Point")
            event['result'] = "FAILED"

        else:
            event['result'] = "SUCCESS"

        return event

    except botocore.exceptions.ClientError as e:
        logger.error("Could not start dms task: %s" % e)
        event['result'] = "FAILED"
        return event
