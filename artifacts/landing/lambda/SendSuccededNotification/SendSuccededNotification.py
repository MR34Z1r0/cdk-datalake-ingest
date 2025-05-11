import datetime
import logging
import os
import time
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def send_success_message(topic_arn, endpoint_name):
    client = boto3.client("sns")
    logger.info(f"sending succeded message for {endpoint_name}")
    response = client.publish(
        TopicArn  = topic_arn,
        Message = f"Successfully load {endpoint_name}"
    )

def lambda_handler(event, context):
    try:
        client = boto3.resource('dynamodb')
        dynamodb_table_name = os.getenv("DYNAMODB_TABLE")
        topic_arn = os.getenv("ARN_TOPIC_SUCCESS")
        endpoint_name = os.getenv("ENDPOINT_NAME") 
        table = client.Table(dynamodb_table_name)

        stage_failed_tables = table.scan(
            FilterExpression =f"ENDPOINT = :val1 AND ACTIVE_FLAG = :val2 AND STATUS_STAGE = :val3 ",
            ExpressionAttributeValues={
                ':val1': endpoint_name,
                ':val2': 'Y',
                ':val3': 'FAILED'
            }
        )
        logger.info(f'Failed tables in stage: {stage_failed_tables}')
        if (not 'Items' in stage_failed_tables.keys() or stage_failed_tables['Items'] == []):
            send_success_message(topic_arn, endpoint_name)
            return {
                'result': "SUCCESS"
            }
        
    
    except Exception as e:
        logger.error(e)
        return {
            'result': "FAILED"
        }
    