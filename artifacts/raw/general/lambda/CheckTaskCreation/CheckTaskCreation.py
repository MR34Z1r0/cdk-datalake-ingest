import json
import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def check_task_status(dms_task_arn):
    client = boto3.client('dms')
    response = client.describe_replication_tasks(
        Filters=[
            {
                'Name': 'replication-task-arn',
                'Values': [
                    dms_task_arn
                ]
            }
        ], WithoutSettings=True

    )
    return response['ReplicationTasks'][0]['Status']


def send_error_message(topic_arn, dynamodb_key, error):
    if error.strip() == 'task_arn':
        error = 'can not locate task arn while searching for DMS resource'
    client = boto3.client("sns")
    msg = f"Failed table:"
    for table in dynamodb_key:
        msg += f"{table} \n"
    msg += f"Step: Check task creation\n"
    msg += f"Log ERROR : {error}"
    client.publish(
        TopicArn=topic_arn,
        Message=msg
    )


def lambda_handler(event, context):
    dynamodb_key = event['dynamodb_key']
    topic_arn = os.getenv("TOPIC_ARN")
    task_arn = "not created"
    replication_instance_arn = event['replication_instance_arn']
    try:
        task_arn = event['task_arn']
        status = check_task_status(task_arn)
        result = event['result']
        logger.info('Actual Task Status: {}'.format(result))
        if status in ['creating', 'moving', 'deleting', 'failed', 'failed-move', 'modifying', 'ready']:
            logger.info('New Task Status: {}'.format(status))
            if (status.lower() in ['deleting', 'failed', 'failed-move']):
                send_error_message(topic_arn, dynamodb_key, "creation has failed")
                result = 'FAILED'
            elif (status.lower() in ['ready']):
                result = 'SUCCESS'
            logger.info(result)
        return {'result': result,
                'task_arn': task_arn,
                "dynamodb_key": dynamodb_key,
                'replication_instance_arn': replication_instance_arn
                }
    except Exception as e:
        logger.error(e)
        send_error_message(topic_arn, dynamodb_key, str(e))
        return {'result': "FAILED",
                'task_arn': task_arn,
                "dynamodb_key": dynamodb_key,
                'replication_instance_arn': replication_instance_arn
                }
