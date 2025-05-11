import logging
import os
import time

import boto3
import botocore

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def check_task_status(dms_task_arn, failed_task):
    time.sleep(2)
    client = boto3.client('dms')
    logger.info("****before calling api\n")
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
    logger.info("****after calling api")
    task = response['ReplicationTasks'][0]
    status = task['Status']
    logger.info("*****status: " + status)
    if status == 'starting' or status == 'modifying' or status == 'creating' or status == 'running' or status == 'stopping' or status == 'deleting':
        result = 'CREATING'
    elif status == 'stopped':
        stop_reason = task['StopReason']
        if stop_reason == 'Stop Reason FULL_LOAD_ONLY_FINISHED':
            result = 'LOADED'
        else:
            result = 'LOADED'
            failed_task.append(dms_task_arn)

    else:
        result = 'LOADED'
        failed_task.append(dms_task_arn)

    logger.info("****result" + result + " \n")
    return result


def lambda_handler(event, context):
    try:
        actual_table = event['actual_table']
        dms_task_arn = event['task_arn']
        result = event['result']
        failed_task = event['failed_task']
        status = event['load_status']
        logger.info('dms task arn: {}'.format(dms_task_arn[actual_table - 1]))
        if status != 'LOADED':
            status = check_task_status(dms_task_arn[actual_table - 1], failed_task)
        logger.info("Response from check_dms_status{}".format(status))
        dynamo_db_key = event["dynamodb_key"]
        return {"result": result,
                "task_arn": dms_task_arn,
                'failed_task': event['failed_task'],
                'load_status': status,
                'actual_table': actual_table,
                "dynamodb_key": dynamo_db_key,
                'replication_instance_arn': event['replication_instance_arn']}

    except botocore.exceptions.ClientError as e:
        logger.error("Could not start dms task: %s" % e)
        return {"result": "FAILED",
                'load_status': status,
                'failed_task': event['failed_task'],
                'actual_table': actual_table,
                "dynamodb_key": dynamo_db_key,
                'replication_instance_arn': event['replication_instance_arn']
                }
