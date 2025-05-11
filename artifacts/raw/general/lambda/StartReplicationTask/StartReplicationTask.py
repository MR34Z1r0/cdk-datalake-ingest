import json
import logging
import os

import boto3
import botocore

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def start_task(dmstask_arn):
    client = boto3.client('dms')
    try:
        response = client.start_replication_task(
            ReplicationTaskArn=dmstask_arn,
            StartReplicationTaskType='start-replication')
        return "RUNNING"
    except Exception as e:
        logger.error("Could not start dms task: %s" % e)
        return "FAILED"


def lambda_handler(event, context):
    actual_table = event['actual_table']
    task_arn = event['task_arn']
    try:
        dynamo_db_key = event['dynamodb_key']
        if actual_table == len(task_arn) - 1:
            result = 'SUCCESS'
        else:
            result = 'PENDING'
        start_task_response = start_task(task_arn[actual_table])
        load_status = start_task_response
        logger.info(load_status)
        return {"result": result,
                "task_arn": task_arn,
                "dynamodb_key": dynamo_db_key,
                "failed_task": event['failed_task'],
                'actual_table': actual_table + 1,
                'load_status': load_status,
                'replication_instance_arn': event['replication_instance_arn']
                }

    except Exception as e:
        logger.error("Could not start dms task: %s" % e)
        return {"result": "FAILED",
                "task_arn": task_arn,
                'actual_table': actual_table + 1,
                "failed_task": event['failed_task'],
                'load_status': 'LOADED',
                "dynamodb_key": dynamo_db_key,
                'replication_instance_arn': event['replication_instance_arn']}
