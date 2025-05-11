import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        task_arn = []
        dynamodb_key = []
        failed_task = []
        result = 'FAILED'
        for task in event:
            if task['result'] == 'SUCCESS':
                for table in task['dynamodb_key']:
                    dynamodb_key.append(table)
                task_arn.append(task['task_arn'])
                result = 'SUCCESS'

        return {
            'result': result,
            'dynamodb_key': dynamodb_key,
            'actual_table': 0,
            'task_arn': task_arn,
            'failed_task': failed_task,
            'replication_instance_arn': event[0]['replication_instance_arn']
        }
    except Exception as e:
        logger.info("exception : " + e)
        return {
            'result': "FAILED",
            'replication_instance_arn': event[0]['replication_instance_arn']
        }
