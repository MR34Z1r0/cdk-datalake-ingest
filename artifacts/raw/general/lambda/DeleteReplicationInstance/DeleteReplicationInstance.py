import logging

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def delete_replication_instance(replication_instance_arn):
    client = boto3.client('dms')
    response = client.delete_replication_instance(
        ReplicationInstanceArn=replication_instance_arn
    )
    return response


def lambda_handler(event, context):

    needs_dms = event['needs_dms']
    replication_instance_arn = event['replication_instance_arn']

    try:
        if needs_dms and replication_instance_arn != "":
            status = delete_replication_instance(replication_instance_arn)
            event['delete_status'] = 'SUCCESS'

            return event

        event['delete_status'] = 'FAILED'
        return event

    except Exception as e:
        event['delete_status'] = 'FAILED'
        return event
