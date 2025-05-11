import json
import logging
import os
import time
import traceback
from datetime import datetime

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def check_instance_status(replication_intance_arn):
    client = boto3.client('dms')
    response = client.describe_replication_instances(
        Filters=[
            {
                'Name': 'replication-instance-arn',
                'Values': [
                    replication_intance_arn,
                ]
            },
        ],
        MaxRecords=99,
        Marker='string'
    )
    status = str(response['ReplicationInstances'][0]['ReplicationInstanceStatus'])
    return status


def lambda_handler(event, context):
    replication_instance_arn = event['replication_instance_arn']
    try:
        time.sleep(860)
        status = check_instance_status(replication_instance_arn)
        if (status.lower() in ['deleted', 'deleting', 'failed', 'creating', 'modifying', 'upgrading', 'rebooting']):
            status = check_instance_status(replication_instance_arn)
            if (status.lower() == 'available'):
                create_status = 'SUCCESS'

            if (status.lower() in ["deleted", "deleting", "failed"]):
                create_status = 'FAILED'
        event['create_status'] = create_status

        return event

    except Exception as e:
        create_status = 'FAILED'
        event['create_status'] = create_status
        logger.error("Exception: {}".format(e))
        return event
