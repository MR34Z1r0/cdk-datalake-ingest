import json
import logging
import os
import time
import traceback
from datetime import datetime

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def create_replication_instance(instance_identifier, instance_class, subnet_group_identifier, engine_version, security_groups_id):
    logger.info("creating replication instance")
    time = datetime.now().strftime("%d%m%Y%H%M%S%f")
    instance_identifier = instance_identifier + '-' + time
    client = boto3.client('dms')
    response = client.create_replication_instance(
        ReplicationInstanceIdentifier=instance_identifier,
        AllocatedStorage=100,
        ReplicationInstanceClass=instance_class,
        VpcSecurityGroupIds=[
            security_groups_id,
        ],
        ReplicationSubnetGroupIdentifier=subnet_group_identifier,
        MultiAZ=False,
        EngineVersion=engine_version,
        AutoMinorVersionUpgrade=True,
        PubliclyAccessible=True,

    )
    arn = str(response['ReplicationInstance']['ReplicationInstanceArn'])
    return arn


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
    replication_instance_identifier = os.environ['REPLICATION_INSTANCE_IDENTIFIER']
    replication_instance_class = event['replication_instance_class']
    engine_version = os.environ['REPLICATION_INSTANCE_ENGINE_VERSION']
    bd_type = event['bd_type']
    create_status = 'FAILED'
    security_groups_id = ''
    if bd_type == 'ORACLE':
        replication_instance_subnet_group_identifier = os.environ['ORACLE_REPLICATION_SUBNET_IDENTIFIER']
        security_groups_id = os.environ['ORACLE_VPC_SECURITY_GROUP_IDENTIFIER']
    if bd_type == 'MYSQL':
        replication_instance_subnet_group_identifier = os.environ['MYSQL_REPLICATION_SUBNET_IDENTIFIER']
        security_groups_id = os.environ['MYSQL_VPC_SECURITY_GROUP_IDENTIFIER']
    if bd_type == 'MSSQL':
        replication_instance_subnet_group_identifier = os.environ['MSSQL_REPLICATION_SUBNET_IDENTIFIER']
        security_groups_id = os.environ['MSSQL_VPC_SECURITY_GROUP_IDENTIFIER']

    try:
        replication_instance_arn = create_replication_instance(replication_instance_identifier, replication_instance_class, replication_instance_subnet_group_identifier, engine_version, security_groups_id)
        logger.info('arn: {}'.format(replication_instance_arn))
        status = check_instance_status(replication_instance_arn)
        time.sleep(860)
        if (status.lower() in ['deleted', 'deleting', 'failed', 'creating', 'modifying', 'upgrading', 'rebooting']):
            status = check_instance_status(replication_instance_arn)
            if (status.lower() == 'available'):
                create_status = 'SUCCESS'

            if (status.lower() in ["deleted", "deleting", "failed"]):
                create_status = 'FAILED'

        event['create_status'] = create_status
        event['replication_instance_arn'] = replication_instance_arn
        event['dms_creation_try'] = int(event['dms_creation_try']) + 1
        return event
    except Exception as e:
        event['create_status'] = 'FAILED'
        logger.error("Exception : {}".format(e))
        return event
