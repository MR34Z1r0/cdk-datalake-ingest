import decimal
import json
import logging
import os
import time
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def read_table_configuration(table_name, key):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.query(KeyConditionExpression=Key('TARGET_TABLE_NAME').eq(key))
    return response['Items'][0]


def read_table_metadata(table_name, table_key, key_value):
    logger.info('read_table_meta_data : {} , {}, {}'.format(table_name, table_key, key_value))
    dynamodb = boto3.resource('dynamodb')
    dynamo_table = dynamodb.Table(table_name)
    response = dynamo_table.get_item(Key={table_key: key_value})
    if 'Item' in response:
        item = response['Item']
        return item
    else:
        logger.warning('Metadata Not Found')


def send_error_message(topic_arn, dynamodb_key, error):
    client = boto3.client("sns")
    msg = f"Failed table:"
    for table in dynamodb_key:
        msg += f"{table} \n"
    msg += f"Step: Create DMS Task for table\n"
    msg += f"Log ERROR : {error}"
    client.publish(
        TopicArn=topic_arn,
        Message=msg
    )


def create_replication_task_for_table(table_mapping, source_endpoint_arn, target_endpoint_arn, replication_instance_arn, replication_task_identifier, replication_task_settings):
    c = boto3.client('dms')
    replication_task_identifier_name = "ETLTask" + replication_task_identifier
    response = c.create_replication_task(
        ReplicationTaskIdentifier=replication_task_identifier_name,
        SourceEndpointArn=source_endpoint_arn,
        TargetEndpointArn=target_endpoint_arn,
        ReplicationInstanceArn=replication_instance_arn,
        MigrationType='full-load',
        ReplicationTaskSettings=replication_task_settings,
        TableMappings=table_mapping,)
    return response['ReplicationTask']['ReplicationTaskArn']


def get_ep_arn(target):
    if target == 'mysql':
        return os.getenv('TARGET_ENDPOINT_ARN_MYSQL')
    elif target == 'oracle':
        return os.getenv('TARGET_ENDPOINT_ARN_ORACLE')
    elif target == 'mssql':
        return os.getenv('TARGET_ENDPOINT_ARN_MSSQL')
    return '', ''


def lambda_handler(event, context):
    try:
        workers_per_task = os.getenv('WORKERS_PER_TASK').strip()
        instance_arn = event['replication_instance_arn'].strip()
        dynamo_table = os.getenv('DYNAMO_DB_TABLE').strip()
        dynamo_credentials_table = os.getenv('DYNAMO_CREDENTIALS_TABLE').strip()
        dynamodb_key = event['dynamodb_key']
        topic_arn = os.getenv("TOPIC_ARN")
        task_number = 0
        all_tables_mapping = []
        table_mapping = {}
        task_id = "task-"
        for table in dynamodb_key:
            try:
                table_config = read_table_configuration(dynamo_table, table)
                table_credentials = read_table_metadata(dynamo_credentials_table, "ENDPOINT_NAME", table_config['ENDPOINT'])
                target_endpoint_arn = get_ep_arn(table_credentials['BD_TYPE'])
                source_endpoint_arn = table_credentials['ENDPOINT_ARN']
                schema_name = table_config['SOURCE_SCHEMA']
                table_name = table_config['SOURCE_TABLE']
                table_mapping = {"rule-type": "selection", "rule-action": "include"}
                table_mapping["rule-id"] = task_number
                table_mapping["rule-name"] = schema_name.replace("_", "-") + "-" + table_name.replace("_", "-") + "-" + table + "-task"
                table_mapping["object-locator"] = {"schema-name": schema_name, "table-name": table_name}

                if table_config['FILTER_OPERATOR'] in ['between', 'incremental-full']:
                    if 'FILTER_TYPE' in table_config.keys() and table_config['FILTER_TYPE'] in ['BIGINT']:
                        start_value = datetime.strptime(table_config['START_VALUE'], "%Y-%m-%d %H:%M:%S")
                        start_value = int(start_value.timestamp())
                        end_value = datetime.strptime(table_config['END_VALUE'], "%Y-%m-%d %H:%M:%S")
                        end_value = int(end_value.timestamp())
                    else:
                        start_value = table_config['START_VALUE']
                        end_value = table_config['END_VALUE']
                    filter_column = table_config['FILTER_COLUMN']
                    table_mapping['filters'] = [{"filter-type": "source",
                                                 "column-name": filter_column,
                                                 "filter-conditions": [
                                                     {"filter-operator": 'between',
                                                      "start-value": start_value,
                                                      'end-value': end_value}]}]

                all_tables_mapping.append(table_mapping)
                logger.info(f"table configs: {table_config}")
                task_number += 1
            except Exception as e:
                send_error_message(topic_arn, table, str(e))
                logger.error(str(e))

        target_mapping_aux = str(json.dumps({'rules': all_tables_mapping}))
        task_id += schema_name.replace("_", "-") + "-" + table_name.replace("_", "-")
        logger.info(task_id)
        replication_task_settings = '{"Logging": {"EnableLogging": true,"LogComponents": [{"Id": "SOURCE_UNLOAD","Severity": "LOGGER_SEVERITY_DEFAULT"},{"Id": "SOURCE_CAPTURE","Severity": "LOGGER_SEVERITY_DEFAULT"},{"Id": "TARGET_LOAD","Severity": "LOGGER_SEVERITY_DEFAULT"},{"Id": "TARGET_APPLY","Severity": "LOGGER_SEVERITY_INFO"},{"Id": "TASK_MANAGER","Severity": "LOGGER_SEVERITY_DEBUG"}]},"FullLoadSettings": { "TargetTablePrepMode": "DROP_AND_CREATE","CreatePkAfterFullLoad": false, "StopTaskCachedChangesApplied": false, "StopTaskCachedChangesNotApplied": false, "MaxFullLoadSubTasks":' + str(workers_per_task) + ' , "TransactionConsistencyTimeout": 600, "CommitRate": 10000}}'
        task_arn = create_replication_task_for_table(target_mapping_aux, source_endpoint_arn, target_endpoint_arn, instance_arn, task_id, replication_task_settings)
        logger.info(f"task_arn : {task_arn}")
        return {'result': 'CREATING',
                'task_arn': task_arn,
                "dynamodb_key": dynamodb_key,
                'replication_instance_arn': event['replication_instance_arn']
                }

    except Exception as e:
        logger.error("Exception: {}".format(e))
        send_error_message(topic_arn, dynamodb_key, str(e))
        return {'result': 'FAILED',
                "dynamodb_key": dynamodb_key,
                'replication_instance_arn': event['replication_instance_arn']
                }
