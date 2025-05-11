import datetime
import json
import logging
import os
import time

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def start_cdc_execution(input_json, state_machine_arn):
    client = boto3.client('stepfunctions')
    response = client.start_execution(
        stateMachineArn=state_machine_arn,
        input=input_json
    )
    return response


def update_attribute_value_dyndb(row_key_field_name, row_key, attribute_name, attribute_value, table_name):
    logger.info('update dynamoDb Metadata : {} ,{},{},{},{}'.format(row_key_field_name, row_key, attribute_name, attribute_value, table_name))
    dynamodb = boto3.resource('dynamodb')
    dynamo_table = dynamodb.Table(table_name)
    response = dynamo_table.update_item(
        Key={row_key_field_name: row_key},
        AttributeUpdates={
            attribute_name: {
                'Value': attribute_value,
                'Action': 'PUT'
            }
        }
    )


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


def get_crawlers(dynamodb_table_name, countries_to_load):
    items = []
    for country in countries_to_load:
        dynamodb = boto3.client('dynamodb')
        response = dynamodb.scan(
            TableName=dynamodb_table_name,
            FilterExpression = "begins_with(ENDPOINT_NAME, :val)",
            ExpressionAttributeValues={':val': {'S' :country}}  
        )
        items.extend(response['Items'])
    return items

    
def get_tables_crawlers(dynamodb_table_name, process, countries_to_load):
    dynamodb = boto3.client('dynamodb')
    items = []
    filter_expresion = "ACTIVE_FLAG = :flag and PROCESS_ID = :process_id and begins_with(ENDPOINT, :val)"
    columns = ', '.join(['TARGET_TABLE_NAME', 'ENDPOINT', '#C'])
    
    for country in countries_to_load:
        expression_attributes = {
                        ':flag': {'S' : 'Y'},
                        ':process_id': {'S' : process},
                        ':val': {'S' :country}
                        }
        last_evaluated_key = None
        more_data = True
        while more_data:
            if last_evaluated_key:
                response = dynamodb.scan(
                    TableName=dynamodb_table_name,
                    ExclusiveStartKey=last_evaluated_key,
                    FilterExpression=filter_expresion,
                    ProjectionExpression=columns,
                    ExpressionAttributeNames = {'#C': 'COLUMNS'},
                    ExpressionAttributeValues=expression_attributes
                )
            else:
                response = dynamodb.scan(
                    TableName=dynamodb_table_name,
                    FilterExpression=filter_expresion,
                    ProjectionExpression=columns,
                    ExpressionAttributeNames = {'#C': 'COLUMNS'},
                    ExpressionAttributeValues=expression_attributes
                )
            logger.info(response)
            items.extend(response['Items'])
            last_evaluated_key = response.get('LastEvaluatedKey')
            if not last_evaluated_key:
                more_data = False
    
    return items


def find_Table_Names(table_name, table_credentials, process, countries_to_load):
    endpoint = ""
    end_point_name = ''
    exclude_endpoints = ['SALESFORCE']
    dynamo_table_name = table_name
    bd_tables = []
    bd_types = []
    input_table_names = []
    
    # get all credentials 
    credentials_data = get_crawlers(table_credentials, countries_to_load)
    credentials = {}
    for credential_row in credentials_data:
        try:
            credential_name = credential_row['ENDPOINT_NAME']['S']
            credentials[credential_name] = credential_row['BD_TYPE']['S']
        except Exception as e:
            logger.error(f"error with credentials from {credential_name} : {e}")        
    logger.info(credentials)
    
    response = get_tables_crawlers(table_name, process, countries_to_load)
    logger.info(response)
    
    # group tables by endpoint type
    for item in response:
        try:
            logger.info(item)
            table_name = item['TARGET_TABLE_NAME']['S']
            end_point_name = item['ENDPOINT']['S']

            logger.info(f"table: {table_name}")
            logger.info(f"process_id_to_load: {item.get('PROCESS_ID', {'S':'*'})['S'].strip()}")

            if item['ENDPOINT']['S'] not in exclude_endpoints:
                update_attribute_value_dyndb('TARGET_TABLE_NAME', item['TARGET_TABLE_NAME']['S'], 'LAST UPDATE DATE', str(datetime.datetime.today().date()), dynamo_table_name)
                update_attribute_value_dyndb('TARGET_TABLE_NAME', item['TARGET_TABLE_NAME']['S'], 'STATUS_RAW', 'IN PROGRESS', dynamo_table_name)
                update_attribute_value_dyndb('TARGET_TABLE_NAME', item['TARGET_TABLE_NAME']['S'], 'STATUS_STAGE', 'IN PROGRESS', dynamo_table_name)

                bd_type_name = credentials[end_point_name]
                bd_type_aux = [x for x in bd_tables if bd_type_name in x.keys()]
                if len(bd_type_aux) == 0:
                    bd_tables.append({bd_type_name: [end_point_name]})
                    input_table_names.append([[table_name]])
                    bd_types.append({"name": bd_type_name, "needs_dms": False})
                else:
                    end_point_aux = [x for x in bd_tables if bd_type_name in x.keys() and end_point_name in x[bd_type_name]]
                    indice = bd_tables.index(bd_type_aux[0])
                    if len(end_point_aux) == 0:
                        input_table_names[indice].append([table_name])
                        bd_tables[indice][bd_type_name].append(end_point_name)
                    else:
                        input_table_names[indice][bd_tables[indice][bd_type_name].index(end_point_name)].append(table_name)
                        
                if item.get('COLUMNS', {'S':''})['S'] == '' and item.get('FILTER_COLUMN', {'S':''})['S'] == '' and item.get('FILTER_EXP', {'S':''})['S'] == '' and item.get('ID_COLUMN', {'S':''})['S'] == '' and item.get('JOIN_EXPR', {'S':''})['S'] == '':
                    for endpoint_load in bd_types:
                        if endpoint_load['name'] == bd_type_name and not endpoint_load['needs_dms']:
                            endpoint_load['needs_dms'] = True
                logger.info(f"succeessfully added")

        except Exception as e:
            logger.error(f"error with table {e}")
    return (input_table_names, bd_types, end_point_name)


def lambda_handler(event, context):
    try:
        table_name = os.getenv('DYNAMO_DB_TABLE').strip()
        table_credentials = os.getenv('DYNAMO_CREDENTIALS_TABLE').strip()
        state_machine_arn = os.getenv('STATE_MACHINE_ARN').strip()
        replication_instance_class = os.getenv('REPLICATION_INSTANCE_CLASS').strip()
        process_ids_to_load = event['PROCESS_ID_TO_LOAD'].split(',')
        countries_to_load = event['COUNTRIES_TO_LOAD'].split(',')
        output = {}
        for process in process_ids_to_load:
            input_tables_names, bd_type_name, endpoint = find_Table_Names(table_name, table_credentials, process, countries_to_load)
            events = []
            for i in input_tables_names:
                json_string = {"replication_instance_class": replication_instance_class,
                               "table_names": i,
                               "bd_type": bd_type_name[input_tables_names.index(i)]['name'].upper(),
                               "dms_creation_try": 0,
                               "needs_dms": bd_type_name[input_tables_names.index(i)]['needs_dms'],
                               "process" : process
                               }
                events.append(json_string)
                json_array = json.dumps(json_string)
                logger.info("json_array:{}".format(json_array))
                start_cdc_execution(json_array, state_machine_arn)
                time.sleep(10)
                
            output[process] = events
                
        return {"result": "SUCCEEDED",
                "events": output
                }

    except Exception as e:
        logger.error("Exception thrown: %s" % str(e))
        return {"result": "FAILED"}
    