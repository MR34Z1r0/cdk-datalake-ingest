# this script change the active flag to 'N' (means dont import), but no the specified endpoints

import time

# requires to install boto3 to the environment
import boto3

# the dynamo tale to update
dynamodb_table_name = 'deltaLoads-IEducadevrawConfigurationDynamoTableCC1F102C-1SE3KCSLRI6PY'
# array with the endpoint names to be exported, this names should exist in the table credentials
allow_endpoint = ['BANNER']


def update_attribute_value_dynamodb(row_key_field_name, row_key, attribute_name, attribute_value, table_name):
    print('update dynamoDb Metadata : {} ,{},{},{},{}'.format(row_key_field_name, row_key, attribute_name, attribute_value, table_name))
    dynamodb = boto3.resource('dynamodb')
    dynamo_table = dynamodb.Table(table_name)
    dynamo_table.update_item(
        Key={row_key_field_name: row_key},
        AttributeUpdates={
            attribute_name: {
                'Value': attribute_value,
                'Action': 'PUT'
            }
        }
    )


def get_crawlers(dynamodb_table_name):
    dynamodb = boto3.client('dynamodb')
    response = dynamodb.scan(TableName=dynamodb_table_name)
    items = response['Items']
    return items


# here start the update, dont modify
response = get_crawlers(dynamodb_table_name)
for items in response:
    key = items['TARGET_TABLE_NAME']['S']
    active_flag = items['ACTIVE_FLAG']['S']
    endpoint = items['ENDPOINT']['S']
    if active_flag == 'Y' and not endpoint in allow_endpoint:
        update_attribute_value_dynamodb('TARGET_TABLE_NAME', key, 'ACTIVE_FLAG', 'N', dynamodb_table_name)
        time.sleep(1)
