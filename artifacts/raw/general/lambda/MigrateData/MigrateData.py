import datetime
import logging
import os
import time

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def send_error_message(topic_arn, table_name, error):
    client = boto3.client("sns")
    response = client.publish(
        TopicArn=topic_arn,
        Message=f"Failed table: {table_name}\nStep: Migrate Data \nLog ERROR \n{error}"
    )


def add_time_stamp_to_file_name(original_filename_without_path):
    file_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S%f")
    file_extension = os.path.splitext(original_filename_without_path)[1]
    new_file_name = file_time + file_extension
    return new_file_name


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


def read_table_meta_data(table_name, table_key, key_value):
    dynamodb = boto3.resource('dynamodb')
    dynamo_table = dynamodb.Table(table_name)
    response = dynamo_table.get_item(Key={table_key: key_value})
    if 'Item' in response:
        logger.debug("item found")
        item = response['Item']
        return item
    else:
        logger.warning('Metadata Not Found')


def get_objects_in_bucket(source_path):
    origin_bucket_name = os.getenv('S3_BUCKET_SOURCE')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(origin_bucket_name)
    return bucket.objects.filter(Prefix=source_path)


def transform_date(time):
    if len(time) == 1:
        time = "0" + time
    return time


def get_input_outout_path(table_name, credentials_table_name, target_table_name, project_name):
    today = datetime.date.today()
    year = str(today.year)
    month = transform_date(str(today.month))
    day = transform_date(str(today.day))
    item = read_table_meta_data(table_name, 'TARGET_TABLE_NAME', target_table_name)
    credentials = read_table_meta_data(credentials_table_name, 'ENDPOINT_NAME', item['ENDPOINT'])
    source_schema = str(item['SOURCE_SCHEMA'])
    source_table = str(item['SOURCE_TABLE'])
    if credentials['BD_TYPE'] == 'mssql':
        bd_type = 'sqlserver'
    else:
        bd_type = credentials['BD_TYPE']
    endpoint_name = credentials['ENDPOINT_NAME']
    source_path = "temp/" + bd_type + '/' + source_schema + '/'
    target_path = bd_type + '/' + endpoint_name + '/' + source_schema + '/' + target_table_name + '/' + year + '/' + month + '/' + day
    target_path = f"{project_name}/{bd_type}/{endpoint_name}/{source_table.split()[0]}/{year}/{month}/{day}/"
    for table in get_objects_in_bucket(source_path):
        if source_table.upper() == table.key.split("/")[3].upper():
            source_path += table.key.split("/")[3] + '/'
            return source_path, target_path
    return "", target_path


def lambda_handler(event, context):
    try:
        client = boto3.client('s3')
        project_name = os.getenv('PROJECT_NAME')
        table_name = os.getenv('DYNAMO_DB_TABLE').strip()
        table_credential = os.getenv('DYNAMO_CREDENTIALS_TABLE').strip()
        new_bucket_name = os.getenv('S3_BUCKET_TARGET').strip()
        origin_bucket_name = os.getenv('S3_BUCKET_SOURCE')
        keys = []
        dynamo_db_key = event['dynamodb_key']

        source_path, target_path = get_input_outout_path(table_name, table_credential, dynamo_db_key, project_name)

        # deleting the target objects if exists
        try:
            for key in get_objects_in_bucket(target_path):
                key.delete()
                logger.info(f"deleting object in target: {key}")
        except Exception as e:
            logger.error("error deleting the target path")

        logger.info('sourcePath: {}'.format(source_path))
        logger.info('targetPath: {}'.format(target_path))
        if source_path != '':
            objetcs = get_objects_in_bucket(source_path)
            for object in objetcs:
                keys.append(object)

            if keys == []:
                logger.warning("Exception thrown: when retriving content. No files to copy")
                update_attribute_value_dyndb('TARGET_TABLE_NAME', dynamo_db_key, 'STATUS_RAW', 'FAILED', table_name)
                update_attribute_value_dyndb('TARGET_TABLE_NAME', dynamo_db_key, 'FAIL REASON', f"no files to move to ", table_name)
                return {'result': 'FAILED', 'reason': 'nothing to move on'}
            for files in keys:
                logger.info('original filename with path: {}'.format(files.key))
                original_filename_without_path = files.key.split('/')[-1:][0]
                logger.info('original_filename_without_path: {}'.format(original_filename_without_path))
                new_file_name = add_time_stamp_to_file_name(original_filename_without_path)
                logger.info('newfilename: {}'.format(new_file_name))
                copy_source = {'Bucket': origin_bucket_name, 'Key': files.key}
                target_key = target_path + '/' + new_file_name
                logger.info('targetKey: {}'.format(target_key))
                client.copy(copy_source, new_bucket_name, target_key)

            if source_path != "":
                try:
                    for key in get_objects_in_bucket(source_path):
                        logger.info("deleting " + str(key))
                        key.delete()
                        update_attribute_value_dyndb('TARGET_TABLE_NAME', dynamo_db_key, 'STATUS_RAW', 'SUCCEDED', table_name)
                        update_attribute_value_dyndb('TARGET_TABLE_NAME', dynamo_db_key, 'FAIL REASON', f"", table_name)
                except Exception as e:
                    logger.error("can not delete: " + str(e))
        else:
            logger.info("the table data is already load")
        return {"result": "SUCCESS",
                "dynamodb_key": dynamo_db_key,
                'replication_instance_arn': event['replication_instance_arn'],
                "status": "Complete"}
    except Exception as e:
        logger.error("Exception: {}".format(e))
        result = {'result': 'FAILED', "dynamodb_key": dynamo_db_key, 'replication_instance_arn': event['replication_instance_arn']}
        update_attribute_value_dyndb('TARGET_TABLE_NAME', dynamo_db_key, 'STATUS_RAW', 'FAILED', table_name)
        update_attribute_value_dyndb('TARGET_TABLE_NAME', dynamo_db_key, 'FAIL REASON', str(e), table_name)
        topic_arn = os.getenv("TOPIC_ARN")
        send_error_message(topic_arn, dynamo_db_key, str(e))
        return result
