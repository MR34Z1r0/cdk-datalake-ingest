import datetime as dt
import logging
import os
import sys
import time

import boto3
import pytz
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from py4j.protocol import Py4JJavaError
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, expr, lpad
from pyspark.sql.functions import col, date_add, to_date, concat_ws, when, regexp_extract, trim, to_timestamp, lit
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *

TZ_LIMA = pytz.timezone('America/Lima')
YEARS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%Y')
MONTHS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%m')
DATE_LIMA = dt.datetime.now(TZ_LIMA).strftime('%Y%m%d-%H%M%S')
NOW_LIMA = dt.datetime.now(pytz.utc).astimezone(TZ_LIMA)
datetime_lima = dt.datetime.now(TZ_LIMA)
today = datetime_lima.strftime('/%Y/%m/%d/')

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("IngestionRawSalesforce")
logger.setLevel(os.environ.get("LOGGING", logging.DEBUG))

args = getResolvedOptions(
    sys.argv, ['LOAD_TYPE', 'JOB_NAME', 'BUCKET_RAW_NAME', 'BUCKET_STAGE_NAME', 'SALESFORCE_S3_PREFIX', 'TABLE_NAME', 'DYNAMO_CONFIG_TABLE', 'DYNAMO_LOGS_TABLE', 'DYNAMO_STAGE_COLUMNS', 'MAIL_LOG', 'ARN_TOPIC_FAILED'])

job_name = args['JOB_NAME']
bucket_raw_name = args['BUCKET_RAW_NAME']
bucket_stage_name = args['BUCKET_STAGE_NAME']
prefix = args['SALESFORCE_S3_PREFIX']
table_name = args['TABLE_NAME']
load_type = args['LOAD_TYPE']
log_mail = args['MAIL_LOG']

dynamodb_client = boto3.client('dynamodb')
client_glue = boto3.client('glue')
s3 = boto3.resource('s3')
client_sns = boto3.client("sns")
raw_bucket = s3.Bucket(bucket_raw_name)

dynamodb = boto3.resource('dynamodb')
dynamo_config_table = args['DYNAMO_CONFIG_TABLE'].strip()
dynamo_logs_table = args['DYNAMO_LOGS_TABLE'].strip()
dynamo_columns_table = args['DYNAMO_STAGE_COLUMNS'].strip()


spark = SparkSession \
    .builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED") \
    .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .getOrCreate()


class NoDataToMigrateException(Exception):
    def __init__(self):
        self.msg = "no data detected to migrate"

    def __str__(self):
        return repr(self.msg)

class column_transformation_controller():
    def __init__(self):
        self.columns = []
        self.msg = f"can not create the columns: "

    def add_table(self, table_name):
        self.columns.append(table_name)
        
    def get_msg(self):
        return  self.msg + ','.join(self.columns)
        
    def is_empty(self):
        return len(self.columns) == 0

def add_log_to_dynamodb(table_name, record):
    dynamodb = boto3.resource('dynamodb')
    dynamo_table = dynamodb.Table(table_name)
    dynamo_table.put_item(Item=record)

def send_error_message(topic_arn, table_name, error):
    client = boto3.client("sns")
    if "no data detected to migrate" in error:
        message = f"RAW WARNING in table: {table_name} \n{error}"
    else:
        message = f"Failed table: {table_name} \nStep: stage job \nLog ERROR \n{error}"
    response = client.publish(
        TopicArn=topic_arn,
        Message=message
    )

def read_landing_csv(table, s3_landing_path):
    try:
        landing_df = spark.read.csv(s3_landing_path)
        columns_timestamp = [
            f.name for f in landing_df.schema.fields if isinstance(f.dataType, TimestampType)]
        for column_timestamp in columns_timestamp:
            landing_df = landing_df.withColumn(
                column_timestamp, landing_df[column_timestamp] - expr('INTERVAL 5 HOURS'))
        return landing_df
    except Exception as e:
        logger.error(
            f'The error was captured: {e}, we proceed to create an empty dataframe table: {table}.')
        landing_df = spark.createDataFrame([], StructType([]))
        return landing_df

def get_appflow_name(object_name, load_type):
    if load_type == 'UPSERT_LOAD':
        object_name_final = object_name.lower() + 'hr'
    else:
        object_name_final = object_name.lower() + 'da'
    return f"ajedtlk-{object_name_final}"

def update_attribute_value_dynamodb(row_key_field_name, row_key, attribute_name, attribute_value, table_name):
    logger.info('update dynamoDb Metadata : {} ,{},{},{},{}'.format(
        row_key_field_name, row_key, attribute_name, attribute_value, table_name))
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


def transform_df(raw_df, function_name, parameters, column_name, data_type):
    function_name = function_name.strip()
    logger.info(f"adding column: {column_name}")
    if ')' in parameters:
        parameters = column_name + parameters[parameters.rfind(")") + 1:]

    if function_name == 'fn_transform_Concatenate':
        columns_to_concatenate = [col_name.strip() for col_name in parameters.split(',')]
        return raw_df.withColumn(column_name, concat_ws("-", *[col(col_name) for col_name in columns_to_concatenate]).cast(data_type))
    
    elif function_name == 'fn_transform_Concatenate_ws':
        params = parameters.split(',')
        columns_to_concatenate = [col_name.strip() for col_name in params[:-1]]
        return raw_df.withColumn(column_name, concat_ws(params[-1], *[col(col_name) for col_name in columns_to_concatenate]).cast(data_type))
    
    elif function_name == 'fn_transform_ByteMagic':
        params = parameters.split(',')
        origin_column = params[0]
        default = params[1]
        columns_to_concatenate = [col_name.strip() for col_name in params[:-1]]
        return raw_df.withColumn(column_name, when(col(origin_column) == "b'T'", 'T').when(col(origin_column) == "b'F'", 'F').otherwise(default).cast(data_type))
        
    elif function_name == 'fn_transform_Case':
        list_params = parameters.split(',') 
        origin_column = list_params[0]
        rules = list_params[1:]
        for ele_case in rules:
            value_case = ele_case.split('->')[0]
            label_case = ele_case.split('->')[1]
            values_to_change = value_case.split('|')
            raw_df = raw_df.withColumn(column_name, when(col(origin_column).isin(values_to_change), label_case).cast(data_type))
        return raw_df
        
    elif function_name == 'fn_transform_Case_with_default':
        list_params = parameters.split(',') 
        origin_column = list_params[0]
        rules = list_params[1:-1]
        default = list_params[-1]
        total_changes = []
        for ele_case in rules:
            value_case = ele_case.split('->')[0]
            label_case = ele_case.split('->')[1]
            values_to_change = value_case.split('|')
            total_changes.extend(values_to_change)
            raw_df = raw_df.withColumn(column_name, when(col(origin_column).isin(values_to_change), label_case).cast(data_type))  
        raw_df = raw_df.withColumn(column_name, when(~col(origin_column).isin(total_changes), col(default)).cast(data_type))
        return raw_df
    
    elif function_name == 'fn_transform_Datetime':
        list_params = parameters.split(',') 
        origin_column = list_params[0]
        if list_params[0] == '':
            raw_df = raw_df.withColumn(column_name, from_utc_timestamp(current_timestamp(), "America/Lima").cast(data_type))
        else:
            raw_df = raw_df.withColumn(column_name, to_timestamp(origin_column).cast(data_type))
        return raw_df  
    
    # pending review 
    elif function_name == 'fn_transform_ClearDouble':
        params = parameters.split(',')
        columns_to_concatenate = [col_name.strip() for col_name in params[:-1]]
        return raw_df.withColumn(column_name, concat_ws(params[-1], *[col(col_name) for col_name in columns_to_concatenate]).cast(data_type))
        
    elif function_name == 'fn_transform_ClearString':
        return raw_df.withColumn(column_name, trim(col(parameters)).cast(data_type))

    elif function_name == 'fn_transform_Date_to_String':
        params = parameters.split(',')
        return raw_df.withColumn(column_name, date_format(col(params[0]), params[1]).cast(data_type))
    
    elif function_name == 'fn_transform_DateMagic':
        base_date = "1900-01-01"
        params = parameters.split(',')
        value_default = params[-1]
        date_format = params[1]
        origin_column = params[0]
        date_pattern = r'^([7-9]\d{5}|[1-2]\d{6}|3[0-5]\d{5})$'
        return raw_df.withColumn(
            column_name,
            when(
                regexp_extract(col(origin_column).cast(StringType()), date_pattern, 1) != "",
                to_date(date_add(to_date(lit(base_date)), col(origin_column).cast(IntegerType()) - lit(693596)))
            ).otherwise(
                to_date(lit(value_default), date_format)
            ).cast(data_type)
        )
        
    elif function_name == 'fn_transform_DatetimeMagic':
        base_date = "1900-01-01"
        params = parameters.split(',')
        value_default = params[-1]
        datetime_format = params[2]
        origin_column_date = params[0]
        origin_column_time = params[1]
        date_pattern = r'^([7-9]\d{5}|[1-2]\d{6}|3[0-5]\d{5})$'
        time_pattern = r'^([01][0-9]|2[0-3])([0-5][0-9])([0-5][0-9])$'
        return raw_df.withColumn(
            column_name,
            when(
                regexp_extract(col(origin_column_date).cast(StringType()), date_pattern, 1) != "",
                when(
                    regexp_extract(col(origin_column_time).cast(StringType()), time_pattern, 1) != "",
                    to_timestamp(
                        concat_ws(" ", 
                            to_date(date_add(to_date(lit(base_date)), col(origin_column_date).cast(IntegerType()) - lit(693596))),
                            concat_ws(":", col(origin_column_time).substr(1, 2), col(origin_column_time).substr(3, 2), col(origin_column_time).substr(5, 2))
                        ),  datetime_format
                    )
                )
                .otherwise(
                    to_timestamp(date_add(to_date(lit(base_date)), col(origin_column_date).cast(IntegerType()) - lit(693596)), datetime_format[:8])
                )
            )
            .otherwise(
                to_timestamp(lit(value_default), datetime_format[:8])
            ).cast(data_type)
        )

    
    elif function_name == 'fn_transform_PeriodMagic':
        params = parameters.split(',')
        period_value = params[0]
        ejercicio_value = params[1]
        return raw_df.withColumn(
            column_name,
            when(
                (col(period_value).isNull()),
                '190001'
            ).otherwise(
                concat(period_value, lpad(ejercicio_value, 2, '0'))
            ).cast(data_type)
        )
    
    else:
        return raw_df
        
def split_function(query):
    if '(' in query:
        start_parameter = query.find("(")
        end_parameter = query.rfind(")")
        function_name = query[:start_parameter]
        functions.append(function_name)
        parameters = query[int(start_parameter) + 1 : int(end_parameter)]
        all_parameters.append(parameters)
        split_function(parameters)
    else:
        return query


# Start of the logic
    
columns_array = dynamodb_client.query(
    TableName=dynamo_columns_table,
    KeyConditionExpression='TARGET_TABLE_NAME  = :partition_key',
    ExpressionAttributeValues={
        ':partition_key': {'S': table_name}
    }
)
config_table_metadata = dynamodb.Table(dynamo_config_table)
table_to_ingest = table_name.strip()
table_data = config_table_metadata.get_item(Key={'TARGET_TABLE_NAME': table_to_ingest})['Item']
table_name_raw = table_data['SOURCE_TABLE']
table_name_stage = table_data['STAGE_TABLE_NAME']
table = get_appflow_name(table_name_raw,load_type)

# Conversion de csv a csv.gz

try:
    s3_raw_path = f"s3a://{bucket_raw_name}/{prefix}/{table_name_raw}{today}"
    s3_stage_path = f"s3a://{bucket_stage_name}/{prefix}/{table_name_stage}/"
    try:
        raw_df = spark.read.format("csv").option("compression", "gzip").option("header", True).load(s3_raw_path)
        if raw_df.count() == 0:
            raise NoDataToMigrateException()
    except Exception as e:
        logger.error(e)
        raise NoDataToMigrateException()
    else:
        id_columns = ''
        columns_order = []
        order_by_columns = []
        columns_controller = column_transformation_controller()
        for column in columns_array['Items']:
            try:
                logger.info(f"column : {column['COLUMN_NAME']['S']}")
                if column.get('IS_ORDER_BY', "").__class__ == dict:
                    if column['IS_ORDER_BY']['BOOL']:
                        order_by_columns.append(column['COLUMN_NAME']['S'])
                if column.get('IS_ID', "").__class__ == dict:
                    if column['IS_ID']['BOOL']:
                        id_columns += column['COLUMN_NAME']['S'] + ','
               
                functions = []
                all_parameters = [] 
                query_string = column['TRANSFORMATION']['S']
                
                if query_string.count("(") == query_string.count(")"):
                    query = split_function(query_string)
                    logger.info(f"funciont : {functions}")
                    logger.info(f"parameters : {all_parameters}")
                else:
                    raise Exception("query transformation error with column ", column['COLUMN_NAME']['S'])
                if query is not None:
                    raw_df = raw_df.withColumn(column['COLUMN_NAME']['S'].strip(),expr(column['TRANSFORMATION']['S'].strip()).cast(column['NEW_DATA_TYPE']['S']))
                else:
                    for i in range(len(functions)-1,-1, -1):
                        raw_df = transform_df(raw_df, functions[i], all_parameters[i], column['COLUMN_NAME']['S'].strip(), column['NEW_DATA_TYPE']['S'])
                    
                columns_order.append({'name' : column['COLUMN_NAME']['S'], 'column_id' : int(column['COLUMN_ID']['N'])})
                raw_df.show()
            except Exception as e:
                columns_controller.add_table(column['COLUMN_NAME']['S'])
                log = {
                    'PROCESS_ID': f"DLB_{table_name.split('_')[0]}_{table_data['ENDPOINT']}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
                    'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
                    'PROJECT_NAME': 'athenea',
                    'FLOW_NAME': 'extract_salesforce',
                    'TASK_NAME': 'extract_table_raw',
                    'TASK_STATUS': 'error',
                    'MESSAGE': f"No data detected to migrate. Details are: {e}",
                    'PROCESS_TYPE': 'D',
                    'CONTEXT': f"{{server='www.salesforce.com', user='{log_mail}', table_name='{table_data['TARGET_TABLE_NAME']}', service_name='{table_data['SOURCE_TABLE']}'}}"
                }
                add_log_to_dynamodb(dynamo_logs_table, log)
                logger.error(e)
                
        if columns_controller.is_empty():
            update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_STAGE', 'SUCCEDED', dynamo_config_table)
            update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'FAIL REASON', f"", dynamo_config_table)
        else:
            logger.info(f"there´re problems with columns  : {columns_controller.is_empty()}")
            update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_STAGE', 'WARNING', dynamo_config_table)
            update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'FAIL REASON', f"STAGE: {columns_controller.get_msg()}", dynamo_config_table)

        new_colums = [item['name'] for item in sorted(columns_order, key=lambda x: x['column_id'])]
        logger.info(new_colums)
        logger.info(f"new columns to add: {new_colums}")
        logger.info(f"oreder by columns: {order_by_columns}")
        raw_df = raw_df.select(*new_colums).orderBy(order_by_columns)
        raw_df.show()
        logger.info(f"total rows: {raw_df.count()}")

        max_tries = 3
        actual_try = 0
        if DeltaTable.isDeltaTable(spark, s3_stage_path):
            while actual_try != max_tries:
                try:
                    if DeltaTable.isDeltaTable(spark, s3_stage_path) and not(raw_df.first() == None) and load_type != 'SNAPSHOT_LOAD':
                        raw_df = raw_df.orderBy("Id", ascending=False)  # Sort DataFrame by "Id" column in descending order
                        raw_df = raw_df.dropDuplicates(['Id'])
                        stage_dt = DeltaTable.forPath(spark, s3_stage_path)
                        stage_dt.alias("old") \
                            .merge(raw_df.alias("new"), "old.Id = new.Id") \
                            .whenMatchedUpdateAll().whenNotMatchedInsertAll() \
                            .execute()
                        deltaTable = DeltaTable.forPath(spark, s3_stage_path)
                        deltaTable.vacuum(100)
                    elif raw_df.first() == None:
                        logger.debug(f'{table} doesn’t have any records: {today}')
                    else:
                        #assume it is full load, only reason why there is no delta table on stage
                        raw_df.write.format("delta").mode("overwrite").save(s3_stage_path)
                    break
                except Exception as e:
                    actual_try += 1
                    time.sleep(actual_try * 60)
                    if actual_try == max_tries:
                        logger.error(e)
                        raise Exception(e)
        else:
            # create delta table on stage
            raw_df.write.format("delta").mode("overwrite").save(s3_stage_path)

        deltaTable = DeltaTable.forPath(spark, s3_stage_path)
        deltaTable.vacuum(100)
        deltaTable.generate("symlink_format_manifest")
        columns_controller.add_table(column['COLUMN_NAME']['S'])
        log = {
            'PROCESS_ID': f"DLB_{table_name.split('_')[0]}_{table_data['ENDPOINT']}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
            'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
            'PROJECT_NAME': 'athenea',
            'FLOW_NAME': 'extract_salesforce',
            'TASK_NAME': 'extract_table_stage',
            'TASK_STATUS': 'satisfactorio',
            'MESSAGE': '',
            'PROCESS_TYPE': 'D',
            'CONTEXT': f"{{server='www.salesforce.com', user='{log_mail}', table_name='{table_data['TARGET_TABLE_NAME']}', service_name='{table_data['SOURCE_TABLE']}'}}"
        }
        add_log_to_dynamodb(dynamo_logs_table, log)
        update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_STAGE', 'SUCCEDED', dynamo_config_table)
        update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'FAIL REASON', f"", dynamo_config_table)
        logger.debug("Data ingestada a stage correctamente")
except NoDataToMigrateException as e:
    update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_STAGE', 'WARNING', dynamo_config_table)
    update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_RAW', 'WARNING', dynamo_config_table)
    update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'FAIL REASON', f"STAGE: {e}", dynamo_config_table)
    log = {
        'PROCESS_ID': f"DLB_{table_name.split('_')[0]}_{table_data['ENDPOINT']}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
        'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
        'PROJECT_NAME': 'athenea',
        'FLOW_NAME': 'extract_salesforce',
        'TASK_NAME': 'extract_table_stage',
        'TASK_STATUS': 'error',
        'MESSAGE': f"No data detected to migrate. Details are: {e}",
        'PROCESS_TYPE': 'D',
        'CONTEXT': f"{{server='www.salesforce.com', user='{log_mail}', table_name='{table_data['TARGET_TABLE_NAME']}', service_name='{table_data['SOURCE_TABLE']}'}}"
    }
    add_log_to_dynamodb(dynamo_logs_table, log)
    send_error_message(args['ARN_TOPIC_FAILED'], table_data['TARGET_TABLE_NAME'], str(e))
    logger.error(e)
    logger.error("No data to import")

except Exception as e:
    update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_STAGE', 'FAILED', dynamo_config_table)
    update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'FAIL REASON', f"STAGE: {e}", dynamo_config_table)
    log = {
        'PROCESS_ID': f"DLB_{table_name.split('_')[0]}_{table_data['ENDPOINT']}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
        'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
        'PROJECT_NAME': 'athenea',
        'FLOW_NAME': 'extract_salesforce',
        'TASK_NAME': 'extract_table_stage',
        'TASK_STATUS': 'error',
        'MESSAGE': f"No data detected to migrate. Details are: {e}",
        'PROCESS_TYPE': 'D',
        'CONTEXT': f"{{server='www.salesforce.com', user='{log_mail}', table_name='{table_data['TARGET_TABLE_NAME']}', service_name='{table_data['SOURCE_TABLE']}'}}"
    }
    add_log_to_dynamodb(dynamo_logs_table, log)
    send_error_message(args['ARN_TOPIC_FAILED'], table_data['TARGET_TABLE_NAME'], str(e))
    logger.error(e)
    logger.error("Error while importing data")