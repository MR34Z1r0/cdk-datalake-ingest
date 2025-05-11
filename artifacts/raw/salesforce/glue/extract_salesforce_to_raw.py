import sys
import pytz
import boto3
import datetime as dt
import time
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
import os
import logging

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
    sys.argv, ['LOAD_TYPE','JOB_NAME', 'BUCKET_LANDING_NAME', 'BUCKET_RAW_NAME', 'SALESFORCE_S3_PREFIX', 'TABLE_NAME', 'DYNAMO_CONFIG_TABLE', 'DYNAMO_LOGS_TABLE', 'MAIL_LOG', 'ARN_TOPIC_FAILED'])

job_name = args['JOB_NAME']
bucket_landing_name = args['BUCKET_LANDING_NAME']
bucket_raw_name = args['BUCKET_RAW_NAME']
prefix = args['SALESFORCE_S3_PREFIX']
load_type = args['LOAD_TYPE']
log_mail = args['MAIL_LOG']

client_glue = boto3.client('glue')
s3 = boto3.resource('s3')
client_sns = boto3.client("sns")
landing_bucket = s3.Bucket(bucket_landing_name)

dynamodb = boto3.resource('dynamodb')
dynamo_config_table = args['DYNAMO_CONFIG_TABLE'].strip()
dynamo_logs_table = args['DYNAMO_LOGS_TABLE'].strip()


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


def refactor_key_s3(tables, prefix, today):
    for table in tables:
        # appflow creates a prefix with the following format (when data are partitioned): de14ca74-8299-3690-ab80-ad04879e29de-2022-11-17T22:28:54
        today_verify = datetime_lima.strftime('%Y-%m-%dT')
        try:
            # Here we list all the path of the csv files
            csv_prefix_files = [obj.key for obj in landing_bucket.objects.filter(
                Prefix=prefix + '/' + table + today)]
            if today_verify in csv_prefix_files[0].rsplit('/', 1)[0]:
                for csv_prefix_file in csv_prefix_files:
                    copy_source = {'Bucket': bucket_landing_name,
                                   'Key': csv_prefix_file}
                    csv_file = csv_prefix_file.rsplit('/', 1)[1]
                    # Copies all parquet files to a previous directory: /2022/11/17/files.csv
                    new_table = table.rsplit('-', 1)[1]
                    new_table = new_table[:-2]  #Deleting hr or da

                    landing_bucket.copy(copy_source,
                                        prefix + '/' + new_table + today + csv_file)
                # removes the prefix created by default by appflow (when data are partitioned)
                landing_bucket.objects.filter(
                    Prefix=csv_prefix_files[0].rsplit('/', 1)[0]).delete()
            else:
                logger.debug(
                    f'Does not contain prefix of appflow format or already moved csv files in table: {table}.')
        except Exception as e:
            logger.error(e)


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

def get_appflow_name(object_name):
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

# Start of the logic

config_table_metadata = dynamodb.Table(dynamo_config_table)
table_to_ingest = args['TABLE_NAME'].strip()
table_data = config_table_metadata.get_item(Key={'TARGET_TABLE_NAME': table_to_ingest})['Item']
table_name_raw = table_data['SOURCE_TABLE']
tables = []
tables.append(get_appflow_name(table_name_raw))

refactor_key_s3(tables, prefix, today)

# Conversion de csv a csv.gz

for table in tables:
    new_table = table.rsplit('-', 1)[1]
    new_table = new_table[:-2]
    s3_landing_path = f"s3a://{bucket_landing_name}/{prefix}/{new_table}{today}"
    s3_raw_path = f"s3a://{bucket_raw_name}/{prefix}/{new_table}{today}"

    try:
        landing_df = read_landing_csv(new_table, s3_landing_path)

        if landing_df.isEmpty():
            logger.info("Empty DataFrame. Finishing the job without loading data to s3 - Landing/Raw. Today there isnt data from the object.")
        else:
            # Check if CSV.gz files already exist in the raw bucket (aqui falta agregar los dias)
            existing_files = [obj.key for obj in s3.Bucket(bucket_raw_name).objects.filter(
                Prefix=f"{prefix}/{new_table}{today}") if obj.key.endswith(".gz")]

            if existing_files:
                # Overwrite the existing data with the new data
                landing_df.write.option("compression", "gzip").csv(
                    s3_raw_path, mode="overwrite")
            else:
                # If no CSV.gz files exist, assume it's a full load
                landing_df.write.option("compression", "gzip").csv(s3_raw_path)

            logger.info("Data successfully loaded to s3 - Landing/Raw")

        update_attribute_value_dynamodb(
            'TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_RAW', 'SUCCEDED', dynamo_config_table)
        update_attribute_value_dynamodb(
            'TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'FAIL REASON', f"", dynamo_config_table)

    except Exception as e:
        update_attribute_value_dynamodb(
        'TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_RAW', 'FAILED', dynamo_config_table)
        update_attribute_value_dynamodb(
            'TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_STAGE', 'FAILED', dynamo_config_table)
        update_attribute_value_dynamodb(
            'TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'FAIL REASON', f"Can not connect to BD: {e}", dynamo_config_table)
        log = {
            'PROCESS_ID': f"DLB_{table_data['TARGET_TABLE_NAME'].split('_')[0]}_{table_data['SOURCE_TABLE']}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
            'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
            'PROJECT_NAME': 'athenea',
            'FLOW_NAME': 'extract_salesforce',
            'TASK_NAME': 'extract_table_raw',
            'TASK_STATUS': 'error',
            'MESSAGE': f"No data detected to migrate. Details are: {e}",
            'PROCESS_TYPE': 'D' if table_data['FILTER_OPERATOR'].strip() in ['between-date'] else 'F',
            'CONTEXT': f"{{server='www.salesforce.com', user='{log_mail}', table_name='{table_data['SOURCE_TABLE']}', service_name='{table_data['COLUMN_NAME']}'}}"
        }
        add_log_to_dynamodb(dynamo_logs_table, log)
        send_error_message(args['ARN_TOPIC_FAILED'], table_data['TARGET_TABLE_NAME'], str(e))
        logger.error(e)
        raise Exception("Exception raised: {}".format(e))