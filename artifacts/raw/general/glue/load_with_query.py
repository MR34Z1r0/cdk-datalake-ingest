import concurrent.futures as futures
import datetime as dt
import calendar
import json
import logging
import os
import sys
import time

import boto3
import pytz
from dateutil.relativedelta import relativedelta
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from py4j.protocol import Py4JJavaError
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.functions import to_date
from pyspark.sql.session import SparkSession

TZ_LIMA = pytz.timezone('America/Lima')
YEARS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%Y')
MONTHS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%m')
DAYS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%d')
NOW_LIMA = dt.datetime.now(pytz.utc).astimezone(TZ_LIMA)

# @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'S3_RAW_PREFIX', 'DYNAMO_CONFIG_TABLE', 'DYNAMO_ENDPOINT_TABLE', 'DYNAMO_LOGS_TABLE', 'TABLE_NAME', 'SECRETS_NAME', 'THREADS_FOR_INCREMENTAL_LOADS', 'TOPIC_ARN', 'SECRETS_REGION', 'PROJECT_NAME'])

spark = SparkSession \
    .builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")\
    .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .config("spark.driver.maxResultSize", "4g") \
    .config("spark.driver.memory", "16g") \
    .config("spark.executor.memory", "16g") \
    .config("spark.sql.shuffle.partitions", "1000") \
    .getOrCreate()

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("raw job")
logger.setLevel(os.environ.get("LOGGING", logging.INFO))

dynamodb = boto3.resource('dynamodb')
dynamo_config_table = args['DYNAMO_CONFIG_TABLE'].strip()
dynamo_endpoint_table = args['DYNAMO_ENDPOINT_TABLE'].strip()
dynamo_logs_table = args['DYNAMO_LOGS_TABLE'].strip()

secrets_region = args['SECRETS_REGION'].strip()

def send_error_message(topic_arn, table_name, error):
    client = boto3.client("sns")
    response = client.publish(
        TopicArn=topic_arn,
        Message=f"Failed table: {table_name} \nStep: raw job \nLog ERROR : {error}"
    )


def delete_from_target(bucket, s3_raw_path):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    bucket.objects.filter(Prefix=s3_raw_path).delete()


def transform_to_dt(date):
    start_dt = dt.datetime(
        year=int(date[:4]),
        month=int(date[5:7]),
        day=int(date[8:10]),
        hour=int(date[11:13]),
        minute=int(date[14:16]),
        second=int(date[17:19])
    )
    return start_dt

def get_limits_for_filter(month_diff, data_type):
    data_type = data_type.strip()
    upper_limit = dt.datetime.now(TZ_LIMA)
    lower_limit = upper_limit - relativedelta(months=(-1*int(month_diff)))
    
    if data_type == "aje_period":
        return lower_limit.strftime('%Y%m'), upper_limit.strftime('%Y%m')
    
    elif data_type == "aje_date":
        _, last_day = calendar.monthrange(upper_limit.year, upper_limit.month)
        upper_limit = upper_limit.replace(day=last_day, tzinfo=TZ_LIMA)
        lower_limit = lower_limit.replace(day=1, tzinfo=TZ_LIMA)
        upper_limit = (upper_limit - dt.datetime(1900, 1, 1, tzinfo=TZ_LIMA)).days + 693596
        lower_limit = (lower_limit - dt.datetime(1900, 1, 1, tzinfo=TZ_LIMA)).days + 693596
        return str(lower_limit), str(upper_limit)
        
    elif data_type == "aje_processperiod":
        _, last_day = calendar.monthrange(upper_limit.year, upper_limit.month)
        upper_limit = upper_limit.replace(day=last_day, tzinfo=TZ_LIMA)
        lower_limit = lower_limit.replace(day=1, tzinfo=TZ_LIMA)
        upper_limit = (upper_limit - dt.datetime(1900, 1, 1, tzinfo=TZ_LIMA)).days + 693596
        lower_limit = (lower_limit - dt.datetime(1900, 1, 1, tzinfo=TZ_LIMA)).days + 693596
        return str(lower_limit), str(upper_limit)
  
    return lower_limit.strftime('%Y%m'), upper_limit.strftime('%Y%m')

def get_secret(secret_name, target_secret, secrets_region):
    region_name = secrets_region[:-1]
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )

    except Exception as e:
        logger.error("Exception thrown: %s" % str(e))
    else:
        if 'SecretString' in get_secret_value_response:
            text_secret_data = get_secret_value_response['SecretString']
        else:
            text_secret_data = get_secret_value_response['SecretBinary']

        secret_json = json.loads(text_secret_data)
        return secret_json[target_secret]


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

def add_log_to_dynamodb(table_name, record):
    dynamodb = boto3.resource('dynamodb')
    dynamo_table = dynamodb.Table(table_name)
    dynamo_table.put_item(Item=record)

def get_data(url, user, password, driver, query, s3_raw_path, actual_thread, number_threads, secrets_region):
    try:
        logger.info(query)
        jdbcDF = spark.read.format("jdbc") \
            .option("url", url) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", driver) \
            .option("numPartitions", 100) \
            .option("fetchsize", 1000) \
            .option("query", query)

        if endpoint_data['BD_TYPE'] == 'oracle':
            jdbcDF = jdbcDF.option("sessionInitStatement", f"ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'") \
                .option("oracle.jdbc.mapDateToTimestamp", "true")

        jdbcDF = jdbcDF.load()
        jdbcDF = jdbcDF.dropDuplicates()
        jdbcDF.write.mode("append").option("header", True).csv(s3_raw_path, compression = 'gzip')
        logger.info(f"finished thread n: {actual_thread}")
        jdbcDF.show()
    except Exception as e:
        logger.error(e)
        raise Exception(e)


def get_query(start, end):
    query = table_data['QUERY_BY_GLUE']
    if 'FILTER_TYPE' in table_data.keys():
        start, end = change_date_format(start, end, table_data['FILTER_TYPE'])
        logger.debug(f"Start Date: {start}")
        logger.debug(f"End Date: {end}")

    if ',' in table_data['FILTER_COLUMN']:
        filter_columns = table_data['FILTER_COLUMN'].split(",")
        first_filter = filter_columns[0]
        last_filter = filter_columns[1]

        query += f" WHERE ({first_filter} IS NOT NULL and {first_filter} BETWEEN {start} AND {end}) OR ({last_filter} IS NOT NULL and {last_filter} BETWEEN {start} AND {end})"
    else:
        first_filter = table_data['FILTER_COLUMN']
        query += f" WHERE {first_filter} is not null and {first_filter} BETWEEN {start} AND {end}"
    logger.info(query)
    return(query)


def change_date_format(start, end, date_type):
    if date_type == 'smalldatetime':
        date_format = f"CONVERT(smalldatetime, 'date_to_replace', 120)"

    elif date_type == 'DATE':
        date_format = f"TO_DATE('date_to_replace', 'YYYY-MM-DD HH24:MI:SS')"
        end = end[:19]
        start = start[:19]

    elif date_type == 'TIMESTAMP(6)':
        date_format = f"TO_TIMESTAMP('date_to_replace', 'YYYY-MM-DD HH24:MI:SS.FF')"

    elif date_type == 'SQL_DATETIME':
        date_format = f"CONVERT(DATETIME, 'date_to_replace',  102)"

    elif date_type == 'BIGINT':
        end = dt.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
        end = int(end.timestamp())
        start = dt.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        start = int(start.timestamp())
        date_format = "date_to_replace"

    end = date_format.replace("date_to_replace", str(end))
    start = date_format.replace("date_to_replace", str(start))
    return start, end


try:
    config_table_metadata = dynamodb.Table(dynamo_config_table)
    endpoint_table_metadata = dynamodb.Table(dynamo_endpoint_table)
    s3_source = args['S3_RAW_PREFIX'].strip()
    table_name = args['TABLE_NAME'].strip()
    table_data = config_table_metadata.get_item(Key={'TARGET_TABLE_NAME': table_name})['Item']
    endpoint_data = endpoint_table_metadata.get_item(Key={'ENDPOINT_NAME': table_data['ENDPOINT']})['Item']
    if endpoint_data['BD_TYPE'] == 'mssql':
        bd_type = 'sqlserver'
    else:
        bd_type = endpoint_data['BD_TYPE']
    day_route = f"{args['PROJECT_NAME']}/{bd_type}/{table_data['ENDPOINT']}/{table_data['SOURCE_TABLE'].split()[0]}/{YEARS_LIMA}/{MONTHS_LIMA}/{DAYS_LIMA}/"
    s3_raw_path = s3_source + day_route
    bucket = s3_source.split("/")[2]

except Exception as e:
    logger.error("Error while searching for table data")
    logger.error(e)
    log = {
        'PROCESS_ID': f"DLB_{table_name.split('_')[0]}_{table_data['SOURCE_TABLE']}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
        'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
        'PROJECT_NAME': args['PROJECT_NAME'],
        'FLOW_NAME': 'extract_bigmagic',
        'TASK_NAME': 'extract_table_bigmagic',
        'TASK_STATUS': 'error',
        'MESSAGE': f"{e}",
        'PROCESS_TYPE': 'D' if table_data['FILTER_OPERATOR'].strip() in ['between-date'] else 'F',
        'CONTEXT': f"{{server='[{endpoint_data['ENDPOINT_NAME']},{endpoint_data['SRC_SERVER_NAME']}]', user='{endpoint_data['SRC_DB_USERNAME']}', table='{table_data['SOURCE_TABLE']}'}}"
    }
    add_log_to_dynamodb(dynamo_logs_table, log)
    update_attribute_value_dynamodb(
        'TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_RAW', 'FAILED', dynamo_config_table)
    update_attribute_value_dynamodb(
        'TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_STAGE', 'FAILED', dynamo_config_table)
    update_attribute_value_dynamodb(
        'TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'FAIL REASON', f"Can not connect to BD: {e}", dynamo_config_table)
    raise Exception
else:
    try:
        incremental_load = False
        delete_from_target(bucket, day_route)
        if endpoint_data['BD_TYPE'] == 'oracle':
            url = f"jdbc:oracle:thin:@{endpoint_data['SRC_SERVER_NAME']}:{endpoint_data['DB_PORT_NUMBER']}/{endpoint_data['SRC_DB_NAME']}"
            driver = "oracle.jdbc.driver.OracleDriver"

        elif endpoint_data['BD_TYPE'] == 'mssql':
            url = f"jdbc:sqlserver://{endpoint_data['SRC_SERVER_NAME']}:{endpoint_data['DB_PORT_NUMBER']};databaseName={endpoint_data['SRC_DB_NAME']}"
            driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

        elif endpoint_data['BD_TYPE'] == 'mysql':
            url = f"jdbc:mysql://{endpoint_data['SRC_SERVER_NAME']}:{endpoint_data['DB_PORT_NUMBER']}/{endpoint_data['SRC_DB_NAME']}"
            driver = "com.mysql.cj.jdbc.Driver"

        logger.info(f"driver: {driver}")
        logger.info(f"url: {url}")

        if table_data['FILTER_OPERATOR'].strip() in ['between-date']:
            incremental_load = True
            end = table_data['END_VALUE'].strip()
            start = table_data['START_VALUE'].strip()
            logger.info("incremental load")
            logger.info(f"end: {end}")
            logger.info(f"start: {start}")
            number_threads = int(args['THREADS_FOR_INCREMENTAL_LOADS'])
            start = transform_to_dt(start)
            end = transform_to_dt(end)

            delta = (end - start) / number_threads

        else:
            number_threads = 1

        futures_executor = []
        actual_thread = 0
        with futures.ThreadPoolExecutor(max_workers=number_threads + 1) as executor:
            user = endpoint_data['SRC_DB_USERNAME']
            password = get_secret(args['SECRETS_NAME'], endpoint_data["SRC_DB_SECRET"], secrets_region)
            for i in range(number_threads):
                if incremental_load:
                    query = get_query(str(start + delta * (i))[:19], str(start + delta * (i + 1))[:19])
                else:
                    columns_aux = {table_data['COLUMNS']}
                    if table_data.get('ID_COLUMN', '') != '':
                        columns_aux = f"{table_data['ID_COLUMN']} as id," + table_data['COLUMNS']
                    
                    query = f"select {columns_aux} from {table_data.get('SOURCE_SCHEMA', 'CAN NOT FIND SCHEMA NAME')}.{table_data.get('SOURCE_TABLE', 'CAN NOT FIND TABLE NAME')} {table_data.get('JOIN_EXPR', '')} "
                    
                    if table_data.get('FILTER_EXP', '').strip() != '' or table_data.get('FILTER_COLUMN', '').strip() != '':
                        
                        lower_limit, upper_limit = get_limits_for_filter(table_data.get('DELAY_INCREMENTAL_INI', -2), table_data.get('FILTER_DATA_TYPE', ""))
                        if table_data.get('FILTER_COLUMN', '').strip() != '' and table_data['FILTER_OPERATOR'] == 'between':
                            FILTER_COLUMN = table_data['FILTER_COLUMN'].replace('{0}', lower_limit).replace('{1}', upper_limit)
                        else:
                            FILTER_COLUMN = '0=0'
                            
                        if table_data.get('FILTER_EXP', '').strip() != '':
                            FILTER_EXP = table_data['FILTER_EXP']
                        else:
                            FILTER_EXP = '0=0'
                            
                        query += f'where {FILTER_EXP} AND {FILTER_COLUMN}'
                        
                    logger.info(f"final query : {query}")
                futures_executor.append(executor.submit(get_data, url, user, password, driver, query, s3_raw_path, actual_thread, number_threads, secrets_region))
                actual_thread += 1

        total_threads_finished = 0
        total_threads_finished += 1
        logger.info(f"finished {total_threads_finished}/{number_threads} threads in total")

        if incremental_load:
            threads_running = True
            total_procesed_tables = 0
            while threads_running:
                for table_df in futures_executor:
                    if not table_df.running():
                        total_procesed_tables += 1
                        futures_executor.remove(table_df)
                        logger.info("data loaded in the job")
                        logger.info(f"{total_procesed_tables}/{number_threads}")
                if len(futures_executor) == 0:
                    threads_running = False
                time.sleep(5)
        else:
            futures.wait(futures_executor)
            result = futures_executor[0].result()

        logger.info("data loaded to s3")

        update_attribute_value_dynamodb(
            'TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_RAW', 'SUCCEDED', dynamo_config_table)
        update_attribute_value_dynamodb(
            'TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'FAIL REASON', f"", dynamo_config_table)
        log = {
            'PROCESS_ID': f"DLB_{table_name.split('_')[0]}_{table_data['SOURCE_TABLE']}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
            'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
            'PROJECT_NAME': args['PROJECT_NAME'],
            'FLOW_NAME': 'extract_bigmagic',
            'TASK_NAME': 'extract_table_bigmagic',
            'TASK_STATUS': 'satisfactorio',
            'MESSAGE': '',
            'PROCESS_TYPE': 'D' if table_data['FILTER_OPERATOR'].strip() in ['between-date'] else 'F',
            'CONTEXT': f"{{server='[{endpoint_data['ENDPOINT_NAME']},{endpoint_data['SRC_SERVER_NAME']}]', user='{endpoint_data['SRC_DB_USERNAME']}', table='{table_data['SOURCE_TABLE']}'}}"
        }
        add_log_to_dynamodb(dynamo_logs_table, log)
        logger.info("dynamo updated")

    except Py4JJavaError as e:
        update_attribute_value_dynamodb(
            'TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_RAW', 'FAILED', dynamo_config_table)
        update_attribute_value_dynamodb(
            'TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_STAGE', 'FAILED', dynamo_config_table)
        update_attribute_value_dynamodb(
            'TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'FAIL REASON', str(e), dynamo_config_table)
        send_error_message(args['TOPIC_ARN'], table_data['TARGET_TABLE_NAME'], str(e))
        logger.error(e)
        logger.error("Error while importing data")
        log = {
            'PROCESS_ID': f"DLB_{table_name.split('_')[0]}_{table_data['SOURCE_TABLE']}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
            'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
            'PROJECT_NAME': args['PROJECT_NAME'],
            'FLOW_NAME': 'extract_bigmagic',
            'TASK_NAME': 'extract_table_bigmagic',
            'TASK_STATUS': 'error',
            'MESSAGE': f"{e}",
            'PROCESS_TYPE': 'D' if table_data['FILTER_OPERATOR'].strip() in ['between-date'] else 'F',
            'CONTEXT': f"{{server='[{endpoint_data['ENDPOINT_NAME']},{endpoint_data['SRC_SERVER_NAME']}]', user='{endpoint_data['SRC_DB_USERNAME']}', table='{table_data['SOURCE_TABLE']}'}}"
        }
        add_log_to_dynamodb(dynamo_logs_table, log)
        raise Exception

    except Exception as e:
        update_attribute_value_dynamodb(
            'TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_RAW', 'FAILED', dynamo_config_table)
        update_attribute_value_dynamodb(
            'TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_STAGE', 'FAILED', dynamo_config_table)
        update_attribute_value_dynamodb(
            'TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'FAIL REASON', str(e), dynamo_config_table)
        send_error_message(args['TOPIC_ARN'], table_data['TARGET_TABLE_NAME'], str(e))
        logger.error("Error while importing data")
        logger.error(e)
        log = {
            'PROCESS_ID': f"DLB_{table_name.split('_')[0]}_{table_data['SOURCE_TABLE']}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
            'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
            'PROJECT_NAME': args['PROJECT_NAME'],
            'FLOW_NAME': 'extract_bigmagic',
            'TASK_NAME': 'extract_table_bigmagic',
            'TASK_STATUS': 'error',
            'MESSAGE': f"{e}",
            'PROCESS_TYPE': 'D' if table_data['FILTER_OPERATOR'].strip() in ['between-date'] else 'F',
            'CONTEXT': f"{{server='[{endpoint_data['ENDPOINT_NAME']},{endpoint_data['SRC_SERVER_NAME']}]', user='{endpoint_data['SRC_DB_USERNAME']}', table='{table_data['SOURCE_TABLE']}'}}"
        }
        add_log_to_dynamodb(dynamo_logs_table, log)
        raise Exception
    