import boto3
import logging
import os

import datetime
import dateutil.tz
import calendar

session = boto3.session.Session()
dynamodb = boto3.resource('dynamodb')

landing_bucket_name = os.getenv('SALESFORCE_S3_BUCKET').strip()
prefix_landing_bucket = os.getenv('SALESFORCE_S3_PREFIX').strip()
salesforce_conection_name = os.getenv('SALESFORCE_CONECTION_NAME').strip()
dynamo_config_table = os.getenv('DYNAMO_CONFIG_TABLE').strip()
dynamo_logs_table = os.getenv('DYNAMO_LOGS_TABLE').strip()

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("IngestionRawSalesforce")
logger.setLevel(os.environ.get("LOGGING", logging.DEBUG))

time_config_lima = dateutil.tz.gettz('America/Lima')
time_lima=datetime.datetime.now(tz=time_config_lima)


schedule_set_one_day_ahead = (time_lima + datetime.timedelta(days=0))
#UTC 5:00 = 0:00 AM LIMA
schedule_set_hour = datetime.time(hour=5, minute=0)
schedule_set_day_hour = datetime.datetime.combine(schedule_set_one_day_ahead, schedule_set_hour).strftime('%Y-%m-%d %H:%M:%S')
schedule_date = datetime.datetime.strptime(schedule_set_day_hour, '%Y-%m-%d %H:%M:%S')
schedule_date_gmt= datetime.datetime(schedule_date.year, schedule_date.month, schedule_date.day, schedule_date.hour, schedule_date.minute)
#Date to Unix 
schedule_date_unix = calendar.timegm(schedule_date_gmt.timetuple())

class IngestionSalesforce:
    def __init__(self) -> None:
        self.appflow_client = session.client('appflow')
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(os.environ.get("LOGGING", logging.DEBUG))

    def create_flow_full(self, table_name, scheduleExpression):
        try:
            response = self.appflow_client.create_flow(
                flowName=self.get_appflow_name(table_name, scheduleExpression),
                description='Pull Data From Salesforce to Amazon s3',
                triggerConfig={
                    'triggerType': 'Scheduled',
                    'triggerProperties': {
                        'Scheduled': {
                            'scheduleExpression': scheduleExpression,
                            'dataPullMode': 'Complete',
                            'scheduleStartTime': schedule_date_unix,
                        }
                    }
                },
                sourceFlowConfig={
                    'connectorType': 'Salesforce',
                    'connectorProfileName': salesforce_conection_name,
                    'sourceConnectorProperties': {'Salesforce': {'object': table_name}}
                },
                destinationFlowConfigList=[
                    {
                        'connectorType': 'S3',
                        'connectorProfileName': 'S3',
                        'destinationConnectorProperties': {
                            'S3': {
                                'bucketName': landing_bucket_name,
                                'bucketPrefix': prefix_landing_bucket,
                                's3OutputFormatConfig': {
                                    'fileType': 'CSV',
                                    'prefixConfig': {
                                        'prefixType': 'PATH_AND_FILENAME',
                                        'prefixFormat': 'DAY'
                                    },
                                    'aggregationConfig': {
                                        'aggregationType': 'None'
                                    },
                                    'preserveSourceDataTyping': True
                                }
                            }
                        }
                    }],
                tasks=[
                    {
                        'taskType': 'Map_all',
                        'sourceFields': [],
                        'connectorOperator': {'Salesforce': 'NO_OP'},
                        "taskProperties": {
                            "EXCLUDE_SOURCE_FIELDS_LIST": "[]"
                        }
                    }
                ]
            )
            self.logger.debug(
                f"Flow ARN Account {response['flowArn']}, Flow Status Account {response['flowStatus']}")
        except Exception as e:
            self.logger.error(e)

    def create_flow_full_account(self, table_name, scheduleExpression):
        try:
            response = self.appflow_client.create_flow(
                flowName=self.get_appflow_name(table_name, scheduleExpression),
                description='Pull Data From Salesforce to Amazon s3',
                triggerConfig={
                    'triggerType': 'Scheduled',
                    'triggerProperties': {
                        'Scheduled': {
                            'scheduleExpression': scheduleExpression,
                            'dataPullMode': 'Complete',
                            'scheduleStartTime': schedule_date_unix,
                        }
                    }
                },
                sourceFlowConfig={
                    'connectorType': 'Salesforce',
                    'connectorProfileName': salesforce_conection_name,
                    'sourceConnectorProperties': {
                            'Salesforce': {
                                'object': table_name,
                                'dataTransferApi': 'REST_SYNC'
                            }
                    }
                },
                destinationFlowConfigList=[
                    {
                        'connectorType': 'S3',
                        'connectorProfileName': 'S3',
                        'destinationConnectorProperties': {
                            'S3': {
                                'bucketName': landing_bucket_name,
                                'bucketPrefix': prefix_landing_bucket,
                                's3OutputFormatConfig': {
                                    'fileType': 'CSV',
                                    'prefixConfig': {
                                        'prefixType': 'PATH_AND_FILENAME',
                                        'prefixFormat': 'DAY'
                                    },
                                    'aggregationConfig': {
                                        'aggregationType': 'None'
                                    },
                                    'preserveSourceDataTyping': True
                                }
                            }
                        }
                    }],
                tasks=[
                    {
                        'taskType': 'Map_all',
                        'sourceFields': [],
                        'connectorOperator': {'Salesforce': 'NO_OP'},
                        "taskProperties": {
                            "EXCLUDE_SOURCE_FIELDS_LIST": "[]"
                        }
                    }
                ]
            )
            self.logger.debug(
                f"Flow ARN Account {response['flowArn']}, Flow Status Account {response['flowStatus']}")
        except Exception as e:
            self.logger.error(e)

    def update_flow_incremental(self, table_name, scheduleExpression):
        try:
            response = self.appflow_client.update_flow(
                flowName=self.get_appflow_name(table_name, scheduleExpression),
                description='Pull Data From Salesforce to Amazon s3',
                triggerConfig={
                    'triggerType': 'Scheduled',
                    'triggerProperties': {
                        'Scheduled': {
                            'scheduleExpression': scheduleExpression,
                            'dataPullMode': 'Incremental',
                            'scheduleStartTime': schedule_date_unix,
                        }
                    }
                },
                sourceFlowConfig={
                    'connectorType': 'Salesforce',
                    'connectorProfileName': salesforce_conection_name,
                    'sourceConnectorProperties': {'Salesforce': {'object': table_name}},
                    'incrementalPullConfig': {
                        'datetimeTypeFieldName': 'LastModifiedDate'
                    }
                },
                destinationFlowConfigList=[
                    {
                        'connectorType': 'S3',
                        'connectorProfileName': 'S3',
                        'destinationConnectorProperties': {
                            'S3': {
                                'bucketName': landing_bucket_name,
                                'bucketPrefix': prefix_landing_bucket,
                                's3OutputFormatConfig': {
                                    'fileType': 'CSV',
                                    'prefixConfig': {
                                        'prefixType': 'PATH_AND_FILENAME',
                                        'prefixFormat': 'DAY'
                                    },
                                    'aggregationConfig': {
                                        'aggregationType': 'None'
                                    },
                                    'preserveSourceDataTyping': True
                                }
                            }
                        }
                    }],
                tasks=[
                    {
                        'sourceFields': [],
                        'connectorOperator': {'Salesforce': 'NO_OP'},
                        'taskType': 'Map_all',
                        "taskProperties": {"EXCLUDE_SOURCE_FIELDS_LIST": "[]"}
                    }
                ]
            )
            self.logger.debug(
                f"Flow Status Account incremental {response['flowStatus']}")
        except Exception as e:
            self.logger.error(e)

    def update_flow_incremental_account(self, table_name, scheduleExpression):
        try:
            response = self.appflow_client.update_flow(
                flowName=self.get_appflow_name(table_name, scheduleExpression),
                description='Pull Data From Salesforce to Amazon s3',
                triggerConfig={
                    'triggerType': 'Scheduled',
                    'triggerProperties': {
                        'Scheduled': {
                            'scheduleExpression': scheduleExpression,
                            'dataPullMode': 'Incremental',
                            'scheduleStartTime': schedule_date_unix,
                        }
                    }
                },
                sourceFlowConfig={
                    'connectorType': 'Salesforce',
                    'connectorProfileName': salesforce_conection_name,
                    'sourceConnectorProperties': {
                            'Salesforce': {
                                'object': table_name,
                                'dataTransferApi': 'REST_SYNC'
                            }
                    },
                    'incrementalPullConfig': {
                        'datetimeTypeFieldName': 'LastModifiedDate'
                    }
                },
                destinationFlowConfigList=[
                    {
                        'connectorType': 'S3',
                        'connectorProfileName': 'S3',
                        'destinationConnectorProperties': {
                            'S3': {
                                'bucketName': landing_bucket_name,
                                'bucketPrefix': prefix_landing_bucket,
                                's3OutputFormatConfig': {
                                    'fileType': 'CSV',
                                    'prefixConfig': {
                                        'prefixType': 'PATH_AND_FILENAME',
                                        'prefixFormat': 'DAY'
                                    },
                                    'aggregationConfig': {
                                        'aggregationType': 'None'
                                    },
                                    'preserveSourceDataTyping': True
                                }
                            }
                        }
                    }],
                tasks=[
                    {
                        'sourceFields': [],
                        'connectorOperator': {'Salesforce': 'NO_OP'},
                        'taskType': 'Map_all',
                        "taskProperties": {"EXCLUDE_SOURCE_FIELDS_LIST": "[]"}
                    }
                ]
            )
            self.logger.debug(
                f"Flow Status Account incremental {response['flowStatus']}")
        except Exception as e:
            self.logger.error(e)        

    def activate_flow(self, table_name, scheduleExpression):
        self.logger.debug(
            f"The flow will be activated: {self.get_appflow_name(table_name, scheduleExpression)}")
        response = self.appflow_client.start_flow(
            flowName=self.get_appflow_name(table_name, scheduleExpression))
        self.logger.debug(
            f"Flow ARN: {response['flowArn']} , Flow Status: {response['flowStatus']}")

    def check_existing_flows(self, table_name, scheduleExpression):
        try:
            self.appflow_client.describe_flow(
                flowName=self.get_appflow_name(table_name, scheduleExpression))
            self.logger.debug(f"The flow exists: {self.get_appflow_name(table_name, scheduleExpression)}")
            return True
        except self.appflow_client.exceptions.ResourceNotFoundException as e:
            self.logger.debug(
                f"The flow does not exist: {self.get_appflow_name(table_name, scheduleExpression)}")
            return False

    def check_executions_records_flows(self, table_name, scheduleExpression):
        try:
            response_data = self.appflow_client.describe_flow_execution_records(
                flowName=self.get_appflow_name(table_name, scheduleExpression))
            if len(response_data['flowExecutions']) > 0:
                self.logger.debug(
                    f'The flow {self.get_appflow_name(table_name, scheduleExpression)} has records executed')
                if response_data['flowExecutions'][0]['executionStatus'] == 'Successful':
                    self.logger.debug(
                        f'The flow {self.get_appflow_name(table_name, scheduleExpression)} will take as reference the last successful execution.')
                    return True
            else:
                self.logger.debug(
                    f'The flow {self.get_appflow_name(table_name, scheduleExpression)} has no executed records')
                return False
        except Exception as e:
            self.logger.error(e)
        return False

    def creation_flows(self, table_name, scheduleExpression):
        try:
            if table_name == 'account':
                self.create_flow_full_account(table_name, scheduleExpression)
            else:
                self.create_flow_full(table_name, scheduleExpression)
            self.logger.debug(
                f'The flow is created {self.get_appflow_name(table_name, scheduleExpression)}')
        except Exception as e:
            self.logger.error(e)

    def convert_to_incremental(self, table_name, scheduleExpression):
        response_data = self.appflow_client.describe_flow(
            flowName=self.get_appflow_name(table_name, scheduleExpression))
        if response_data['triggerConfig']['triggerProperties']['Scheduled']['dataPullMode'] == "Complete" and self.check_executions_records_flows(table_name, scheduleExpression):
            self.logger.debug(
                'It was verified that the flow is in full load and has been executed once successfully.')
            self.logger.debug(
                f'the flow {self.get_appflow_name(table_name,scheduleExpression)} will be incremental')
            if table_name == 'account':
                self.update_flow_incremental_account(table_name, scheduleExpression)
            else:
                self.update_flow_incremental(table_name, scheduleExpression)
        else:
            self.logger.debug(
                f"Flow ARN: {response_data['flowArn']} , Flow Status: {response_data['flowArn']}")
            
    def get_appflow_name(self, object_name, scheduleExpression):
        if scheduleExpression == 'rate(5 hour)':
            object_name_final = object_name.lower() + 'hr'
        else:
            object_name_final = object_name.lower() + 'da'
        return f"ajedtlk-{object_name_final}"

    def process_ingestion(self, appflow_tables_names, scheduleExpression):
        for name_table in appflow_tables_names:
            if self.check_existing_flows(name_table, scheduleExpression):
                self.logger.info(
                    f"There is a flow with the name {self.get_appflow_name(name_table,scheduleExpression)}")
                self.convert_to_incremental(name_table, scheduleExpression)
            else:
                self.logger.info(
                    f"There is no flow with the name {self.get_appflow_name(name_table,scheduleExpression)}")
                self.creation_flows(name_table, scheduleExpression)
                self.activate_flow(name_table, scheduleExpression)


def get_crawlers(dynamodb_table_name):
    dynamodb = boto3.client('dynamodb')
    response = dynamodb.scan(TableName=dynamodb_table_name)
    items = response['Items']
    return items


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


def find_Table_Names(table_name):
    endpoint = 'SALESFORCE'
    response = get_crawlers(table_name)
    dynamo_table_name = table_name
    input_table_names = []
    appflow_tables_names = []
    
    # extract tables by endpoint type - SALESFORCE
    for item in response:
        active_flag = item.get('ACTIVE_FLAG', {}).get('S', '').strip()
        end_point_name = item.get('ENDPOINT', {}).get('S', '').strip()
        if active_flag == 'Y' and end_point_name == endpoint:
            update_attribute_value_dyndb('TARGET_TABLE_NAME', item['TARGET_TABLE_NAME']['S'], 'LAST UPDATE DATE', str(datetime.datetime.today().date()), dynamo_table_name)
            update_attribute_value_dyndb('TARGET_TABLE_NAME', item['TARGET_TABLE_NAME']['S'], 'STATUS_RAW', 'IN PROGRESS', dynamo_table_name)
            update_attribute_value_dyndb('TARGET_TABLE_NAME', item['TARGET_TABLE_NAME']['S'], 'STATUS_STAGE', 'IN PROGRESS', dynamo_table_name)

            table_name_dynamodb = item['TARGET_TABLE_NAME']['S']
            logger.info(f'Table de config leida: {table_name_dynamodb}')
            table_name_appflow_raw = item['SOURCE_TABLE']['S']
            
            input_table_names.append(table_name_dynamodb)
            appflow_tables_names.append(table_name_appflow_raw)

    return (input_table_names, appflow_tables_names)


def lambda_handler(event, context):

    load_type = event['LOAD_TYPE']

    if load_type == 'UPSERT_LOAD':
        scheduleExpression = 'rate(5 hour)'
    else:
        scheduleExpression = 'rate(1 day)'

    try:
        input_tables_names, appflow_tables_names = find_Table_Names(dynamo_config_table)
        logger.info(f'Input table names: {input_tables_names}')
        logger.info(f"Appflow table names: {appflow_tables_names}")
        logger.info(f'Lambda start time: hh:{time_lima.hour}, mm: {time_lima.minute}')
        IngestionSalesforce().process_ingestion(appflow_tables_names, scheduleExpression)
        logger.info(f'Lambda end time: hh:{time_lima.hour}, mm: {time_lima.minute}')
        event['table_names'] = input_tables_names
        return event

    except Exception as e:
        logger.error("Exception: {}".format(e))
        return {
            'table_names': []
        }


    
