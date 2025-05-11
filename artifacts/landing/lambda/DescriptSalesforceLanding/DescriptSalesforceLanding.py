import boto3
import logging
import os

session = boto3.session.Session()

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("IngestionRawSalesforce")
logger.setLevel(os.environ.get("LOGGING", logging.DEBUG))

dynamodb = boto3.resource('dynamodb')
dynamo_config_table = os.getenv('DYNAMO_CONFIG_TABLE').strip()
dynamo_logs_table = os.getenv('DYNAMO_LOGS_TABLE').strip()

def get_appflow_name(object_name, load_type):
    if load_type == 'UPSERT_LOAD':
        object_name_final = object_name.lower() + 'hr'
    else:
        object_name_final = object_name.lower() + 'da'
    return f"ajedtlk-{object_name_final}"

def evaluateFlow(flow_to_evaluate):
    try:
        appflow_client = session.client('appflow')
        execution = appflow_client.describe_flow_execution_records(flowName=flow_to_evaluate, maxResults=1)['flowExecutions'][0]
        logger.debug(
                    f"El estado de la ejecución del flujo: {flow_to_evaluate} es {execution['executionStatus']}.")
        if(execution['executionStatus'] == 'InProgress'):
            return "CREATING"
        elif(execution['executionStatus'] == 'Successful'):
            return "SUCCESS"
        else:
            return "FAILED"
    except Exception as e:
        logger.error(e)


def lambda_handler(event, context):
    load_type = event['LOAD_TYPE']

    try:
        config_table_metadata = dynamodb.Table(dynamo_config_table)
        table_to_evaluate = event['table_name']
        table_data = config_table_metadata.get_item(Key={'TARGET_TABLE_NAME': table_to_evaluate})['Item']
        table_name_evaluate = table_data['SOURCE_TABLE']

        flow_to_evaluate = get_appflow_name(table_name_evaluate, load_type)
        isValid = evaluateFlow(flow_to_evaluate)

        event['result'] = isValid
        return event

    except Exception as e:
        logger.error("Exception: {}".format(e))
        return {
            'result': "FAILED"
        }

