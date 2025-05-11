import json
import logging
import os

import aws_cdk as cdk
from aws_cdk import Duration, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_glue_alpha as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3_deployment
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subs
from aws_cdk import aws_stepfunctions as sf
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct

logger = logging.getLogger()
logger.setLevel(logging.INFO)

path_to_bucket_landing_scripts = "landing/salesforce/jobs/"
path_to_bucket_raw_jobs_scripts = "raw/salesforce/jobs/"
path_to_bucket_stage_jobs_scripts = "stage/salesforce/jobs/"

path_lambdas = "./artifacts/landing/lambda/"
path_raw_jobs = './artifacts/raw/salesforce/glue/'
path_stage_job = './artifacts/stage/salesforce/jobs/'
path_layers_lambda = "./artifacts/layers/"


def add_enviroment(lambda_function: _lambda, values_and_keys: dict):
    try:
        for value in values_and_keys:
            lambda_function.add_environment(value, values_and_keys[value])
    except Exception as e:
        logger.error("Exception : {}".format(e))


class SalesforceIngestionStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, props, s3_landing_bucket: s3 = None, s3_raw_bucket: s3 = None, s3_stage_bucket: s3 = None, s3_artifacts_bucket: s3 = None, sns_failed_topic: sns = None, sns_success_topic: sns = None, dynamodb_logs_table: dynamodb = None, dynamodb_config_table: dynamodb = None, columns_table: dynamodb = None, crawler_role_arn: str = "",**kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Buckets & DynamoDB--------------------------------------------------------------------------
        artifacts_bucket = s3_artifacts_bucket

        landing_bucket = s3_landing_bucket

        raw_bucket = s3_raw_bucket

        stage_bucket = s3_stage_bucket

        logs_table = dynamodb_logs_table

        etl_configuration_table = dynamodb_config_table

        crawler_role_arn = crawler_role_arn

        columns_table = columns_table

        # Deployment of lambdas y glues --------------------------------------------------------------

        s3_deployment.BucketDeployment(
            self,
            f"aje-{props['Environment']}-sf-landing-jobs",
            sources=[
                s3_deployment.Source.asset(path_lambdas)
            ],
            destination_bucket=artifacts_bucket,
            destination_key_prefix=path_to_bucket_landing_scripts
        )
        s3_deployment.BucketDeployment(
            self,
            f"aje-{props['Environment']}-sf-raw-jobs",
            sources=[
                s3_deployment.Source.asset(path_raw_jobs)
            ],
            destination_bucket=artifacts_bucket,
            destination_key_prefix=path_to_bucket_raw_jobs_scripts
        )
        s3_deployment.BucketDeployment(
            self,
            f"aje-{props['Environment']}-sf-stage-jobs",
            sources=[
                s3_deployment.Source.asset(path_stage_job)
            ],
            destination_bucket=artifacts_bucket,
            destination_key_prefix=path_to_bucket_stage_jobs_scripts
        )

        # SNS Topics ---------------------------------------------------------------------------------------

        failed_topic = sns_failed_topic
        success_topic = sns_success_topic

        # Failed sns policy
        failed_sns_topic = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["sns:Publish"],
            resources=[failed_topic.topic_arn, success_topic.topic_arn]
        )

        # Setting environment for lambda functions that executes appflow -----------------------------------

        policies_s3_appflow = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            principals=[iam.ServicePrincipal("appflow.amazonaws.com")],
            actions=["s3:PutObject",
                     "s3:AbortMultipartUpload",
                     "s3:ListMultipartUploadParts",
                     "s3:ListBucketMultipartUploads",
                     "s3:GetBucketAcl",
                     "s3:PutObjectAcl"],
            resources=[
                landing_bucket.bucket_arn, landing_bucket.bucket_arn + "/*"]
        )
        landing_bucket.add_to_resource_policy(policies_s3_appflow)

        boto3_lambda_layer = _lambda.LayerVersion(self,
                                                  f"boto3-Layer",
                                                  code=_lambda.AssetCode(
                                                      path_layers_lambda + "boto3"),
                                                  description="boto3 version 1.26.11",
                                                  compatible_runtimes=[_lambda.Runtime.PYTHON_3_8])

        # Lambda function that start app flow --------------------------------------------------------------
        _lambdastart_salesforce_workflow = _lambda.Function(
            self,
            f"aje-{props['Environment']}-landing-StartSalesforceLanding",
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler="StartSalesforceLanding.lambda_handler",
            code=_lambda.Code.from_asset(
                path_lambdas + "StartSalesforceLanding"),
            timeout=Duration.minutes(15),
            layers=[boto3_lambda_layer]
        )

        policies_lambda_appflow = iam.ManagedPolicy.from_aws_managed_policy_name(
            'AmazonAppFlowFullAccess')
        _lambdastart_salesforce_workflow.role.attach_inline_policy(
            iam.Policy(
                self,
                f"aje-{props['Environment']}-landing-starting-all_tables_salesforce_publish_sns_policy",
                statements=[iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["sns:Publish"],
                    resources=[failed_topic.topic_arn])]
            )
        )

        policies_dynamo = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["dynamodb:Scan",
                     "dynamodb:GetItem",
                     "dynamodb:UpdateItem",
                     "dynamodb:Query"],
            resources=[etl_configuration_table.table_arn]
        )

        _lambdastart_salesforce_workflow.role.add_managed_policy(
            policies_lambda_appflow)
        _lambdastart_salesforce_workflow.add_to_role_policy(policies_dynamo)



        # Lambda function that descript app flow -----------------------------------------------------------
        _lambdadescript_salesforce_workflow = _lambda.Function(
            self,
            f"aje-{props['Environment']}-landing-DescriptSalesforceLanding",
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler="DescriptSalesforceLanding.lambda_handler",
            code=_lambda.Code.from_asset(
                path_lambdas + "DescriptSalesforceLanding"),
            timeout=Duration.minutes(15),
            layers=[boto3_lambda_layer]
        )

        _lambdadescript_salesforce_workflow.role.attach_inline_policy(
                    iam.Policy(
                        self,
                        f"aje-{props['Environment']}-landing-descripting-all_tables_salesforce_publish_sns_policy",
                        statements=[iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["sns:Publish"],
                            resources=[failed_topic.topic_arn])]
                    )
                )
        _lambdadescript_salesforce_workflow.role.add_managed_policy(
            policies_lambda_appflow)
        _lambdadescript_salesforce_workflow.add_to_role_policy(policies_dynamo)

        # Lambda function to send success message sns
        _lambda_succeeded_message_parameters = {
            "DYNAMODB_TABLE":etl_configuration_table.table_name,
            "ARN_TOPIC_SUCCESS":success_topic.topic_arn,
            "ENDPOINT_NAME":'SALESFORCE'
        }

        _lambdasendsucceededmessage_salesforce_workflow = _lambda.Function(
            self,
            f"aje-{props['Environment']}-sns-send_success_message",
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler="SendSuccededNotification.lambda_handler",
            code=_lambda.Code.from_asset(
                path_lambdas + "SendSuccededNotification"),
            timeout=Duration.minutes(5),
            layers=[boto3_lambda_layer],
            environment=_lambda_succeeded_message_parameters,
        )

        _lambdasendsucceededmessage_salesforce_workflow.role.attach_inline_policy(
                    iam.Policy(
                        self,
                        f"aje-{props['Environment']}-lambda-send-succeeded-notification-policy",
                        statements=[
                            iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["sns:Publish"],
                            resources=[success_topic.topic_arn])
                            ]
                    )
                )
        
        _lambdasendsucceededmessage_salesforce_workflow.add_to_role_policy(policies_dynamo)
        _lambdastart_salesforce_workflow.add_to_role_policy(policies_dynamo)
        
        # Glue for raw job ----------------------------------------------------------------------------------------------      

        role_access_to_s3 = iam.Role(self, f"aje-{props['Environment']}-access-to-stage-raw-s3-role",
                                               assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
                                               )
        
        role_access_to_stage_by_crawler = iam.Role(self, f"aje-{props['Environment']}-access-to-stage-by-crawler-role",
                                               assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
                                               )

        # Policies for glue to access dynamodb
        policies_dynamo_raw = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["dynamodb:Scan",
                     "dynamodb:GetItem",
                     "dynamodb:UpdateItem",
                     "dynamodb:Query",
                     "dynamodb:PutItem"],
            resources=[etl_configuration_table.table_arn, logs_table.table_arn, columns_table.table_arn]
        )

        policies_glue_logs = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "logs:CreateLogGroup",
                "logs:PutLogEvents",
                "logs:CreateLogStream",
            ],
            resources=["arn:aws:logs:*:*:/aws-glue/*"]
        )

        policies_glue_for_crawler = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["glue:GetCrawler",
                     "glue:GetDatabase",
                     "glue:UpdateCrawler",
                     "glue:GetJob",
                     "glue:CreateDatabase",
                     "glue:CreateCrawler",
                     "glue:StartCrawler",
                     "lakeformation:GrantPermissions",
                     "lakeformation:AddLFTagsToResource",
                     "iam:PassRole"
                     ],
            # allows on everything, because the resources doesn't exist yet
            resources=["*"]
        )

        # This is just for the crawlers job
        policies_access_to_s3_stage = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["s3:ListBucket",
                     "s3:ListBucket",
                     "s3:GetObject",
                     "s3:PutObject",
                     "s3:ReplicateObject",
                     "s3:DeleteObject",
                     "s3:GetBucketLocation",
                     "s3:ListBucket",
                     "s3:ListAllMyBuckets",
                     "s3:GetBucketAcl",],
            resources=[stage_bucket.bucket_arn, stage_bucket.bucket_arn + "/*", artifacts_bucket.bucket_arn, artifacts_bucket.bucket_arn + "/*"]
        )

        role_access_to_s3.add_to_policy(policies_dynamo_raw)
        role_access_to_s3.add_to_policy(policies_glue_logs)
        role_access_to_s3.add_to_policy(failed_sns_topic)

        role_access_to_stage_by_crawler.add_to_policy(policies_access_to_s3_stage)
        role_access_to_stage_by_crawler.add_to_policy(policies_dynamo_raw)
        role_access_to_stage_by_crawler.add_to_policy(policies_glue_logs)
        role_access_to_stage_by_crawler.add_to_policy(policies_glue_for_crawler)
        
        raw_job = glue.Job(self, f"aje-{props['Environment']}-sf-load-raw-job",
                           executable=glue.JobExecutable.python_etl(
                                 glue_version=glue.GlueVersion.V4_0,
                                 python_version=glue.PythonVersion.THREE,
                                 script=glue.Code.from_asset(
                                     path_raw_jobs + "extract_salesforce_to_raw.py")
                           ),
                           default_arguments={
                               '--DYNAMO_CONFIG_TABLE': etl_configuration_table.table_name,
                               '--DYNAMO_LOGS_TABLE': logs_table.table_name,
                               '--BUCKET_LANDING_NAME': landing_bucket.bucket_name,
                               '--BUCKET_RAW_NAME': raw_bucket.bucket_name,
                               '--SALESFORCE_S3_PREFIX': props['SalesforceS3Prefix'],
                               '--ARN_TOPIC_FAILED': failed_topic.topic_arn,
                               '--ARN_TOPIC_SUCCESS': success_topic.topic_arn,
                               '--enable-continuous-log-filter': "true",
                               '--TABLE_NAME': "",
                               '--LOAD_TYPE': "",
                               '--datalake-formats': "delta",
                               '--MAIL_LOG': props['MAIL_SALESFORCE_LOG']
                           },
                           worker_type=glue.WorkerType.G_1_X,
                           worker_count=4,
                           continuous_logging=glue.ContinuousLoggingProps(
                               enabled=True),
                           enable_profiling_metrics=True,
                           timeout=Duration.hours(2),
                           max_concurrent_runs=1000,
                           role=role_access_to_s3
                           )
        landing_bucket.grant_read_write(raw_job)
        raw_bucket.grant_read_write(raw_job)

        stage_job = glue.Job(self, f"aje-{props['Environment']}-sf-load-stage-job",
                                executable=glue.JobExecutable.python_etl(
                                        glue_version=glue.GlueVersion.V4_0,
                                        python_version=glue.PythonVersion.THREE,
                                        script=glue.Code.from_asset(
                                            path_stage_job + "transform_light.py")
                                ),
                                default_arguments={
                                    '--DYNAMO_CONFIG_TABLE': etl_configuration_table.table_name,
                                    '--DYNAMO_LOGS_TABLE': logs_table.table_name,
                                    '--DYNAMO_STAGE_COLUMNS': columns_table.table_name,
                                    '--BUCKET_STAGE_NAME': stage_bucket.bucket_name,
                                    '--BUCKET_RAW_NAME': raw_bucket.bucket_name,
                                    '--SALESFORCE_S3_PREFIX': props['SalesforceS3Prefix'],
                                    '--ARN_TOPIC_FAILED': failed_topic.topic_arn,
                                    '--ARN_TOPIC_SUCCESS': success_topic.topic_arn,
                                    '--enable-continuous-log-filter': "true",
                                    '--TABLE_NAME': "",
                                    '--LOAD_TYPE': "",
                                    '--datalake-formats': "delta",
                                    '--MAIL_LOG': props['MAIL_SALESFORCE_LOG']
                                },
                                worker_type=glue.WorkerType.G_1_X,
                                worker_count=6,
                                continuous_logging=glue.ContinuousLoggingProps(
                                    enabled=True),
                                enable_profiling_metrics=True,
                                timeout=Duration.hours(1),
                                max_concurrent_runs=1000,
                                role=role_access_to_s3
                                )
        stage_bucket.grant_read_write(stage_job)
        raw_bucket.grant_read_write(stage_job)

        # job for crawlers
        crawler_job = glue.Job(self, f"aje-{props['Environment']}-crawlers-sf-job",
                               executable=glue.JobExecutable.python_etl(
                                   glue_version=glue.GlueVersion.V4_0,
                                   python_version=glue.PythonVersion.THREE,
                                   script=glue.Code.from_asset(path_stage_job + "crawlers_job_sf.py"),
                               ),
                               default_arguments={
                                   '--S3_STAGE_PREFIX': f"s3://{stage_bucket.bucket_name}/",
                                   '--DYNAMO_CONFIG_TABLE': etl_configuration_table.table_name,
                                   '--PROJECT_NAME' : props['PROJECT_NAME'],
                                   '--enable-continuous-log-filter': "true",
                                   '--datalake-formats': "delta",
                                   '--additional-python-modules': 'boto3==1.26.42',
                                   '--ARN_ROLE_CRAWLER': crawler_role_arn
                               },
                               worker_type=glue.WorkerType.G_1_X,
                               worker_count=4,
                               continuous_logging=glue.ContinuousLoggingProps(enabled=True),
                               enable_profiling_metrics=True,
                               timeout=Duration.hours(2),
                               max_concurrent_runs=10,
                               role=role_access_to_stage_by_crawler
                               )

        # Step Function workflow --------------------------------------------------------------------
        # Lambda task that "starts" Appflow
        start_lambda_task = tasks.LambdaInvoke(
            self, "Configure Salesforce Lambda Appflow",
            lambda_function=_lambdastart_salesforce_workflow,
            retry_on_service_exceptions=False,
            payload_response_only=True,
            input_path= "$"
        )


        # Waiting time of 5 minutes if the status of the flow isnt "Successfull"
        wait_appflow_ingestion = sf.Wait(self, "Wait for Appflow to initiate",
                                         time=sf.WaitTime.duration(
                                             Duration.minutes(5))
                                         )
        
        # Lambda task that "evaluates" flow status
        evaluate_flow_state = tasks.LambdaInvoke(
            self, "Evaluate Flow State",
            lambda_function=_lambdadescript_salesforce_workflow,
            retry_on_service_exceptions=False,
            payload_response_only=True
        )

        # Glue task that "executes" a flow that has a "Successfull" state
        execute_glue_job_raw = tasks.GlueStartJobRun(
            self, "Execute Glue raw job",
            glue_job_name=raw_job.job_name,
            integration_pattern=sf.IntegrationPattern.RUN_JOB,
            arguments=sf.TaskInput.from_object({
                                                   "--TABLE_NAME.$": "$.table_name",
                                                   "--LOAD_TYPE.$": "$.LOAD_TYPE"
                                               }),
            result_path='$.glue_result'
        )

        execute_glue_job_raw.add_retry(backoff_rate=1, max_attempts=10, errors=["Glue.AWSGlueException", "Glue.ConcurrentRunsExceededException"])

        # Glue task that gets executed after the raw job
        execute_glue_job_stage = tasks.GlueStartJobRun(
            self, "Execute Glue stage job",
            glue_job_name=stage_job.job_name,
            integration_pattern=sf.IntegrationPattern.RUN_JOB,
            arguments=sf.TaskInput.from_object({
                                                   "--TABLE_NAME.$": "$.table_name",
                                                   "--LOAD_TYPE.$": "$.LOAD_TYPE"
                                               }),
            result_path='DISCARD'
        )
        # Defining failed stage step
        job_stage_failed = sf.Pass(self, "Error: stage job")

        execute_glue_job_stage.add_retry(backoff_rate=1, max_attempts=10, errors=["Glue.AWSGlueException", "Glue.ConcurrentRunsExceededException"])
        execute_glue_job_stage.add_catch(errors=['States.TaskFailed', 'States.Timeout'], handler=job_stage_failed)
        # Glue task that gets executed after all the stage jobs
        job_crawlers_step = tasks.GlueStartJobRun(self, "Execute Glue crawler job",
                                                        glue_job_name=crawler_job.job_name,
                                                        integration_pattern=sf.IntegrationPattern.RUN_JOB,
                                                        result_path='DISCARD'
                                                        )
        job_crawlers_step.add_retry(backoff_rate=5, max_attempts=10)

        # Lambda task that sends succeded sns message 
        send_sns_lambda_task = tasks.LambdaInvoke(
            self, "Send Succeed message",
            lambda_function=_lambdasendsucceededmessage_salesforce_workflow,
            retry_on_service_exceptions=False
        )

        # Defining flows to execute in parallel, those are passed by the first lambda task
        map_state = sf.Map(self, "Process Flows in Parallel",
                        max_concurrency=100,
                        items_path=sf.JsonPath.string_at("$.table_names"),
                        parameters={"table_name.$": "$$.Map.Item.Value",
                                 "LOAD_TYPE.$": "$.LOAD_TYPE"},
                        result_path="$.mapResults")

        # Defining states inside the state map iterator
        lambda_appflow_created_choice = sf.Choice(self, "Appflow execution status successful?")
        lambda_appflow_created_choice.when(sf.Condition.string_equals("$.result", "CREATING"), wait_appflow_ingestion)
        lambda_appflow_created_choice.when(sf.Condition.or_(sf.Condition.string_equals("$.result", "SUCCESS"), sf.Condition.string_equals("$.result", "FAILED")), execute_glue_job_raw)
        lambda_appflow_created_choice.otherwise(execute_glue_job_raw)

        wait_appflow_ingestion.next(evaluate_flow_state)
        evaluate_flow_state.next(lambda_appflow_created_choice)
        execute_glue_job_raw.next(execute_glue_job_stage)

        map_iterator_definition = wait_appflow_ingestion

        # Conectar el iterator al estado Map
        map_state.iterator(map_iterator_definition)

        map_state.next(send_sns_lambda_task)
        send_sns_lambda_task.next(job_crawlers_step)

        # Conectar el estado Map al resto del flujo
        definition = (start_lambda_task.next(map_state))

        # Create the state machine
        state_function = sf.StateMachine(self, f"aje-{props['Environment']}-landing-State-Machine-SalesforceLoad",
                                         definition=definition,
                                         )

        # event bridge to start the upsert load  (every 2 hours)
        event_bridge_upsert = events.Rule(self, f"aje-{props['Environment']}-start-salesforce-load-rule-upsert",
                                   schedule=events.Schedule.rate(cdk.Duration.hours(5)),
                                   )
        
        # event bridge to start the delete-insert load  (every day)
        event_bridge_snapshot = events.Rule(self, f"aje-{props['Environment']}-start-salesforce-load-rule-delete-insert",
                                        schedule=events.Schedule.rate(cdk.Duration.days(1)),
                                        )

        event_bridge_upsert.add_target(targets.SfnStateMachine(state_function, input=events.RuleTargetInput.from_object({"LOAD_TYPE": "UPSERT_LOAD"})))
        event_bridge_snapshot.add_target(targets.SfnStateMachine(state_function, input=events.RuleTargetInput.from_object({"LOAD_TYPE": "SNAPSHOT_LOAD"})))

        # policies for step function
        policies_glue_fpr_step = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["glue:StartJobRun",
                     "glue:GetJobRun",
                     "glue:GetJobRuns",
                     "glue:BatchStopJobRun"],
            resources=[raw_job.job_arn, stage_job.job_arn, crawler_job.job_arn]
        )

        state_function.add_to_role_policy(policies_glue_fpr_step)

        start_workflow_environment = {
            "SALESFORCE_S3_BUCKET": landing_bucket.bucket_name,
            "SALESFORCE_S3_PREFIX": props['SalesforceS3Prefix'],
            "SALESFORCE_CONECTION_NAME": props['SalesforceConectionName'],
            "DYNAMO_CONFIG_TABLE": etl_configuration_table.table_name,
            "DYNAMO_LOGS_TABLE": logs_table.table_name
        }

        evaluate_flow_environment = {
            "DYNAMO_CONFIG_TABLE": etl_configuration_table.table_name,
            "DYNAMO_LOGS_TABLE": logs_table.table_name
        }

        add_enviroment(_lambdastart_salesforce_workflow,
                       start_workflow_environment)
        
        add_enviroment(_lambdadescript_salesforce_workflow, 
                        evaluate_flow_environment)
        
