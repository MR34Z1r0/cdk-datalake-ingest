1. copy and rename the .env-example to .env and complete the enviroment variables

2. Use: "cdk bootstrap" and "cdk deploy" to deploy the stack

3. Secrets for passwords:

  3.1 Store all the passwords in the secrets 
  ![](./images/secrets2.PNG)
  Store the password using the model

4. Create all the DMS EndPoints for every database as source EndPoint

  ![](./images/dms1.PNG) 

5. In lake Formation create the tags Leve - Stage, Level - Analytics

    ![](./images/lake_formation_1.png)

6. Add the crawler_job_role to admins in lake formation

    ![](./images/lake_formation_2.png)

7. Add in Dynamo Table all the data to import

# Dynamo credentials data model:
  
  7.1 this is for the EndpointsÂ´s credentials 

{
  "ENDPOINT_NAME": {
    "S": "BANNER"
  },
  "SRC_DB_NAME": {
    "S": "PROD"
  },
  "SRC_DB_SECRET": {
    "S": "banner_password"
  },
  "SRC_DB_USERNAME": {
    "S": "USRDWHAWS"
  },
  "SRC_SERVER_NAME": {
    "S": "10.200.2.80"
  },
  "DB_PORT_NUMBER": {
    "S": "1521"
  },
  "BD_TYPE": {
    "S": "oracle"
  },
  "ENDPOINT_ARN": {
    "S": "arn:aws:dms:us-east-1:905663700767:endpoint:ZEX4XXQ35K52CW2DV5SU37T2OCPZVMWFT5D3LFQ"
  }
}

  BD types:
  oracle
  mysql
  mssql

# Dynamo import data model:

{
  "TARGET_TABLE_NAME": {
    "S": "IECAMC_PS_KEYWORD_TBL"
  },
  "ACTIVE_FLAG": {
    "S": "Y"
  },
  "ENDPOINT": {
    "S": "CAMPUS_PROD"
  },
  "END_VALUE": {
    "S": "2022-12-12 12:32:22"
  },
  "FILTER_COLUMN": {
    "S": "ADD_DATE"
  },
  "FILTER_OPERATOR": {
    "S": "between"
  },
  "ID_COLUMN": {
    "S": "ID"
  },
  "SOURCE_SCHEMA": {
    "S": "SYSADM"
  },
  "SOURCE_TABLE": {
    "S": "PS_KEYWORD_TBL"
  },
  "START_VALUE": {
    "S": "2022-12-10 12:32:22"
  }
}

# Operators:
between: incremental
lte: full loads

# Should look like this
![](./images/6_1.PNG)


8. Create  S3, secrets manager and dynamo VPC endpoints for the VPC

    8.1 Set names and select secretsmanager endpoint
  ![](./images/5_1.PNG)
  secretManager EndPoint (add the created Security Group)

    8.2 Select the respective VPC and it´s subnets
  ![](./images/5_2.PNG)

    8.3 Select the respective Security groups
  ![](./images/5_3.PNG)

    8.4 same for all endpoints

9. To start the workflow, run the StartWorkflow Lambda