import pandas as pd
import boto3

boto3.setup_default_session(profile_name='ssoajedevprofile')
client = boto3.client('dynamodb')

dynamo_config_table = 'dynamo_tables_table'
dynamo_columns_table = 'dynamo_culms_table'

file = './excel_source.xlsx'
databases = ['BD_names']

raw_s_table = pd.read_excel(file).fillna("")
raw_p_table = pd.read_excel(file, sheet_name=1).fillna("")
raw_columns = pd.read_excel(file, sheet_name=2).fillna("")
stage_tables = pd.read_excel(file, sheet_name=3).fillna("")
stage_columns_table = pd.read_excel(file, sheet_name=4).fillna("")

total_raw = raw_p_table.join(raw_s_table.set_index('TABLA'), on= 'table_name', how= 'inner')
total_raw = total_raw.join(stage_tables.set_index('source_table_name'), on= 'table_name',  rsuffix= '_stage', how= 'inner')

for row in total_raw.index:
    print(row)
    item = {}

    table_name = total_raw['table_name'][row]
    schema_name = total_raw['ESQUEMA'][row]
    stage_table_name = total_raw['table_name_stage'][row]
    subset_columns_raw = raw_columns.query(f"table_name == '{table_name}' and column_id >= -1")
    subset_columns_stage = stage_columns_table.query(f"table_name == '{stage_table_name}'")

    item['ACTIVE_FLAG'] = {'S' : 'Y'}
    item['COLUMNS'] = {'S' : ','.join(subset_columns_raw["exp_column_calculated"] + " " +subset_columns_raw["column_name"])}
    item['CRAWLER'] = {'BOOL' : False}

    if total_raw['delay_incremental_ini'][row] != '':
        item['DELAY_INCREMENTAL_INI'] = {'N' : str(total_raw['delay_incremental_ini'][row])}

    item['FILTER_COLUMN'] = {'S' : total_raw['exp_filter_full'][row]}
    item['FILTER_EXP'] = {'S' : total_raw['exp_filter'][row] }
    item['FILTER_OPERATOR'] = {'S' : 'lte'} if total_raw['exp_filter_full'][row] == '' else {'S' : 'between'}
    item['ID_COLUMN'] = {'S' : total_raw['PRIMARY KEY COLUMN(S)'][row] }
    item['JOIN_EXPR'] = {'S' : total_raw['exp_join'][row] }
    item['PROCESS_ID'] = {'S' : str(total_raw['process_id'][row]) }
    item['SOURCE_SCHEMA'] = {'S' : schema_name}
    item['SOURCE_TABLE'] = {'S' : total_raw['table_alias'][row].replace("dbo.",'').replace("(nolock)",'')
}
    item['STAGE_TABLE_NAME'] = {'S' : stage_table_name}
    
    for bd in databases:
        target_table_name = f"{bd}_{str(stage_table_name).upper()}"
        item['TARGET_TABLE_NAME'] = {'S' : target_table_name}
        item['ENDPOINT'] = {'S' : bd}

        print(item)

        response = client.put_item(
            TableName = dynamo_config_table,
            Item = item
        )

        print(response['ResponseMetadata']['HTTPStatusCode'])

        for column_row in subset_columns_stage.index:
            columns = {}

            columns['TARGET_TABLE_NAME'] = {'S' : target_table_name}
            columns['COLUMN_NAME'] = {'S' : subset_columns_stage['column_name'][column_row]}
            columns['NEW_DATA_TYPE'] = {'S' : subset_columns_stage['column_type'][column_row]}
            columns['COLUMN_ID'] = {'N' : subset_columns_stage['column_id'][column_row]}
            
            
            columns['IS_ID'] = {'BOOL' : False} if subset_columns_stage['is_pk_table'][column_row] == '' else {'BOOL' : True}
            columns['IS_ORDER_BY'] = {'BOOL' : False} if subset_columns_stage['is_order'][column_row] == '' else {'BOOL' : True}
            columns['IS_PARTITION'] = {'BOOL' : False} if subset_columns_stage['is_partition'][column_row] == '' else {'BOOL' : True}

            function = subset_columns_stage['fn_transform_name1'][column_row]
            input_1 = subset_columns_stage['fn_transform_input_column1'][column_row]
            parameter_1 = subset_columns_stage['fn_transform_input_column1'][column_row]
            default_1 = subset_columns_stage['fn_transform_input_default1'][column_row].replace('$','')

            if 'fn_transform_ClearString' in function:
                columns['TRANSFORMATION'] = f"fn_transform_ClearString({input_1})"

            elif 'fn_transform_Concatenate' in function:
                columns['TRANSFORMATION'] = f"fn_transform_Concatenate({input_1})"

            elif 'fn_transform_DateMagic' in function:
                columns['TRANSFORMATION'] = f"fn_transform_DateMagic({input_1},yyyy-MM-dd,{default_1})"

            elif 'fn_transform_DatetimeMagic' in function:
                columns['TRANSFORMATION'] = f"fn_transform_DatetimeMagic({input_1},yyyy-MM-dd HH:mm:ss,{default_1})"
            
            elif 'fn_transform_Datetime' in function:
                columns['TRANSFORMATION'] = f"fn_transform_Datetime({input_1})"
            
            elif 'fn_transform_ByteMagic' in function:
                columns['TRANSFORMATION'] = f"fn_transform_ByteMagic({input_1},{default_1})"

            elif 'fn_transform_Case' in function:
                columns['TRANSFORMATION'] = f"fn_transform_Case_with_default({input_1},{parameter_1},{default_1})"

            elif 'fn_transform_PeriodMagic' in function:
                columns['TRANSFORMATION'] = f"fn_transform_PeriodMagic({input_1})"

            else:
                columns['TRANSFORMATION'] = subset_columns_stage['source_column_name'][column_row]

            columns['TRANSFORMATION'] = {'S' : columns['TRANSFORMATION']}
            
            print(columns)
            response = client.put_item(
                TableName = dynamo_columns_table,
                Item = columns
            )

            print(response['ResponseMetadata']['HTTPStatusCode'])
