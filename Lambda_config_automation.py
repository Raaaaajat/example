import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import datetime
import logging
import logging.config
import os
import sys
import traceback
import xml.etree.ElementTree as ET

DYNAMODB = boto3.resource('dynamodb')
s3 = boto3.client('s3')
LAMBDA_CLIENT = boto3.client('lambda')

def load_log_config():
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    return root

LOGGER = load_log_config()

JSON_FILE = open('config_automation.json')r
CONFIG = json.load(JSON_FILE)

DYNAMODB_RUN_INFO_TABLE = CONFIG['dynamodbRunInfoTable']
DYNAMODB_WORKFLOWS_TABLE = CONFIG['dynamodbWorkflowsTable']
DYNAMODB_REDSHIFT_OPERATIONS_TABLE = CONFIG['redshiftOperationsTable']
DYNAMODB_DWH_TABLE = CONFIG['dynamodbTablesTable']
COL_RUN_INFO_SOURCE = CONFIG['runInfoSource']
COL_RUN_INFO_BATCHID = CONFIG['runInfoBatchID']
COL_RUN_INFO_CLEANSER_STATUS = CONFIG['runInfoCleanserStatus']
COL_RUN_INFO_CONFORMED_STATUS = CONFIG['runInfoConformedStatus']
COL_RUN_INFO_DWH_STATUS = CONFIG['runInfoDWHStatus']
SUCCESSFUL = CONFIG['successful']
FAILED = CONFIG['failed']
CONFIG_PATH_DAY0 = CONFIG['conformedConfigDay0Path']
CONFIG_PATH_INCREMENTAL = CONFIG['conformedConfigIncrementalPath']
CONFIG_PATH_BISTAGING=CONFIG['conformedConfigBistagingPath']
CONFIG_PATH = CONFIG['configpath']
CONFORMED_GLUE_JOB_NAME = CONFIG['glueJobNameConformed']
DWH_GLUE_JOB_NAME = CONFIG['glueJobNameDWH']
CONFORMED_STAGE = "conformed"
DWH_STAGE = "dwh"
DYNAMODB_DWH_RUNS_TABLE = CONFIG['dynamodbDWHTableRuns']
OPERATION_COPY_CONFORMED = CONFIG['operationCopyConformed']
OPERATION_COPY_CONFORMED_BISTAGING = CONFIG['operationCopyConformedBIStaging']
PARALLEL_FACT = CONFIG['parallelFact']
LAMBDA_STATUS_LOGGING = CONFIG['lambdaStatusLogging']
RERUN_FLAG = CONFIG['rerunFlag']
UNLOAD_FLAG = CONFIG['unloadFlag']
TRANSFORM_FLAG = CONFIG['tranformFlag']
PREPROCESS = "preprocess"
SEND_EMAIL_INDICATOR = "false"
IN_PROGRESS = "IN_PROGRESS"
NOT_STARTED = "NOT_STARTED"
BISTAGINGSOURCE=CONFIG["bistagingsource"]
XML_PATH = CONFIG['xml_path']
S3_CONFIG = CONFIG['s3_config']


def read_dynamodb_workflows(source, workflow):
    try:
        table_workflows = DYNAMODB.Table(DYNAMODB_WORKFLOWS_TABLE)
        table_workflow_data = table_workflows.get_item(Key = {'source': source, 'workflow': workflow})['Item']
    except Exception as e:
        raise Exception("No item found for the given source and workflow on dynamodb.")
    return table_workflow_data
    
def update_run_info(batch_id, source):
    table_run_info = DYNAMODB.Table(DYNAMODB_RUN_INFO_TABLE)
    response_write_run_info = table_run_info.put_item(
      Item = {
            'batchID': batch_id,
            'source': source,
            'cleanserStatus': SUCCESSFUL,
            'conformedStatus': IN_PROGRESS,
            'dwhStatus': NOT_STARTED
        }
    )
    LOGGER.info('DynamoDB run_info Insert: ' + str(response_write_run_info))
 

def get_glue_configs(table_workflow_data):
    glue_number_of_workers = table_workflow_data['glueConfIncr'][0]
    glue_worker_type = table_workflow_data['glueConfIncr'][1]
    glue_number_of_repartitions = table_workflow_data['glueConfIncr'][2]
    is_day_0 = str(table_workflow_data["isDay0"])
    glue_config = { 
        'glueNumberOfWorkers': glue_number_of_workers,
        'glueWorkerType': glue_worker_type,
        'glueNumberOfRepartitions': glue_number_of_repartitions,
        'isDay0': is_day_0
    }
    return glue_config


def get_unload_operations(batchId, source, workflow): 
    LOGGER.info("Getting unload operations.")
    unload_operations = []
    unload_response = []
    try:
        table_reshift_operations = DYNAMODB.Table(DYNAMODB_REDSHIFT_OPERATIONS_TABLE)
        keyCondition = Key('operation').eq("unload_reference")
        response = table_reshift_operations.query(
            KeyConditionExpression=keyCondition        
        )
        unload_response.append(response['Items'][0])
        secondKeyCondition = Key('operation').eq("unload_dwh")
        dwh_response = table_reshift_operations.query(
            KeyConditionExpression=secondKeyCondition        
        )
        unload_response.append(dwh_response['Items'][0])
        LOGGER.info(unload_response) 
        for record in unload_response:
            tables_split = record['tables'].split(",")
            for table in tables_split:
                unload_obj = {}
                unload_obj["operation"] = record["operation"]
                unload_obj["db"] = record["database"]
                unload_obj["staging_schema"] = record["staging_schema"]
                unload_obj["target_schema"] = record["target_schema"]
                unload_obj["file_object"] = record["file_object"]
                unload_obj["s3_path"] = record["s3_path"] + table + "/" if (record["operation"] == "unload_reference") else record["s3_path"] + "dwh_" + table + "/" 
                unload_obj["s3_bucket"] = record["s3_bucket"]
                unload_obj["db_creds"] = record["credentials"]
                unload_obj["arn_role"] = record["arn_role"]
                unload_obj["tablename"] = table
                unload_obj["tableCols"] = "None"
                unload_obj["temp_schema"] = record["temp_schema"]
                unload_obj["s3_format"] = "None"
                unload_obj["batchId"] = batchId
                unload_obj["source"] = source
                unload_obj["workflow"] = workflow
                unload_obj["ref_schema"] = "None"
                unload_obj["audit_schema"] = "None"
                unload_operations.append(unload_obj)
                
    except Exception as e:
        raise Exception("No item found for the given source and workflow on dynamodb.")
    return unload_operations

    
def get_conformed_tables(source):
    LOGGER.info("Getting conformed tables.")
    conformed_tables = []
    try:
        table_dwh_table = DYNAMODB.Table(DYNAMODB_DWH_TABLE)
        filterExp = Attr('stage').eq("conformed");
        response = table_dwh_table.scan(
            FilterExpression=filterExp        
        )
        for record in response['Items']:
            if record["source"].lower()==source.lower():
                conformed_tables.append(record["table"])
    except Exception as e:
        raise Exception("No item found on dynamodb.")
    LOGGER.info(conformed_tables)
    return conformed_tables    
    

def get_fact_or_dimension_tables(operation): 
    LOGGER.info("Getting dimension tables operations.")
    tables_split = []
    try:
        table_reshift_operations = DYNAMODB.Table(DYNAMODB_REDSHIFT_OPERATIONS_TABLE)
        keyCondition = Key('operation').eq(operation)
        response = table_reshift_operations.query(
            KeyConditionExpression=keyCondition        
        )
        LOGGER.info(response['Items'][0]) 
        for record in response['Items']:
            tables_split = record['tables'].split(",")
    except Exception as e:
        raise Exception("No item found on dynamodb.")
    return tables_split

def get_config_from_dynamodb(table_name, item_key):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # Assuming 'item_key' is the primary key value for the item
    response = table.get_item(
        Key={
            'workflow': source
        }
    )

    # Check if the item was found
    if 'Item' in response:
        item = response['Item']
        # Assuming 'xml_config_path' and 'is_day0' are attribute names in the DynamoDB item
        xml_config_path = item.get('xml_config_path', '')
        is_day0 = item.get('is_day0', False)
        try:
            xml_content = response['Body'].read().decode('utf-8')
        return xml_content, is_day0

        except Exception as e:
            print(f"Error reading XML from S3: {e}")
            return None
    else:
        # Return None if item not found
        return None, False

# def read_xml_from_s3(s3_path):
#     s3 = boto3.client('s3')
#     try:
#         response = s3.get_object(Bucket= XML_path, Key=s3_path.lstrip('s3://'))
#         xml_content = response['Body'].read().decode('utf-8')
#         return xml_content
#     except Exception as e:
#         print(f"Error reading XML from S3: {e}")
#         return None

# def get_tables_from_dynamodb(table_name, source):
#     dynamodb = boto3.resource('dynamodb')
#     table = dynamodb.Table(table_name)

#     try:
#         response = table.scan(FilterExpression=Attr('stage').eq('conformed') & Attr('source').eq(source))
#         items = response.get('Items', [])
#         return [item.get('table', '') for item in items]
#     except Exception as e:
#         print(f"Error in fetching tables from DynamoDB: {e}")
#         return []

def extract_table_info(xml_content):
    tree = ET.fromstring(xml_content)

    # Extract table names, source types, key columns, and column schemas
    table_info = []
    for prop in tree.findall(".//property"):
        table_name_element = prop.find("name")
        table_value_element = prop.find("value")
        if table_name_element is not None and table_value_element is not None:
            table_name_with_prefix = table_name_element.text if table_name_element is not None else ""
            table_name = table_name_with_prefix.replace('diffen.table.', '')
            source_type_element = tree.find(f".//property[name='diffen.table.{table_name}.sourcetype']/value")
            sourcing_type_element = tree.find(f".//property[name='diffen.table.{table_name}.sourcing.type']/value")
            key_cols_element = tree.find(f".//property[name='diffen.table.{table_name}.keycols']/value")
            col_schema_element = tree.find(f".//property[name='diffen.table.{table_name}.col_schema']/value")

            if source_type_element is not None and sourcing_type_element is not None and key_cols_element is not None and col_schema_element is not None:
                table_info.append({
                    "table_name": table_name,
                    "source_type": source_type_element.text,
                    "sourcing_type": sourcing_type_element.text,
                    "key_cols": key_cols_element.text.split(","),
                    "col_schema": col_schema_element.text.split("^")
                })

    return table_info

def generate_json(table_info):
    json_data = {
        "stage": "conformed",
        "source": f"s3://cebu-dwh-cleanser-zone-dev/dw_processed/"+source+"/cleanser/",
        "refSource": "s3://cebu-dwh-conformed-zone-dev/dw_processed/flatfiles/unload/",
        "target": f"s3://cebu-dwh-conformed-zone-dev/dw_processed/"+source+"/conformed/",
        "dwhTables": []
    }

    for table in table_info:
        dwh_table = {
            "dwhTable": table["table_name"],
            "tableType": "DIM",
            "mainTables": [table["table_name"]],
            "sourceTables": [table["table_name"]],
            "refTables": [],
            "transSQL": [
                {
                    "sqlQuery": f"SELECT {', '.join(table['key_cols'])} FROM {table['table_name']}",
                    "tempTable": "OUTPUT"
                }
            ],
            "tableSchema": [
                # Additional columns for every table
                {"columnName": f"{table['table_name']}Key", "dataType": "int", "defaultValue": ""},
                {"columnName": "Source", "dataType": "string", "defaultValue": "ODS"}
            ]
        }

        for col_schema in table["col_schema"]:
            col_info = col_schema.split(" ")
            col_name = col_info[0]
            col_data_type = col_info[1]
            default_value = col_info[2] if len(col_info) > 2 else ""

            column_entry = {
                "columnName": col_name,
                "dataType": col_data_type,
                "defaultValue": default_value
            }

            dwh_table["tableSchema"].append(column_entry)

        # Additional columns for every table
        additional_columns = [
            {"columnName": "IsActive", "dataType": "boolean", "defaultValue": "true"},
            {"columnName": "IsDeletedInSource", "dataType": "boolean", "defaultValue": "false"},
            {"columnName": "EndPHT", "dataType": "timestamp", "defaultValue": ""},
            {"columnName": "CreatedPHT", "dataType": "timestamp", "defaultValue": ""},
            {"columnName": "ModifiedPHT", "dataType": "timestamp", "defaultValue": ""},
            {"columnName": "HasDeletedField", "dataType": "boolean", "defaultValue": "false"}
        ]

        # Add additional columns to the table schema
        dwh_table["tableSchema"] += additional_columns

        json_data["dwhTables"].append(dwh_table)

    return json_data

def save_json_to_s3(json_data, s3_path):
    with open("output.json", "w") as json_file:
        json.dump(json_data, json_file, indent=2)

def save_json_to_s3(json_data, s3_path):
    # Convert JSON data to a string
    json_str = json.dumps(json_data, indent=2)

    try:
        # Upload the JSON string to S3
        s3.put_object(Body=json_str, Bucket='S3_CONFIG', Key=s3_path.lstrip('s3://'))
        print(f"JSON uploaded to S3: {s3_path}")
    except Exception as e:
        print(f"Error uploading JSON to S3: {e}")


if __name__ == "__main__":

    # Fetch tables from DynamoDB based on criteria
    # tables = get_tables_from_dynamodb('cebu-dwh-tables-dev', source)
    tables = conformed_tables

    # Check if any tables are found
    if tables:
        # Iterate through the tables and process each one
        for table in tables:
            # Assuming 'item_key' is available from the event or elsewhere
            item_key = 'your_primary_key_value'

            # Fetch XML config path and isDay0 from DynamoDB
            xml_config_path, is_day0 = get_config_from_dynamodb('cebu-dwh-workflows-dev', item_key)

            # Check if XML config path is not empty
            if xml_config_path:
                # Read XML content from S3
                # xml_content = read_xml_from_s3(xml_config_path)
                xml_content = get_config_from_dynamodb(table_name, item_key)

                # Extract table info from XML content
                table_info = extract_table_info(xml_content)

                # Generate JSON from table info
                json_data = generate_json(table_info)

                # Save JSON to S3
                s3_output_path = f"s3://cebu-dwh-config-dev/conformed_config/dwh_conformed_config_{item_key}_{table}.json"
                save_json_to_s3(json_data, s3_output_path)
            else:
                print("XML config path is empty. Unable to proceed.")
    else:
        print(f"No tables found in DynamoDB for source: {source}")

    
def get_conformed_table_details(conformed_tables, batchId, source, workflow, glue_config):
    conformed_tables_details = []
    for table in conformed_tables:
        transform_obj = {} 
        glue_args = {}
        glue_args["--tableName"] = table
        glue_args["--batchId"] = batchId
        glue_args["--source"] = source 
        
        if (glue_config["isDay0"].lower() == "true"):
            glue_args["--configPath"] = CONFIG_PATH+"day0_"+source+".json"
            glue_args["--isFromNonOpen"]="false"
        elif source.lower()==BISTAGINGSOURCE.lower():
            glue_args["--configPath"]=CONFIG_PATH_BISTAGING
            glue_args["--isFromNonOpen"]="true"
        else:
            glue_args["--configPath"] = CONFIG_PATH+"incremental_"+source+".json"
            glue_args["--isFromNonOpen"]="false"
            
        glue_args["--outputRepartition"] = str(glue_config["glueNumberOfRepartitions"])
        glue_args["--isDay0"] = glue_config["isDay0"]
        
        transform_obj["glueJobName"] = CONFORMED_GLUE_JOB_NAME
        transform_obj["tableName"] = table
        transform_obj["workflow"] = workflow
        transform_obj["glueJobArgs"] = glue_args
        transform_obj["glueConfig"] = glue_config
        conformed_tables_details.append(transform_obj)
    LOGGER.info(conformed_tables_details)
    return conformed_tables_details
    

def get_dwh_table_details(conformed_tables, batchId, source, glue_config, workflow, redshift_operation):
    dwh_tables_details = []
    copy_details = {}
    try:
        table_cebu_redshift = DYNAMODB.Table(DYNAMODB_REDSHIFT_OPERATIONS_TABLE)
        response = table_cebu_redshift.query(
            KeyConditionExpression=Key("operation").eq(redshift_operation)        
        )
        for record in response['Items']:
            copy_details["operation"] = redshift_operation
            copy_details["db"] = record["database"]
            copy_details["db_creds"] = record["credentials"]
            copy_details["s3_bucket"] = record["s3_bucket"]
            copy_details["staging_schema"] = record["staging_schema"]
            copy_details["target_schema"] = record["target_schema"]
            copy_details["file_object"] = record["file_object"]
            copy_details["s3_path"] = record["s3_path"]
            copy_details["arn_role"] = record["arn_role"]
            copy_details["temp_schema"] = record["temp_schema"]
            copy_details["audit_schema"] = record["audit_schema"]
            copy_details["ref_schema"] = record["ref_schema"]
        LOGGER.info(copy_details) 
    except Exception as e:
        raise Exception("No runs found for the given operation on dynamodb.")
         
    for table in conformed_tables:
        dwh_obj = {}
        dwh_obj["batchId"] = batchId
        dwh_obj["source"] = source
        dwh_obj["workflow"] = workflow
        dwh_obj["operation"] = redshift_operation
        dwh_obj["db"] = copy_details["db"]
        dwh_obj["staging_schema"] = copy_details["staging_schema"]
        dwh_obj["target_schema"] = copy_details["target_schema"]
        dwh_obj["file_object"] = copy_details["file_object"]
        dwh_obj["s3_path"] = copy_details["s3_path"] + table.lower() + "/dt=" + batchId[0:8] + "/hh=" + batchId[8:10] + "/"
        dwh_obj["s3_bucket"] = copy_details["s3_bucket"]
        dwh_obj["db_creds"] = copy_details["db_creds"]
        dwh_obj["arn_role"] = copy_details["arn_role"]
        dwh_obj["tablename"] = table.lower()
        dwh_obj["tableCols"] = "None"
        dwh_obj["temp_schema"] = copy_details["temp_schema"]
        dwh_obj["s3_format"] = "ORC" 
        dwh_obj["audit_schema"] = copy_details["audit_schema"]
        dwh_obj["ref_schema"] = copy_details["ref_schema"]
        dwh_tables_details.append(dwh_obj)
    return dwh_tables_details
    
def get_non_successful_dwh_runs(batchId):
    LOGGER.info("Getting non-successful dwh table runs.")
    dwh_tables = []
    try:
        table_dwh_table_runs = DYNAMODB.Table(DYNAMODB_DWH_RUNS_TABLE)
        response = table_dwh_table_runs.query(
            KeyConditionExpression=Key("batchID").eq(batchId)        
        )
        for record in response['Items']:
            if (record["run_status"] != "SUCCESSFUL") and (record["workflow"] == "dwh"):
                dwh_tables.append(record["table"])
    except Exception as e:
        raise Exception("No runs found for the given batchId on dynamodb.")
    LOGGER.info(dwh_tables)
    return dwh_tables    
    
    
def get_reference_tables(workflow,source):
    LOGGER.info("Getting reference tables.")
    ods_reference_to_run = ["organization", "location", "bookingcontacttype"]
    reference_tables = []
    try:
        table_dwh_table = DYNAMODB.Table(DYNAMODB_DWH_TABLE)
        #filterExp = Attr('stage').eq("dwh");
        filterExp=Attr('stage').eq(workflow) & Attr('source').eq(source)
        response = table_dwh_table.scan(
            FilterExpression=filterExp        
        )
        for record in response['Items']:
            if (record['table'].lower() in ods_reference_to_run):
                reference_tables.append({'table':record['table'],'targetS3':record['targetS3']})
    except Exception as e:
        raise Exception("No item found for the given source and workflow on dynamodb.")
    return reference_tables   
    
    
def lambda_handler(event, context):

    # Fetch tables from DynamoDB based on criteria
    # tables = get_tables_from_dynamodb('cebu-dwh-tables-dev', source)
    tables = conformed_tables

    # Check if any tables are found
    if tables:
        # Iterate through the tables and process each one
        for table in tables:
            # Assuming 'item_key' is available from the event or elsewhere
            item_key = 'your_primary_key_value'

            # Fetch XML config path and isDay0 from DynamoDB
            xml_config_path, is_day0 = get_config_from_dynamodb('cebu-dwh-workflows-dev', item_key)

            # Check if XML config path is not empty
            if xml_config_path:
                # Read XML content from S3
                # xml_content = read_xml_from_s3(xml_config_path)
                xml_content = get_config_from_dynamodb(table_name, item_key)

                # Extract table info from XML content
                table_info = extract_table_info(xml_content)

                # Generate JSON from table info
                json_data = generate_json(table_info)

                # Save JSON to S3
                s3_output_path = f"s3://cebu-dwh-config-dev/conformed_config/dwh_conformed_config_{item_key}_{table}.json"
                save_json_to_s3(json_data, s3_output_path)
            else:
                print("XML config path is empty. Unable to proceed.")
    else:
        print(f"No tables found in DynamoDB for source: {source}")
        
    try:
        LOGGER.info("Started pre-processing for Conformed/DWH Layer.")
        batchId = None
        workflow = None
        source = None
        unload_opertaions = []
        conformed_tables_details = []
        conformed_tables = []
        dwh_tables = [] 
        scd_dimension_tables = []
        sequential_fact_tables = []
        reference_tables = []
        rerun_flag = RERUN_FLAG
        unload_flag = UNLOAD_FLAG
        transform_flag = TRANSFORM_FLAG
        preprocess = PREPROCESS
        send_email_indicator = SEND_EMAIL_INDICATOR
        log_status = SUCCESSFUL
        
        
        #IF DICT, FIRST PROCESSING FOR CONFORMED AND DWH LAYER 
        if(type(event) is dict):
            LOGGER.info("FIRST PROCESSING FOR CONFORMED/DWH.")
            # runType = event['runType']
            batchId = event['batchId']
            workflow = event['workflow']
            source = event['source']
             
            #GET GLUE CONFIGS
            table_workflow_data = read_dynamodb_workflows(source, workflow)
            isDay0 = str(table_workflow_data["isDay0"])
            glue_config = get_glue_configs(table_workflow_data)
            rerun_flag = str(table_workflow_data["isRerun"])
            
            
            #GET CONFOMRED TABLES FROM DYNAMODB
            conformed_tables = get_conformed_tables(source)     

            # IF CONFORMED
            # 1. SET UNLOAD AND TRANFORM FLAG TO TRUE
            # 2. GET TABLES TO BE UNLOADED 
            # 3. GET TABLES TO BE TRANSFORMED
            if (workflow == CONFORMED_STAGE):
                transform_flag = "true"
                unload_flag = "true"
                if (rerun_flag.lower() == "true"):  
                    update_run_info(batchId, source)
                #1. GET TABLES TO BE UNLOADED 
                unload_opertaions = get_unload_operations(batchId, source, workflow)
                LOGGER.info(unload_opertaions)
                #2. GET TABLES TO BE TRANSFORMED
                conformed_tables_details = get_conformed_table_details(conformed_tables, batchId, source, workflow, glue_config)
                if not conformed_tables_details:
                    raise Exception("No table to process for conformed.")
                LOGGER.info(conformed_tables)
                
            # IF DWH
            #   1. CHECK IF RERUN
            #   2. REMOVED SUCCESS TABLES IF RERUN
            #   3. GET DWH DETAILS COPY FROM CONFORMED 
            #   4. GET ODS REFERENCE TABLES
            elif (workflow == DWH_STAGE):
                if (rerun_flag.lower() == "true"):  
                    rerun_tables = get_non_successful_dwh_runs(batchId)
                    #getting conformed_tables that is in rerun_tables
                    conformed_tables = [item for item in conformed_tables if item in rerun_tables]
                if source.lower()==BISTAGINGSOURCE.lower():
                    operation_copyfromconformed=OPERATION_COPY_CONFORMED_BISTAGING
                else:
                    operation_copyfromconformed=OPERATION_COPY_CONFORMED+source
                    
                dwh_tables = get_dwh_table_details(conformed_tables, batchId, source, glue_config, workflow, operation_copyfromconformed)
                
                #GET ODS CLEANSER
                reference_tables = get_reference_tables('cleanser',source)
                
                LOGGER.info(dwh_tables)

         
            response = {
                'statusCode': 200,
                'batchId': batchId,
                'source': source,
                'workflow': workflow,
                'day0': isDay0,
                'unloadFlag': unload_flag,
                'transformFlag': transform_flag,
                'rerunFlag': rerun_flag,
                'unloadOpertaions': unload_opertaions,
                'conformedTables': conformed_tables_details,
                'dwhTables': dwh_tables,
                'referenceTables': reference_tables,
                'table': preprocess,
                'logStatus': SUCCESSFUL, 
                'logMessage': 'Preprocess Successful',
                'detailedMessage': '', 
                'sendEmail': 'false',
                'updateRunInfo': 'false'
            }
            
        #=============================================================================================
        #=============================================================================================
        #IF NOT DICT, SUBSEQUENT PREPROCESSING FOR CONFORMED/DWH LAYER 
        else:
            LOGGER.info("SUBSEQUENT PREPROCESSING FOR CONFORMED/DWH LAYER")
            new_event = event[0].copy()  
            batchId = new_event['batchId']
            source = new_event['source']
            workflow = new_event['workflow']
            
            #GET GLUE CONFIGS
            table_workflow_data = read_dynamodb_workflows(source, workflow)
            glue_config = get_glue_configs(table_workflow_data)
            
            #GET CONFOMRED TABLES FROM DYNAMODB
            conformed_tables = get_conformed_tables(source)   
            
            # IF CONFORMED
            # 1. GET CONFORMED TABLE DETAILS TO BE TRANSFORMED
            if (workflow == CONFORMED_STAGE):
                conformed_tables_details = get_conformed_table_details(conformed_tables, batchId, source, workflow, glue_config)
                LOGGER.info(conformed_tables_details)
            
                 
            # IF DWH
            # 1. GET SCD DIMENSION TRANSFORM DETAILS
            if (workflow == DWH_STAGE):
                success_table_names = []
                success_tables = [] if ("success_tables" not in new_event) else new_event["success_tables"]
                print(DWH_STAGE)
                print(success_tables)
                failed_tables = [] if ("failed_tables" not in new_event) else new_event["failed_tables"]
                print(failed_tables)
                for table in event:
                    if table["glueJobStatus"] == SUCCESSFUL:
                        success_details = {}
                        success_details["tableName"] = table["tableName"]
                        success_details["glueJobRunId"] = table["glueJobRunId"]
                        success_tables.append(success_details) 
                    else:
                        failed_details = {}
                        failed_details["tableName"] = table["tableName"]
                        failed_details["logMessage"] = table["logMessage"]
                        failed_details["detailedMessage"] = table["detailedMessage"]
                        failed_details["glueJobRunId"] = table["glueJobRunId"]
                        failed_tables.append(failed_details)
                
                for table_details in success_tables:
                    success_table_names.append(table_details["tableName"])
                    
                print(success_table_names)
                # RUN DIMENSIONS IN PARALLEL
                if (new_event["operation"] == OPERATION_COPY_CONFORMED+source or new_event["operation"] == OPERATION_COPY_CONFORMED_BISTAGING):
                    LOGGER.info("PREPARING FOR DIMENSIONS TRANSFORM")
                    if source.lower()==BISTAGINGSOURCE.lower():
                        dimension_operation = "dimension_scd_transform_bistaging"
                    else:
                        dimension_operation = "dimension_scd_transform"+"_"+source
                        
                    dimension_tables = get_fact_or_dimension_tables(dimension_operation)    
                    tables_for_run = [item for item in dimension_tables if item in success_table_names]
                    scd_dimension_tables = get_dwh_table_details(tables_for_run, batchId, source, glue_config, workflow, dimension_operation)
                      
                    #ADD SUCCESS AND FAILED COPY FROM CONFORMED
                    for dim_table in scd_dimension_tables:
                        dim_table["success_tables"] = success_tables
                        dim_table["failed_tables"] = failed_tables
                        
                    if not scd_dimension_tables:
                        scd_dimension_tables = "ALL FAILED COPY"
                        preprocess = failed_tables
                        send_email_indicator = "true"
                        log_status = FAILED
                    
                    #if at least one dimension table failed set logstatus to failure
                    if len(dim_table["failed_tables"]) > 0:
                        scd_dimension_tables = "At least one dimension table failed"
                        preprocess = failed_tables
                        send_email_indicator = "true"
                        log_status = FAILED
                        
                    check_dim_fail_flag=0
                    for dimension_gluestatus in event:
                        LOGGER.info(dimension_gluestatus)
                        print(dimension_gluestatus)
                        if dimension_gluestatus['glueJobStatus']=="FAILED":
                            check_dim_fail_flag=1
                            
                    if check_dim_fail_flag==1:
                        response = {
                            'statusCode': 100,
                            'batchId': batchId,
                            'source': source, 
                            'workflow': workflow,
                            'transformFlag': "true",
                            'conformedTables': conformed_tables_details,
                            'dimensionTables': scd_dimension_tables,
                            "sequentialFactTables": sequential_fact_tables,
                            "table": preprocess,
                            'logStatus': "FAILED",
                            'logMessage': 'Not all dimension tables successful',
                            'detailedMessage': 'One dimension table failed in processing, please check Glue Job log for cleanser',
                            'sendEmail': 'true',
                            'updateRunInfo': 'true' 
                        }
                        
                        return response
                     
                # RUN FACT SEQUENTIALLY
                if new_event["operation"] == "dimension_scd_transform_" + source or new_event["operation"] == "dimension_scd_transform_bistaging":
                    LOGGER.info("PREPARING FOR SEQUENTIAL FACT TRANSFORM")
                    
                    #check if dimensions are all successful
                    check_dim_fail_flag=0
                    for dimension_gluestatus in event:
                        LOGGER.info(dimension_gluestatus)
                        print(dimension_gluestatus)
                        if dimension_gluestatus['glueJobStatus']=="FAILED":
                            check_dim_fail_flag=1
                    
                    if check_dim_fail_flag==1:
                        response = {
                            'statusCode': 100,
                            'batchId': batchId,
                            'source': source, 
                            'workflow': workflow,
                            'transformFlag': "true",
                            'conformedTables': conformed_tables_details,
                            'dimensionTables': scd_dimension_tables,
                            "sequentialFactTables": sequential_fact_tables,
                            "table": preprocess,
                            'logStatus': "FAILED",
                            'logMessage': 'Not all dimension tables successful',
                            'detailedMessage': 'One dimension table failed in processing, please check Glue Job log for cleanser',
                            'sendEmail': 'true',
                            'updateRunInfo': 'true' 
                        }
                        
                        return response
                        
                    
                    if source.lower()==BISTAGINGSOURCE.lower():
                        fact_operation = "fact_transform_bistaging"
                    else:
                        fact_operation = "fact_transform"
                    
                    fact_tables = get_fact_or_dimension_tables(fact_operation)
                    tables_for_run = [item for item in fact_tables if item in success_table_names]
                    tables_for_run.append("bookingchannel")
                    sequential_fact_tables = get_dwh_table_details(tables_for_run, batchId, source, glue_config, workflow, fact_operation)
                    
                    #ADD SUCCESS AND FAILED DIMENSION TRANFORM
                    for fact_table in sequential_fact_tables:
                        fact_table["success_tables"] = success_tables
                        fact_table["failed_tables"] = failed_tables
                         
                         
            response = {
                'statusCode': 200,
                'batchId': batchId,
                'source': source, 
                'workflow': workflow,
                'transformFlag': "true",
                'conformedTables': conformed_tables_details,
                'dimensionTables': scd_dimension_tables,
                "sequentialFactTables": sequential_fact_tables,
                "table": preprocess,
                'logStatus': log_status,
                'logMessage': 'Preprocess Successful',
                'detailedMessage': '',
                'sendEmail': send_email_indicator,
                'updateRunInfo': 'false' 
            }
        return response

    except Exception as e:
        error_stack_trace = traceback.format_exc()
        error_log_message = '{errorClass}: {errorMessage}'.format(errorClass = str(e.__class__), errorMessage = str(e))
        error_message = '{errorLogMessage} - {errorStackTrace}'.format(errorLogMessage = error_log_message, errorStackTrace = error_stack_trace)
        LOGGER.error('Error in pre-processing Conformed layer: {error}'.format(error = e))
        response = {
            'batchId': batchId, 
            'source': source,
            'table': 'preprocess',
            'workflow': workflow,
            'logStatus': FAILED,
            'logMessage': error_log_message,
            'detailedMessage': error_stack_trace,
            'updateRunInfo': 'false',
            'sendEmail': 'true'
        }
        LOGGER.info("Calling cebu-dwh-status-logging.")
        LAMBDA_CLIENT.invoke(
            FunctionName=LAMBDA_STATUS_LOGGING,
            InvocationType='RequestResponse',
            Payload = json.dumps(response)
        )
        return response