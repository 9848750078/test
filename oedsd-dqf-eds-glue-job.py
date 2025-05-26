import pandas as pd
import sys
import requests
import re
import io
import os
import json
import requests
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import logging
import boto3
from pyspark.sql import functions as F
from datetime import datetime
import numpy as np
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, array, when, lit, trim
from pyspark.sql.functions import col, when, trim, lit, isnull
from pyspark.sql import DataFrame
from functools import reduce
from pyspark.sql.functions import col, trim, regexp_replace
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from botocore.exceptions import ClientError
from urllib.parse import quote
from urllib.parse import unquote
import pyspark.sql.functions as F
from pyspark.sql.functions import monotonically_increasing_id, broadcast

current_time = datetime.now()

MSG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger("ODS-LMS")
logger.setLevel(logging.INFO)


# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("DataValidation").getOrCreate()
sqlContext = SQLContext(spark.sparkContext)

s3_client = boto3.client('s3')
ses_client = boto3.client('ses')

'''args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "source_prefix", "file_datetime", "S3_bucket_to_directory", "Database_to_schema","Schema_to_database_view", "secret_name", 'file_prefix', 'community_id', 'relation_type_id','Is_Nullable', 'domain_id_raw', 'raw_table_name', 'Database_view_to_column', 'Directory_to_file_group','File_group_to_table', 'source_bucket_raw', 'reject_bucket_name','reject_base_path_soft','hard_base_path','pipeline'
])'''

args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "source_prefix", "target_bucket", "target_prefix", "file_datetime", "file_name", 
    "schema_name", "Input_param_dynamodb", "pipeline", "S3_bucket_to_directory", "Database_to_schema",
    "Schema_to_database_view", "secret_name", 'file_prefix', 'community_id', 'relation_type_id', 
    'Is_Nullable', 'domain_id_raw', 'raw_table_name', 'Database_view_to_column', 'Directory_to_file_group',
    'File_group_to_table', 'source_bucket_raw', 'reject_bucket_name', 'reject_base_path','reject_base_path_soft','hard_base_path'
])

# Extract and initialize job parameters
job_name = args["JOB_NAME"]
schema_name = args["schema_name"]
reject_base_path = args["reject_base_path"]
filename = args["file_name"]
Input_param_dynamodb = args["Input_param_dynamodb"]
target_bucket = args["target_bucket"]
target_prefix = args["target_prefix"]
pipeline = args["pipeline"]
hard_base_path = args["hard_base_path"]
reject_base_path_soft = args["reject_base_path_soft"]
#reject_base_path_schema = args["reject_base_path_schema"]
reject_bucket_name = args["reject_bucket_name"]
source_prefix = args["source_prefix"]
file_datetime = args["file_datetime"]
S3_bucket_to_directory = args["S3_bucket_to_directory"]
Schema_to_database_view = args["Schema_to_database_view"]
source_bucket_raw = args['source_bucket_raw'].upper()
Database_to_schema = args['Database_to_schema']
Directory_to_file_group = args['Directory_to_file_group']
File_group_to_table = args['File_group_to_table']
Database_view_to_column = args['Database_view_to_column']
raw_table_name = args['raw_table_name'].upper()
domain_id_raw = args['domain_id_raw']
file_prefix = args['file_prefix'].upper()
COMMUNITY_ID = args['community_id']
relation_type_id = args['relation_type_id']
Is_Nullable = args['Is_Nullable']
secret_name = args['secret_name']


logger.info(f"file_datetime : {file_datetime}")
logger.info(f"source_prefix : {source_prefix}")
logger.info(f"source_bucket_raw : {source_bucket_raw}")

target_prefix = target_prefix+'/'+filename+'/'+file_datetime
print("100_target_prefix:",target_prefix)

client = boto3.client("secretsmanager", region_name="us-east-1")
get_secret_value_response = client.get_secret_value(SecretId=args['secret_name'])
secret = json.loads(get_secret_value_response['SecretString'])
collibra_url = secret.get('url')
collibra_auth = secret.get('Authorization')
content_type = secret.get('Content_Type')

HEADERS = {
    'Authorization': collibra_auth,
    'Content-Type': content_type
}

# Collibra API URL and headers (You should replace these with actual values)
#collibra_url = "https://your-collibra-url"
#HEADERS = {
#    "Authorization": "Bearer your-token",
#    "Content-Type": "application/json"
#}

# Validate column count between expected schema and actual DataFrame


# Validate column count between expected schema and actual DataFrame
def validate_column_count(df, expected_columns, reject_base_path_soft, reject_bucket_name):
    """Check if column count matches and store errors in S3 if not."""
    expected_len = len(expected_columns)
    actual_len = len(df.columns)  # Use df.columns to get the number of columns
    
    # Identify missing columns
    missing_columns = [col for col in expected_columns if col not in df.columns]
    
    # Print the missing columns and DataFrame columns
    print(f"Missing columns: {missing_columns}")
    print(f"DataFrame columns: {df.columns}")
    
    try:
        if expected_len != actual_len:
            # Raise an exception with the details
            raise Exception(f"Column count mismatch! Expected {expected_len}, but got {actual_len}. Missing columns: {missing_columns}")
        
        logger.info(f"Column count validation passed. Both datasets have {expected_len} columns.")
        return expected_len, actual_len, missing_columns  # Return lengths and missing columns to main
    
    except Exception as e:
        # Prepare the error message
        error_details = {
            "error_message": str(e),
            "expected_column_count": expected_len,
            "actual_column_count": actual_len,
            "missing_columns": missing_columns
        }
        
        # Convert error details to JSON string
        error_details_json = json.dumps(error_details)
        
        # Store the error details in the specified S3 bucket
        s3_key = f"{reject_base_path_soft}validate_column_count/column_count_error.json"
        s3_client.put_object(
            Bucket=reject_bucket_name,
            Key=s3_key,
            Body=error_details_json,
            ContentType='application/json'
        )
        
        print(f"Error details stored in S3 at: s3://{reject_bucket_name}/{s3_key}")
        logger.error(f"Column count mismatch details have been stored in S3 at: s3://{reject_bucket_name}/{s3_key}")
        
        # Raise the exception to fail the Glue job
        raise e


# Checks if the dataset is empty
def check_no_data(df):
    if df.count() == 0:
        logger.error("Dataset contains no data.")
        sys.exit(1)  # Exit the job with failure code (1) when the dataset is empty
    logger.info("Dataset contains data.")
    return True

# Reads data from a file (CSV or JSON) into a Spark DataFrame
'''def read_data(file_path, file_type):
    """
    Reads data from a file (CSV or JSON) into a Spark DataFrame.
    Supports CSV and JSON formats.
    """
    if file_type.lower() == "csv":
        # Read CSV with mode "DROPMALFORMED" to drop malformed records
        df = sqlContext.read.option("header", "true").option("mode", "DROPMALFORMED").csv(file_path)
    elif file_type.lower() == "json":
        # Read JSON with mode "DROPMALFORMED" to drop malformed records
        df = sqlContext.read.option("mode", "DROPMALFORMED").json(file_path)
        print("97_df:",df)
    else:
        raise Exception("Unsupported file type. Please use CSV or JSON.")
    
    return df'''

def read_data(file_path, variable):
    """
    Reads data from a file (CSV or JSON) into a Spark DataFrame.
    Supports CSV and JSON formats.
    """
    try:
        if variable.lower().endswith('.csv'):
            # Read CSV with mode "DROPMALFORMED" to drop malformed records
            print(f"Reading CSV file from: {file_path}")
            df = spark.read.csv(f"{file_path}/{variable}", header=True)
            print(f"JSON DataFrame data: {df}")
            print(f"JSON DataFrame schema: {df.printSchema()}")
            print(f"JSON DataFrame row count: {df.count()}")
        elif variable.lower().endswith('.json'):
            # Read JSON with mode "DROPMALFORMED" to drop malformed records
            print(f"Reading JSON file from: {file_path}")
            df = spark.read.json(f"{file_path}/{variable}")
            print(f"JSON DataFrame data: {df}")
            print(f"JSON DataFrame schema: {df.printSchema()}")
            print(f"JSON DataFrame row count: {df.count()}")
        else:
            raise Exception("Unsupported file type. Please use CSV or JSON.")

        # Log DataFrame schema and row count
        if df.count() > 0:
            print(f"DataFrame schema: {df.printSchema()}")
            print(f"Number of rows: {df.count()}")
        else:
            print("Warning: The DataFrame is empty. Please check the file content.")

        return df

    except Exception as e:
        print(f"Error while reading file {file_path} of type {variable}: {str(e)}")
        return None


# Get table ID from Collibra based on table name
def get_table_id_from_collibra(table_name, DOMAIN_ID, COMMUNITY_ID):
    params = {
        'offset': '0',
        'limit': '0',
        'countLimit': '-1',
        'name': table_name,
        'nameMatchMode': 'EXACT',
        'domainId': DOMAIN_ID,
        'communityId': COMMUNITY_ID,
        'typeInheritance': 'true',
        'excludeMeta': 'true',
        'sortField': 'NAME',
        'sortOrder': 'ASC',
    }
    
    response = requests.get(f'{collibra_url}/assets', params=params, headers=HEADERS)
    
    if response.status_code == 200:
        response_data = response.json()
        if response_data['results']:
            return response_data['results'][0]['id']
    else:
        logger.error(f"Failed to fetch table ID: {response.text}")
        raise Exception(f"Failed to fetch table ID for '{table_name}'. Status Code: {response.status_code}")

    return None

# Get existing columns from Collibra based on table ID and relation type
def get_existing_columns_from_collibra(table_id, relation_type_id, fetch_details=False):
    params = {
        'offset': '0',
        'limit': '0',
        'countLimit': '-1',
        'relationTypeId': relation_type_id,
        'targetId': table_id,
        'sourceTargetLogicalOperator': 'AND',
    }
    response = requests.get(f'{collibra_url}/relations', params=params, headers=HEADERS)
    
    if response.status_code == 200:
        response_data = response.json()
        
        # If only names are needed
        if not fetch_details:
            column_names = [relation['source']['name'] for relation in response_data['results']]
            logger.info(f"Fetched column names from Collibra: {column_names}")
            return column_names

        # If detailed attributes are also needed
        existing_columns_and_data_types = []
        result_dict = {relation['source']['id']: relation['source']['name'] for relation in response_data['results']}
        logger.info(f"Result dict (columns from Collibra): {result_dict}")
        
        # Fetch detailed attributes for each column
        for col_id, col_name in result_dict.items():
            col_name_upper = col_name.upper()
            technical_data_type = None
            nullable = None  # Default to True if nullable attribute is missing
            
            # Fetch attributes for each column (asset)
            attributes_response = requests.get(f'{collibra_url}/attributes', params={'assetId': col_id}, headers=HEADERS)
            
            if attributes_response.status_code == 200:
                attributes_data = attributes_response.json()
                
                # Extract data type and nullable attributes
                for attribute in attributes_data.get('results', []):
                    if attribute['type']['name'] == 'Technical Data Type':
                        technical_data_type = attribute['value']
                    if attribute['type']['name'] == 'Is Nullable':
                        nullable = attribute['value']
            
            if technical_data_type is None:
                technical_data_type = 'Data type missing in Collibra'
            
            existing_columns_and_data_types.append((col_name_upper, technical_data_type, nullable))
            logger.info(f"Processed Column: {col_name_upper}, Type: {technical_data_type}, Nullable: {nullable}")
        
        logger.info(f"Fetched columns, data types, and nullability from Collibra: {existing_columns_and_data_types}")
        return existing_columns_and_data_types
    else:
        logger.error(f"Failed to fetch columns from Collibra: {response.text}")
        return []

def check_hard_reject_and_store(df, reject_bucket_name, reject_base_path_soft):
    """
    Function to perform hard reject validation and store results in S3.
    """
    try:
        # Debug: Print the type of 'df' to see what is being passed
        print(f"Type of 'df' inside function: {type(df)}")
        
        # Check if df is already a DataFrame
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Input data is not a pandas DataFrame.")
        
        print("224_df:", df)

        # Hard Reject condition: If any column has all missing values across all rows
        empty_columns = df.columns[df.isnull().all()]
        print("227_empty_columns:", empty_columns)

        # If hard reject condition is met, stop the job and store the column name in S3
        if not empty_columns.empty:
            column_name = empty_columns.tolist()[0]  # Get the first column with all missing values
            print("232_column_name:", column_name)

            # Initialize the S3 client
            s3 = boto3.client('s3')

            # Prepare the data to store
            #data_to_store = {'column_name': column_name}
            data_to_store = {
                'column_name': column_name,
                'null_count': int(df[column_name].isna().sum()),  # Convert to Python int
                'empty_count': int(df[column_name].eq('').sum()),  # Convert to Python int
                'row_count': int(len(df)),  # Convert to Python int
            }


            # Upload the column name to S3 as a JSON file
            s3_key = f"{reject_base_path_soft}check_col_check/column_data_check.json"
            s3.put_object(
                Bucket=reject_bucket_name,
                Key=s3_key,
                Body=json.dumps(data_to_store),
                ContentType='application/json'
            )
            print(f"Column name '{column_name}' has been stored in S3.")
            return f"Job Stopped: The column '{column_name}' has all missing data. Stored in S3."
        
        # If no column has missing data for all rows, continue with the job (return success)
        return "No Hard Reject: No column has missing data for all rows. Continuing job."
    
    except Exception as e:
        print(f"Error in check_hard_reject_and_store: {str(e)}")
        return f"Error: {str(e)}"

'''def check_column_number_mismatch_spark(df, reject_bucket_name, reject_base_path):
    """
    Check rows for column number mismatch using PySpark DataFrame, store rejected rows in S3, and return count.
    Args:
        df (DataFrame): Input DataFrame containing the data.
        reject_bucket_name (str): S3 bucket to store rejected rows.
        reject_base_path (str): Base path in S3 for rejected rows.
    Returns:
        int: Count of rows with column number mismatch.
    """
    # Extract column names and calculate their count
    column_list = df.columns
    column_count = len(column_list)
    
    # Add column count for each row
    df = df.withColumn("column_count", F.size(F.split(F.concat_ws(",", *df.columns), ",")))
    print("302_df:",df)

    # Prepare metadata with column names and count
    metadata = {
        "column_list": ", ".join(column_list),
        "column_count": column_count
    }

    # Convert DataFrame to JSON format for mismatched rows
    mismatched_rows = df.filter(F.col("column_count") != column_count)
    print("312_mismatched_rows:",mismatched_rows)
    mismatch_count = mismatched_rows.count()

    # Store mismatched rows with metadata
    if mismatch_count > 0:
        mismatched_data = mismatched_rows.toPandas().to_dict(orient="records")
        print("318_mismatched_data:",mismatched_data)
        output_data = {
            "_1": [metadata] + mismatched_data
        }

        # Convert output_data to JSON and store in S3
        json_data = json.dumps(output_data, indent=2)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        s3_key = f"{reject_base_path}/{timestamp}/column_row_mismatch.json"
        s3_client.put_object(
            Bucket=reject_bucket_name,
            Key=s3_key,
            Body=json_data,
            ContentType="application/json"
        )
        print(f"Rejected rows stored in S3: s3://{reject_bucket_name}/{s3_key}")
    else:
        print("No column number mismatches found.")

    return mismatch_count'''

'''def validate_column_order(expected_columns, actual_columns, reject_bucket_name, hard_base_path,variable,src_path):
    """Ensures that the order of the columns matches the expected schema, logs mismatches, and stores them in S3. 
    Exits with code 1 to fail the job when mismatches are found."""
    mismatched_columns = []
    
    for idx, (expected, actual) in enumerate(zip(expected_columns, actual_columns)):
        if expected != actual:
            logger.error(f"Column order mismatch: Expected '{expected}' at position {idx}, but found '{actual}'")
            mismatched_columns.append((expected, actual, idx))
        else:
            logger.info(f"Column '{expected}' at position {idx} is in correct order.")
    
    # If mismatches are found, log and store them in S3, then exit with failure code (1)
    if mismatched_columns:
        logger.error(f"Total column position mismatches: {len(mismatched_columns)}")
        s3 = boto3.client('s3')
        
        # Prepare data to store in S3
        data_to_store = {
            'path': src_path,
            'file_name': variable
            'mismatched_columns': [
                {
                    'expected': expected,
                    'actual': actual,
                    'position': idx
                }
                for expected, actual, idx in mismatched_columns
            ]
        }

        # Define the S3 key where data will be stored
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        s3_key = f"{pipeline}/Hard_reject/col_position_mis_match/{timestamp}/column_order_mismatches.json"

        # Upload the mismatch data to S3
        try:
            s3.put_object(
                Bucket=reject_bucket_name,
                Key=s3_key,
                Body=json.dumps(data_to_store),
                ContentType='application/json'
            )
            logger.info(f"Column order mismatch data has been stored in S3 at {s3_key}")
        except Exception as e:
            logger.error(f"Error uploading mismatch data to S3: {str(e)}")
            # Exit with failure code (1) on S3 upload failure
            sys.exit(1)

        # Log the mismatch and exit with failure code (1)
        logger.error(f"Column position mismatch detected. {len(mismatched_columns)} mismatch(es) found.")
        sys.exit(1)  # Exit with failure code to indicate the job failed
    
    else:
        logger.info("No column position mismatches found.")
    
    return mismatched_columns  # Return mismatched columns for further handling if necessary'''
    

def validate_column_order(expected_columns, actual_columns, reject_bucket_name, hard_base_path, variable, src_path):
    """Ensures that the order of the columns matches the expected schema, logs mismatches, and stores them in S3. 
    Exits with code 1 to fail the job when mismatches are found."""
    mismatched_columns = []
    
    for idx, (expected, actual) in enumerate(zip(expected_columns, actual_columns)):
        if expected != actual:
            logger.error(f"Column order mismatch: Expected '{expected}' at position {idx}, but found '{actual}'")
            mismatched_columns.append((expected, actual, idx))
        else:
            logger.info(f"Column '{expected}' at position {idx} is in correct order.")
    
    # If mismatches are found, log and store them in S3, then exit with failure code (1)
    if mismatched_columns:
        logger.error(f"Total column position mismatches: {len(mismatched_columns)}")
        s3 = boto3.client('s3')
        
        # Prepare data to store in S3
        data_to_store = {
            'path': src_path,
            'file_name': variable,
            'mismatched_columns': [
                {
                    'expected': expected,
                    'actual': actual,
                    'position': idx
                }
                for expected, actual, idx in mismatched_columns
            ]
        }

        # Log the mismatched columns before storing them to S3
        logger.info("Mismatched columns to be stored in S3:")
        logger.info(json.dumps(data_to_store, indent=4))  # Log the entire mismatch data that will be stored

        # Define the S3 key where data will be stored
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        s3_key = f"{pipeline}/Hard_reject/col_position_mis_match/{timestamp}/column_order_mismatches.json"

        # Upload the mismatch data to S3
        try:
            s3.put_object(
                Bucket=reject_bucket_name,
                Key=s3_key,
                Body=json.dumps(data_to_store),
                ContentType='application/json'
            )
            logger.info(f"Column order mismatch data has been stored in S3 at {s3_key}")
        except Exception as e:
            logger.error(f"Error uploading mismatch data to S3: {str(e)}")
            # Exit with failure code (1) on S3 upload failure
            sys.exit(1)

        # Log the mismatch and exit with failure code (1)
        logger.error(f"Column position mismatch detected. {len(mismatched_columns)} mismatch(es) found.")
        
        # Email Notification Logic
        subject = f"Column Order Mismatch Detected in {variable}"
        body = f"Column order mismatches have been detected for the dataset.\n\nDetails:\n" \
               f"Source Path: {src_path}\n" \
               f"Variable: {variable}\n" \
               f"Total Mismatches: {len(mismatched_columns)}\n\n" \
               f"Please review the S3 location for the mismatch details.\n\n" \
               f"S3 Location: s3://{reject_bucket_name}/{s3_key}"

        send_email(subject, body)
        
        # Exit with failure code after sending the email
        sys.exit(1)  # Exit with failure code to indicate the job failed
    
    else:
        logger.info("No column position mismatches found.")
    
    return mismatched_columns  # Return mismatched columns for further handling if necessary
    
def handle_missing_columns(data):
    """
    Ensures all rows in the dataset have the same columns.
    Adds missing columns with empty string ("") as default values.
    
    Args:
        data: List of dictionaries representing the dataset.
    
    Returns:
        A new dataset with consistent columns and empty strings for missing fields.
    """
    # Determine the full set of keys (columns) across all rows
    all_keys = set(key for row in data for key in row.keys())
    
    # Fill missing keys with empty strings for each row
    updated_data = [
        {key: row.get(key, "") if key not in row or row[key] is None else row[key] for key in all_keys}
        for row in data
    ]
    return updated_data

# Function to get table ID from Collibra
def get_table_id_from_collibra(table_name, DOMAIN_ID, COMMUNITY_ID):
    params = {
        'offset': '0',
        'limit': '0',
        'countLimit': '-1',
        'name': table_name,
        'nameMatchMode': 'EXACT',
        'domainId': DOMAIN_ID,
        'communityId': COMMUNITY_ID,
        'typeInheritance': 'true',
        'excludeMeta': 'true',
        'sortField': 'NAME',
        'sortOrder': 'ASC',
    }
    
    # Sending GET request to Collibra API
    response = requests.get(f'{collibra_url}/assets', params=params, headers=HEADERS)
    
    # Check if the response status code is 200
    if response.status_code == 200:
        response_data = response.json()
        if response_data['results']:
            return response_data['results'][0]['id']
    else:
        # Log the error and raise an exception to fail the job
        logger.error(f"Failed to fetch table ID: {response.text}")
        raise Exception(f"Failed to fetch table ID for '{table_name}'. Status Code: {response.status_code}")

    return None

# Function to get existing column names and attributes from Collibra
def get_existing_columns_from_collibra(table_id, relation_type_id, fetch_details=False):
    params = {
        'offset': '0',
        'limit': '0',
        'countLimit': '-1',
        'relationTypeId': relation_type_id,
        'targetId': table_id,
        'sourceTargetLogicalOperator': 'AND',
    }
    response = requests.get(f'{collibra_url}/relations', params=params, headers=HEADERS)
    
    if response.status_code == 200:
        response_data = response.json()
        
        # If only names are needed
        if not fetch_details:
            column_names = [relation['source']['name'] for relation in response_data['results']]
            logger.info(f"Fetched column names from Collibra: {column_names}")
            print("column_names:",column_names)

        # If detailed attributes are also needed
        existing_columns_and_data_types = []
        result_dict = {relation['source']['id']: relation['source']['name'] for relation in response_data['results']}
        logger.info(f"Result dict (columns from Collibra): {result_dict}")
        
        # Fetch detailed attributes for each column
        for col_id, col_name in result_dict.items():
            col_name_upper = col_name.upper()
            technical_data_type = None
            nullable = None  # Default to True if nullable attribute is missing
            
            # Fetch attributes for each column (asset)
            attributes_response = requests.get(f'{collibra_url}/attributes', params={'assetId': col_id}, headers=HEADERS)
            
            if attributes_response.status_code == 200:
                attributes_data = attributes_response.json()
                #print("attributes_data:",attributes_data)
                
                # Extract data type and nullable attributes
                for attribute in attributes_data.get('results', []):
                    if attribute['type']['name'] == 'Technical Data Type':
                        technical_data_type = attribute['value']
                        print("technical_data_type:",technical_data_type)
                    if attribute['type']['name'] == 'Is Nullable':
                        nullable = attribute['value']
                        print("nullable:",nullable)
            
            # Set default for missing technical data type
            if technical_data_type is None:
                technical_data_type = 'Data type missing in Collibra'
            
            existing_columns_and_data_types.append((col_name_upper, technical_data_type, nullable))
            logger.info(f"Processed Column: {col_name_upper}, Type: {technical_data_type}, Nullable: {nullable}")
        
        logger.info(f"Fetched columns, data types, and nullability from Collibra: {existing_columns_and_data_types}")
        return existing_columns_and_data_types
        
def handle_missing_columns_and_store(df, reject_bucket_name, reject_base_path_soft):
    """
    Identifies rows with missing columns, fills the missing columns with empty strings,
    and uploads those rows to S3. Does not consider rows with null (`None`) values.
    
    Args:
        data: Pandas DataFrame or list of dictionaries representing the dataset.
        bucket_name: Name of the S3 bucket.
        key: S3 object key (file path).
        reject_bucket_name: S3 bucket for rejected rows with missing columns.
        reject_base_path: Base path in the S3 bucket for rejected data.
    
    Returns:
        DataFrame with consistent columns (missing fields filled with empty strings).
    """
    # Convert to DataFrame if input is a list of dictionaries
    if isinstance(df, list):
        df = pd.DataFrame(df)
    elif isinstance(df, pd.DataFrame):
        df = df.copy()
    else:
        raise ValueError("Input data must be a Pandas DataFrame or a list of dictionaries.")
    
    # Determine the full set of columns across all rows (union of columns)
    all_columns = set(df.columns)
    print("407_all_col:",all_columns)
    
    # Identify rows with missing columns (i.e., columns that are not present in the row)
    missing_columns_df = df.loc[df.isnull().any(axis=1)].copy()  # Rows with any NaN value (missing columns)
    print("411_missing_columns_df:",missing_columns_df)

    # Fill missing columns with empty strings for these rows
    for col in missing_columns_df.columns:
        missing_columns_df[col] = missing_columns_df[col].fillna("")

    # Upload rows with missing columns to S3 if they exist
    if not missing_columns_df.empty:
        # Convert to JSON
        json_data = json.dumps(missing_columns_df.to_dict(orient="records"), indent=2)
        
        # Generate a timestamp for the S3 object path
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        s3_key = f"{reject_base_path_soft}/{timestamp}/column_row_mismatch.json"
        
        # Upload to the S3 bucket
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket=reject_bucket_name,
            Key=s3_key,
            Body=json_data,
            ContentType="application/json"
        )
        
        print(f"Rejected rows stored in S3: s3://{reject_bucket_name}/{s3_key}")
    else:
        print("No rows with missing columns found.")
    
    # Return the updated DataFrame with all rows filled with empty strings for missing columns
    return df

def handle_null_values_and_store(df, reject_bucket_name, reject_base_path_soft):
    """
    Identifies rows with null (None or NaN) values, fills those null values with empty strings,
    and uploads those rows to S3.
    
    Args:
        df: Pandas DataFrame containing the dataset.
        bucket_name: Name of the S3 bucket.
        key: S3 object key (file path).
    
    Returns:
        DataFrame with null values replaced by empty strings.
    """
    # Identify rows with any null (None or NaN) value
    null_rows_df = df[df.isnull().any(axis=1)].copy()

    # Replace null values with empty strings
    for col in null_rows_df.columns:
        null_rows_df[col] = null_rows_df[col].fillna("")

    # If there are rows with null values, upload them to S3
    if not null_rows_df.empty:
        s3_client = boto3.client('s3')
        json_data = json.dumps(null_rows_df.to_dict(orient="records"), indent=2)
        
        # Create timestamp for the file name
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        s3_key = f"{reject_base_path_soft}/{timestamp}/null_records.json"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_data,
            ContentType="application/json"
        )
        
        print(f"Rows with null values stored in S3: s3://{reject_bucket_name}/{s3_key}")
    else:
        print("No rows with null values found.")

    # Return the updated DataFrame with null values replaced by empty strings
    return df

def handle_missing_values_and_store_in_s3(df, reject_bucket_name, reject_base_path_soft):
    """
    Filters rows with missing (null or empty) values and stores them in S3.

    :param df: Input DataFrame
    :param reject_bucket_name: S3 bucket name for storing rejected rows
    :param reject_base_path: S3 base path for storing rejected rows
    """
    # Filter rows with missing values in any column (either None, NaN, or empty strings)
    missing_values_df = df[df.isnull().any(axis=1) | (df == '').any(axis=1)]
    print("missing_values_df:",missing_values_df)
    
    # If there are rows with missing values
    if not missing_values_df.empty:
        missing_values_json = missing_values_df.to_dict(orient="records")
        
        # Generate a timestamped S3 key
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        s3_key = f"{reject_base_path_soft}/{timestamp}/missing_values_rows.json"
        
        # Upload to S3
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket=reject_bucket_name,
            Key=s3_key,
            Body=json.dumps(missing_values_json, indent=4),
            ContentType="application/json"
        )
        
        print(f"Rows with missing values stored in S3: s3://{reject_bucket_name}/{s3_key}")
    else:
        print("No rows with missing values found.")


def filter_rows_with_null_or_missing_values(df, reject_bucket_name, reject_base_path_soft):
    """
    Filters rows with null or missing values and stores them in S3.
    
    :param df: Input DataFrame.
    :param reject_bucket_name: S3 bucket name for storing rows with missing values.
    :param reject_base_path: S3 base path for storing rows with missing values.
    """
    # Identify rows with null or missing values
    rows_with_missing_values = df[df.isnull().any(axis=1) | (df == "").any(axis=1)]

    # Save rows with missing values to S3 if any exist
    if not rows_with_missing_values.empty:
        # Convert DataFrame to JSON format (using `to_dict` for proper serialization)
        rows_with_missing_values_json = rows_with_missing_values.to_dict(orient="records")

        # Generate a timestamped S3 key
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        s3_key = f"{reject_base_path_soft}/{timestamp}/null_or_missing_rows.json"

        # Upload to S3 using boto3
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket=reject_bucket_name,
            Key=s3_key,
            Body=json.dumps(rows_with_missing_values_json, indent=4),
            ContentType="application/json"
        )

        print(f"Rows with null/missing values stored in S3: s3://{reject_bucket_name}/{s3_key}")
    else:
        print("No rows with null/missing values found.")


def identify_null_mismatch(df, reject_bucket_name, reject_base_path_soft):
    expected_columns = set(df.columns)
    logger.info("Expected columns: %s", expected_columns)
    mismatch_rows = []
    for index, row in df.iterrows():
        row_columns = set(row.dropna().index)  # Get non-null column names for this row
        print("row_columns:", row_columns)
        if row_columns != expected_columns:
            # Add the 'index' to the row dictionary before appending it
            mismatch_row = row.to_dict()
            print("mismatch_row:", mismatch_row)
            mismatch_row['index'] = index  # Explicitly add 'index'
            mismatch_rows.append(mismatch_row)  # Append the entire row with 'index'
    print("mismatch_rows:", mismatch_rows)
    
    logger.info("Mismatch rows: %s", json.dumps(mismatch_rows, indent=4))
    return mismatch_rows


def identify_missing_values(df, id_null_mismatch, reject_bucket_name, reject_base_path_soft):
    null_missing_rows = []
    # Collect indices of the mismatch rows correctly
    mismatch_indices = [row['index'] for row in id_null_mismatch]  # Adjusted to use 'index' field
    print("829_mismatch_indices:",mismatch_indices)
    logger.info("Mismatch indices: %s", mismatch_indices)

    for index, row in df.iterrows():
        # Skip rows that have already been flagged as column mismatched
        if index in mismatch_indices:
            continue
        
        # Check for null or empty string values
        if row.isnull().any() or (row == "").any():
            null_missing_rows.append(row.to_dict())  # Convert row to dict for JSON serialization
            print("840_null_missing_rows:",null_missing_rows)
    logger.info("Missing rows: %s", json.dumps(null_missing_rows, indent=4))
    return null_missing_rows

def send_email(subject, body):
    """Function to send an email using AWS SES."""
    try:
        response = ses_client.send_email(
            Source='no.reply@omf.com',
            Destination={
                'ToAddresses': ['saikumar.ankam.ce@omf.com'],
            },
            Message={
                'Subject': {
                    'Data': subject,
                },
                'Body': {
                    'Text': {
                        'Data': body,
                    }
                }
            }
        )
        logger.info(f"Email sent successfully to {recipient_email}. Message ID: {response['MessageId']}")
    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")



'''def write_combined_parquet(df, id_null_mismatch, id_missing_values, reject_bucket_name, reject_base_path_soft,variable,src_path):
    # Combine mismatch and missing rows into a single list
    combined_rows = id_null_mismatch + id_missing_values
    logger.info("Total combined rows: %d", len(combined_rows))
    logger.info("combined_rows rows: %s", json.dumps(combined_rows, indent=4))
    
    logger.info("Combined rows to be stored in S3: %s", json.dumps(combined_rows, indent=4))

    # Start Spark session
    spark = SparkSession.builder \
        .appName("MismatchedAndMissingRowsToParquet") \
        .getOrCreate()

    # Convert combined rows into a Spark DataFrame
    combined_df_spark = spark.createDataFrame(combined_rows)
    
    # Ensure we have only one partition to write into a single file
    combined_df_spark = combined_df_spark.coalesce(1)

    # Generate a timestamped S3 key for the directory (auto-file naming by Spark)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    s3_dir_path = f"{pipeline}/Soft_reject/{timestamp}/"

    # Write the Spark DataFrame to a Parquet file directly to S3
    combined_df_spark.write \
        .mode("overwrite") \
        .parquet(f"s3a://{reject_bucket_name}/{s3_dir_path}")

    logger.info("Combined rows with null/missing values stored in S3: s3://%s/%s", reject_bucket_name, s3_dir_path)'''

def write_combined_parquet(df, id_null_mismatch, id_missing_values, reject_bucket_name, reject_base_path_soft, variable, src_path, pipeline):
    try:
        # Check if DataFrames are empty
        if id_null_mismatch.isEmpty() and id_missing_values.isEmpty():
            logger.info("No data to process. Exiting the job.")
            return  # Exit if there are no combined rows

        # Combine the DataFrames
        combined_df_spark = id_null_mismatch.union(id_missing_values)
        logger.info("Total rows in combined DataFrame: %d", combined_df_spark.count())
        
        # Ensure we have only one partition to write into a single file
        combined_df_spark = combined_df_spark.coalesce(1)
        
        # Generate a timestamped S3 key for the directory (auto-file naming by Spark)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        s3_dir_path = f"{pipeline}{reject_base_path_soft}/{timestamp}/"
        logger.info("S3 Directory Path: %s", s3_dir_path)
        
        # Write the Spark DataFrame to a Parquet file directly to S3
        combined_df_spark.write \
            .mode("overwrite") \
            .parquet(f"s3a://{reject_bucket_name}/{s3_dir_path}")

        logger.info("Combined rows with null/missing values stored in S3: s3://%s/%s", reject_bucket_name, s3_dir_path)

        # Send email notification with details
        subject = f"null/missing records Stored for Pipeline: {pipeline}"
        body = f"null/missing rows have been successfully stored in S3.\n\nDetails:\n\n" \
               f"Source_Path: {src_path}\n" \
               f"file_name: {variable}\n" \
               f"S3 Location: s3://{reject_bucket_name}/{s3_dir_path}"

        send_email(subject, body)

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

'''def identify_issues(glue_df: DataFrame) -> tuple:
    """
    Identifies rows with missing values (null, None, empty strings) and handles them in both JSON and CSV data formats.
    
    Parameters:
    glue_df (DataFrame): The Glue DataFrame to process.

    Returns:
    tuple: Three DataFrames:
        - mismatch_rows: Rows with column mismatches or missing values.
        - missing_value_rows: Rows with null, None, empty string values.
        - clean_df: The DataFrame after removing mismatch_rows and missing_value_rows.
    """
    
    # Get all columns
    columns = glue_df.columns
    print("Columns:", columns)
    
    # Add a column to count non-null values in each row
    glue_df_with_size = glue_df.withColumn(
        "non_null_count",
        F.size(F.array([F.when(F.trim(F.col(c)).isNotNull(), F.col(c)).otherwise(None) for c in columns]))
    )
    print("With Size:")
    glue_df_with_size.show(truncate=False)

    # Expected column count
    expected_column_count = len(columns)
    print("Expected Column Count:", expected_column_count)

    # Identify rows with column mismatch (where non-null column count != expected column count)
    mismatch_rows = glue_df_with_size.filter(
        F.col("non_null_count") != F.lit(expected_column_count)
    ).drop("non_null_count")
    
    # Display mismatch rows
    mismatch_rows.show(truncate=False)
    print("Mismatch Rows (first 5 records):")
    
    # Create the condition to check for null, empty, or placeholders like 'None'
    missing_conditions = reduce(
        lambda acc, c: acc | (F.col(c).isNull() | (F.col(c) == '') | (F.col(c) == 'None')),
        columns,
        F.lit(False)
    )

    # Identify rows with missing values (null, empty strings, or 'None')
    missing_value_rows = glue_df_with_size.filter(missing_conditions).drop("non_null_count")
    print("Missing Value Rows (first 5 records):")
    missing_value_rows.show(truncate=False)

    # Filter out mismatch_rows and missing_value_rows to create clean_df
    mismatch_and_missing_ids = mismatch_rows.union(missing_value_rows).select(F.monotonically_increasing_id().alias("row_id"))
    print("1028_mismatch_and_missing_ids:",mismatch_and_missing_ids)
    
    clean_df = glue_df.withColumn("row_id", F.monotonically_increasing_id()) \
                      .join(mismatch_and_missing_ids, on="row_id", how="left_anti") \
                      .drop("row_id")
    
    print("Clean DataFrame (first 5 records):",clean_df)
    clean_df.show(truncate=False)
    
    # Return the results
    return mismatch_rows, missing_value_rows, clean_df'''

'''def identify_issues(glue_df):
    try:
        # Get columns
        columns = glue_df.columns
        print("Columns:", columns)
        
        # Add a column to count non-null values in each row
        glue_df_with_size = glue_df.withColumn(
            "non_null_count",
            F.size(F.array([F.when(F.trim(F.col(c)).isNotNull(), F.col(c)).otherwise(None) for c in columns]))
        )
        print("With Size:")
        glue_df_with_size.show(truncate=False)

        # Expected column count
        expected_column_count = len(columns)
        print("Expected Column Count:", expected_column_count)

        # Identify rows with column mismatch (where non-null column count != expected column count)
        mismatch_rows = glue_df_with_size.filter(
            F.col("non_null_count") != F.lit(expected_column_count)
        ).drop("non_null_count")
        
        # Display mismatch rows
        mismatch_rows.show(truncate=False)
        mismatch_row_count = mismatch_rows.count()
        print(f"Mismatch rows count: {mismatch_row_count}")

        # Create the condition to check for null, empty, or placeholders like 'None'
        missing_conditions = F.lit(False)
        for c in columns:
            missing_conditions |= (F.col(c).isNull() | (F.col(c) == ' ') | (F.col(c) == 'None'))

        # Identify rows with missing values (null, empty strings, or 'None')
        missing_value_rows = glue_df_with_size.filter(missing_conditions).drop("non_null_count")
        print("Missing Value Rows (first 5 records):")
        missing_value_rows.show(truncate=False)
        missing_value_row_count = missing_value_rows.count()
        print(f"Missing value rows count: {missing_value_row_count}")

        # Check if mismatch_rows or missing_value_rows are empty
        if mismatch_row_count == 0 and missing_value_row_count == 0:
            print("No rows with mismatch or missing values found.")
            return mismatch_rows, missing_value_rows, glue_df

        # Proceed with filtering if mismatch_rows or missing_value_rows are not empty
        if mismatch_row_count > 0:
            mismatch_and_missing_ids = mismatch_rows.rdd.zipWithIndex().toDF().selectExpr("_1.*", "_2 as row_id")
        else:
            mismatch_and_missing_ids = None
        
        if missing_value_row_count > 0:
            missing_value_rows_with_ids = missing_value_rows.rdd.zipWithIndex().toDF().selectExpr("_1.*", "_2 as row_id")
        else:
            missing_value_rows_with_ids = None

        # Combine the mismatch and missing value rows with row_id if they are not empty
        if mismatch_and_missing_ids is not None and missing_value_rows_with_ids is not None:
            mismatch_and_missing_ids_combined = mismatch_and_missing_ids.union(missing_value_rows_with_ids).select("row_id")
        elif mismatch_and_missing_ids is not None:
            mismatch_and_missing_ids_combined = mismatch_and_missing_ids.select("row_id")
        elif missing_value_rows_with_ids is not None:
            mismatch_and_missing_ids_combined = missing_value_rows_with_ids.select("row_id")
        else:
            mismatch_and_missing_ids_combined = None

        # If mismatch_and_missing_ids_combined is not None, filter out rows
        if mismatch_and_missing_ids_combined is not None:
            clean_df = glue_df.rdd.zipWithIndex().toDF().selectExpr("_1.*", "_2 as row_id") \
                            .join(mismatch_and_missing_ids_combined, on="row_id", how="left_anti") \
                            .drop("row_id")
            print("Clean DataFrame (first 5 records):", clean_df)
            clean_df.show(truncate=False)
            print(f"Clean DataFrame count: {clean_df.count()}")
        else:
            clean_df = glue_df
            print("No rows to filter. Returning original DataFrame.")

        # Return the results
        return mismatch_rows, missing_value_rows, clean_df
    except Exception as e:
        print(f"Error: {str(e)}")
        return None, None, None'''

def identify_issues(glue_df):
    try:
        # Get columns
        columns = glue_df.columns
        print("Columns:", columns)
        
        # Add a column to count non-null values in each row
        glue_df_with_size = glue_df.withColumn(
            "non_null_count",
            F.size(F.array([F.when(F.trim(F.col(c)).isNotNull(), F.col(c)).otherwise(None) for c in columns]))
        )
        print("With Size:")
        glue_df_with_size.show(truncate=False)

        # Expected column count
        expected_column_count = len(columns)
        print("Expected Column Count:", expected_column_count)

        # Identify rows with column mismatch (where non-null column count != expected column count)
        mismatch_rows = glue_df_with_size.filter(
            F.col("non_null_count") != F.lit(expected_column_count)
        ).drop("non_null_count")
        
        # Display mismatch rows
        mismatch_rows.show(truncate=False)
        mismatch_row_count = mismatch_rows.count()
        print(f"Mismatch rows count: {mismatch_row_count}")

        # Create the condition to check for null, empty, or placeholders like 'None'
        missing_conditions = F.lit(False)
        for c in columns:
            missing_conditions |= (F.col(c).isNull() | (F.col(c) == ' ') | (F.col(c) == 'None'))

        # Identify rows with missing values (null, empty strings, or 'None')
        missing_value_rows = glue_df_with_size.filter(missing_conditions).drop("non_null_count")
        print("Missing Value Rows (first 5 records):")
        missing_value_rows.show(truncate=False)
        missing_value_row_count = missing_value_rows.count()
        print(f"Missing value rows count: {missing_value_row_count}")

        # Check if mismatch_rows or missing_value_rows are empty
        if mismatch_row_count == 0 and missing_value_row_count == 0:
            print("No rows with mismatch or missing values found.")
            return mismatch_rows, missing_value_rows, glue_df

        # Proceed with filtering if mismatch_rows or missing_value_rows are not empty
        if mismatch_row_count > 0:
            mismatch_and_missing_ids = mismatch_rows.rdd.zipWithIndex().toDF().selectExpr("_1.*", "_2 as row_id")
        else:
            mismatch_and_missing_ids = None
        
        if missing_value_row_count > 0:
            missing_value_rows_with_ids = missing_value_rows.rdd.zipWithIndex().toDF().selectExpr("_1.*", "_2 as row_id")
        else:
            missing_value_rows_with_ids = None

        # Combine the mismatch and missing value rows with row_id if they are not empty
        if mismatch_and_missing_ids is not None and missing_value_rows_with_ids is not None:
            mismatch_and_missing_ids_combined = mismatch_and_missing_ids.union(missing_value_rows_with_ids).select("row_id")
        elif mismatch_and_missing_ids is not None:
            mismatch_and_missing_ids_combined = mismatch_and_missing_ids.select("row_id")
        elif missing_value_rows_with_ids is not None:
            mismatch_and_missing_ids_combined = missing_value_rows_with_ids.select("row_id")
        else:
            mismatch_and_missing_ids_combined = None

        # If mismatch_and_missing_ids_combined is not None, filter out rows using broadcast
        if mismatch_and_missing_ids_combined is not None:
            mismatch_and_missing_ids_combined = mismatch_and_missing_ids_combined.selectExpr("row_id")
            glue_df_with_id = glue_df.rdd.zipWithIndex().toDF().selectExpr("_1.*", "_2 as row_id")
            clean_df = glue_df_with_id.join(
                F.broadcast(mismatch_and_missing_ids_combined), on="row_id", how="left_anti"
            ).drop("row_id")
            print("Clean DataFrame (first 5 records):")
            clean_df.show(truncate=False)
            print(f"Clean DataFrame count: {clean_df.count()}")
        else:
            clean_df = glue_df
            print("No rows to filter. Returning original DataFrame.")

        # Return the results
        return mismatch_rows, missing_value_rows, clean_df
    except Exception as e:
        print(f"Error: {str(e)}")
        return None, None, None


def process_struct_column(input_df):
    """
    Processes the input DataFrame to flatten the first struct column it finds.
    
    Parameters:
    - input_df: The input Spark DataFrame.
    
    Returns:
    - input_df: The transformed DataFrame with a combined schema.
    """
    input_df.printSchema()
    
    # Initialize struct_column
    struct_column = None

    # Iterating over the fields of the DataFrame schema
    for field in input_df.schema.fields:
        # Checking if the field is of struct type
        if str(field.dataType).startswith("StructType"):
            struct_column = field.name
            break

    # Check if struct_column is found
    if struct_column:
        print("struct column:", struct_column)
        dictionary_fields = [field.name for field in input_df.schema[str(struct_column)].dataType.fields]
        output_fields = [col(struct_column + "." + field).alias(struct_column + "." + field) for field in dictionary_fields]
        output_df = input_df.select(output_fields)
        output_df.printSchema()
        input_df = input_df.drop(struct_column)
        input_df.printSchema()
        schema_input_df = input_df.schema
        schema_output_df = output_df.schema
        combined_schema = StructType(schema_input_df.fields + schema_output_df.fields)
        input_df = spark.createDataFrame([], schema=combined_schema)
        input_df.printSchema()
        schema_dict = input_df.schema.jsonValue()
        print(schema_dict)
        input_df = input_df.toDF(*[c.upper() for c in input_df.columns])
        print("F:",input_df)
    return input_df

def flatten_and_clean(df):
   return [col.upper().strip() for col in df]
   
def normalize_column_names(flattened_df):
    return [col.upper().strip() for col in flattened_df]

def normalize_colibra_col_names(expected_columns):
    return [col.upper().strip() for col in expected_columns]
   
def clean_non_essential_columns(df_cleaned):
    # Identify all columns with StringType
    string_cols = [col_name for col_name, dtype in df_cleaned.dtypes if dtype == 'string']
    
    # Define the extended pattern to match non-printable characters and the special unicode characters
    non_printable_pattern = r'[^\w\s\x00-\x1F\x7F\u200B\u200C\u200D\u200E\u200F\u2028\u2029\u202F\u205F\u2060\uFEFF]'
    
    # Loop through all string columns and clean them
    for col_name in string_cols:
        df_cleaned = df_cleaned.withColumn(col_name, 
                                          regexp_replace(trim(col(col_name)), non_printable_pattern, ''))
        logger.info(f"Trimmed and cleaned special characters from column: {col_name}")
    
    return df_cleaned
    
def column_count_missing_col(expected_columns, actual_columns, variable, src_path):
    """Check if column count matches and store errors in S3 if not."""
    expected_len = len(expected_columns)
    actual_len = len(actual_columns)
    
    # Normalize column names to avoid mismatches due to whitespace or case sensitivity
    expected_columns = [col.strip().lower() for col in expected_columns]
    actual_columns = [col.strip().lower() for col in actual_columns]
    
    # Identify missing and extra columns
    missing_columns = [col for col in expected_columns if col not in actual_columns]
    extra_columns = [col for col in actual_columns if col not in expected_columns]
    
    print("Missing Columns:", missing_columns)
    print("Extra Columns:", extra_columns)
    
    try:
        if expected_len != actual_len or missing_columns or extra_columns:
            # Raise an exception with the details
            error_message = (
                f"Column count mismatch! Expected {expected_len}, but got {actual_len}. "
                f"Missing columns: {missing_columns}. Extra columns: {extra_columns}."
            )
            logger.info(error_message)
            raise Exception(error_message)
        
        logger.info(f"Column count validation passed. Both datasets have {expected_len} columns.")
        return expected_len, actual_len, missing_columns, extra_columns  # Return lengths and column details to main
    
    except Exception as e:
        # Prepare the error message in JSON format
        error_details = {
            "validate_column_count_mis_col": {
                "error_message": str(e),
                "expected_column_count": expected_len,
                "actual_column_count": actual_len,
                "missing_columns": missing_columns,
                "extra_columns": extra_columns,
                "file_path": src_path,
                "file_name": variable
            }
        }
        
        # Convert error details to JSON string
        error_details_json = json.dumps(error_details, indent=4)  # Pretty-print the JSON
        
        # Store the error details in the specified S3 bucket
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        s3_key = f"{pipeline}/schema_reject/validate_column_count/{timestamp}/column_count_error.json"
        s3_client.put_object(
            Bucket=reject_bucket_name,
            Key=s3_key,
            Body=error_details_json,
            ContentType='application/json'
        )
        
        print(f"Error details stored in S3 at: s3://{reject_bucket_name}/{s3_key}")
        logger.error(f"Column count mismatch details have been stored in S3 at: s3://{reject_bucket_name}/{s3_key}")
        
        # Send an email notification with the error details (sending the JSON)
        subject = f"Column Count Mismatch Detected for Pipeline: {pipeline}"
        body = (
            f"An error occurred during column validation:\n\n{error_details_json}"
        )
        send_email(subject, body)
        
        # Raise the exception to fail the Glue job
        raise e

def validate_schema(df, expected_schema, variable, src_path):
    """
    Validates schema including column names, data types, and order.
    Logs warnings if columns or data types do not match and stores all mismatches in S3 at the end.
    Returns missing columns and mismatched data types.
    """
    missing_columns = []
    mismatched_data_types = []

    actual_columns = [col_name.upper() for col_name, col_type in df.dtypes]
    expected_columns = expected_schema['columns']
    expected_types = expected_schema['types']

    logger.info(f"Validating schema for table with expected columns: {expected_columns}")
    logger.info(f"Actual columns in the dataset: {actual_columns}")

    try:
        # Column-level validation
        if len(expected_columns) != len(actual_columns):
            logger.warning(f"Schema validation warning: Expected {len(expected_columns)} columns, found {len(actual_columns)}")

        # Column names and order validation
        for idx, (exp_col, act_col) in enumerate(zip(expected_columns, actual_columns)):
            if exp_col != act_col:
                logger.warning(f"Schema validation warning: Expected column '{exp_col}' at position {idx}, found '{act_col}'")
            else:
                logger.info(f"Column '{exp_col}' at position {idx} matches expected schema.")

        # Data type validation
        for col_name, expected_type in zip(expected_columns, expected_types):
            try:
                if col_name in actual_columns:
                    # Handle nested columns
                    if '.' in col_name:
                        actual_type = df.selectExpr(f"`{col_name}`").dtypes[0][1]
                    else:
                        actual_type = df.select(col_name).dtypes[0][1]

                    logger.info(f"Column '{col_name}' has type: {actual_type}")
                    #mapped_actual_type = map_spark_type_to_collibra_type(actual_type)

                    if mapped_actual_type not in expected_type:
                        mismatched_data_types.append({
                            "column_name": col_name,
                            "expected_type": expected_type,
                            "found_type": mapped_actual_type
                        })
                        logger.warning(f"Field validation warning for column {col_name}: Expected type {expected_type}, Found type {mapped_actual_type}")

                else:
                    missing_columns.append(col_name)
                    logger.warning(f"Column '{col_name}' from Collibra not found in the dataset. Skipping type validation for this column.")

            except Exception as e:
                logger.warning(f"Error validating column '{col_name}': {e}. Skipping this column.")

        # If there are mismatched data types, store the details in S3 and send an email
        if mismatched_data_types:
            # Store error details in S3
            s3 = boto3.client('s3')
            error_details = {
                "schema_col_datatype_val": {
                    "error_message": "Field validation errors detected for data types.",
                    "mismatched_data_types": mismatched_data_types,
                    "path": src_path,
                    "file_name": variable
                }
            }
            error_details_json = json.dumps(error_details)
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            s3_key = f"{pipeline}/schema_reject/validate_col_datatype/{timestamp}/col_datatype_val.json"
            s3_client.put_object(
                Bucket=reject_bucket_name,
                Key=s3_key,
                Body=error_details_json,
                ContentType='application/json'
            )
            logger.error(f"Error details stored in S3 at: s3://{reject_bucket_name}/{s3_key}")

            # Send email with error details
            subject = f"Field Validation Errors Detected for Pipeline: {pipeline}"
            body = f"Field validation errors have been detected in the schema validation process. Details:\n\n{error_details_json}"
            send_email(subject, body)

            raise Exception("Field validation errors detected. See S3 for details.")

    except Exception as e:
        logger.error(f"Error during schema validation: {e}")
        # Store the general error details in S3
        error_details = {
            "error_message": f"Error during schema validation: {str(e)}"
        }
        
        raise  # Re-raise the exception to stop the process

    # Return the results: missing columns and mismatched data types
    return missing_columns, mismatched_data_types
    
def compare_schemas(collibra_schema, dataframe_schema, variable, src_path):
    """
    Compares the nullability of columns between the Collibra schema and DataFrame schema.
    If a mismatch is found, it logs the error and stores the details in S3, and sends an email.
    """
    error_columns = []
    
    # Create a map from column name to nullability for Collibra schema (normalized to lower case)
    collibra_map = {col[0].lower(): col[2] for col in collibra_schema}
    logger.info(f"Collibra map: {collibra_map}")
    
    # Go through the DataFrame schema and compare nullability with the Collibra schema (normalize to lower case)
    for field in dataframe_schema.fields:
        column_name = field.name.lower()  # Normalize to lower case for comparison
        logger.info(f"Column name: {column_name}")
        
        if column_name in collibra_map:
            json_nullable = field.nullable
            collibra_nullable = collibra_map[column_name]
            
            # Check if the nullability matches
            if collibra_nullable != json_nullable:
                error_columns.append(column_name)
    
    # If there are any errors, create the error message and store it in S3
    if error_columns:
        error_details = {
            "null_valuesval_col": {
                "error_message": f"Column nullability mismatch! Null columns: {error_columns}",
                "expected_column_nullable": False,
                "actual_column_nullable": True,
                "Null_columns": error_columns,
                "path": src_path,
                "variable": variable
            }
        }
        
        # Convert error details to JSON string
        error_details_json = json.dumps(error_details, indent=4)
        
        # Store the error details in the specified S3 bucket
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        s3_key = f"{pipeline}/schema_reject/nullable_column_count/{timestamp}/nullable_count_error.json"
        s3_client.put_object(
            Bucket=reject_bucket_name,
            Key=s3_key,
            Body=error_details_json,
            ContentType='application/json'
        )
        
        logger.error(f"Error details stored in S3 at: s3://{reject_bucket_name}/{s3_key}")
        
        # Send email notification with the error details
        subject = f"Column Nullability Mismatch Detected for Pipeline: {pipeline}"
        body = f"A nullability mismatch has been detected between the Collibra schema and the DataFrame schema. Details:\n\n{error_details_json}"
        send_email(subject, body)
        
        # Raise the exception to fail the Glue job
        raise Exception(f"Column nullability mismatch detected. Details stored in S3 at: s3://{reject_bucket_name}/{s3_key}")
    
    # If no errors, return success message
    logger.info("Schemas are consistent.")
    return {"message": "Schemas are consistent"}

def build_uri_from_bucket_and_key(bucket, key):
    return "s3://" + os.path.join(bucket, key)
    
def main():
    global target_prefix
    global file_names
    s3 = boto3.client('s3')
    # Extracting bucket and prefix from args
    source_bucket_raw = args['source_bucket_raw']  # Ensure this is set correctly in the args
    source_prefix = args['source_prefix']  # Base source prefix
    source_prefix = source_prefix + '/' + file_datetime
    print("1331_source_prefix_and_date:",source_prefix)
    #file_datetime = args['file_datetime']  # Date-time parameter
    
    #source_prefix = f"{source_prefix.rstrip('/')}/{file_datetime.lstrip('/')}"
    #print("1331_source_prefix:", source_prefix)
    
    # Construct the S3 path
    src_path = f"s3://{source_bucket_raw}{source_prefix}"
    print("744_source_path:", src_path)
    
    # Fetch the object list from S3
    try:
        response = s3.list_objects_v2(Bucket=source_bucket_raw, Prefix=source_prefix)
        print("745_response:", response)
        
        # Extract file names (keys without the directory part)
        file_names = [obj['Key'].split('/')[-1] for obj in response.get('Contents', []) if not obj['Key'].endswith('/')]
        print("745_file_names:", file_names)
        
        #if 'Contents' in response:
        #    for obj in response['Contents']:
        #        print(f"Found file: {obj['Key']}")
        #else:
        #    print(f"No files found in {source_prefix}")
        # Store file names in a variable
        variable = file_names
        print("749_variable:", variable)
    
    except ClientError as e:
        print(f"Error accessing bucket {source_bucket_raw} with prefix {dynamic_prefix}: {e}")
        variable = []  # Set variable to empty list in case of error
    
    # Track failed files
    failed_files = []
    
    # Process each file independently
    for file_name in variable:
        try:
            # Read the dataset based on file type (CSV/JSON)
            file_path = f"s3://{source_bucket_raw}/{source_prefix}"
            df = read_data(file_path, file_name)
            print("143_df:", df)
            if df.count() == 0:
                raise Exception(f"Empty dataset found in {source_path}")
            flattened = process_struct_column(df)
            print("301_flattened_df:",flattened)
            flattened.printSchema()
            
            df_cleaned = flatten_and_clean(flattened.columns)
            print("df_cleaned:",df_cleaned)
            
            df_cleaned_spl = clean_non_essential_columns(flattened)
            print("df_cleaned_spl",df_cleaned_spl)
            
            # Step 3: Remove duplicates based on all columns (or specify a subset of columns)
            flattened_df = df_cleaned_spl.dropDuplicates()
            print("flattened_df:",flattened_df)
            
            # Extract schema information from S3 dataset
            s3_schema = []
            for field in flattened_df.schema.fields:
                col_name = field.name.upper()
                print("col_name:",col_name)
                data_type = field.dataType.simpleString()
                nullable = field.nullable
                s3_schema.append((col_name, data_type, nullable))
            column_names_data_type = s3_schema
            print("column_names_data_type:",column_names_data_type)
            
            normalized_actual_columns = normalize_column_names(flattened_df.columns)
            print("normalized_actual_columns:",normalized_actual_columns)
            
            # Get table ID from Collibra
            table_id = get_table_id_from_collibra(raw_table_name, domain_id_raw, COMMUNITY_ID)
            print("table_id:",table_id)
            
            # Get existing columns from Collibra
            '''existing_columns_and_data_types = get_existing_columns_from_collibra(table_id, relation_type_id)
            print("collibra_existing_columns_datatypes:",existing_columns_and_data_types)'''
            #existing_columns_and_data_types = [('LOAN_NUM', 'string', True),('REPORT_DATE', 'string', True),('ARCHIVED_DATE', 'string', True),('ARCHIVED', 'string', True), ('PRIMARY_STATUS', 'string', True), ('PRINCIPAL_BALANCE', 'string', True), ('CHARGEOFF_BALANCE', 'string', True), ('DIVISION', 'string', True)]
            existing_columns_and_data_types = [('LOAN_NUM', 'string', True), ('REPORT_DATE', 'string', True),
                                               ('ARCHIVED_DATE', 'string', True), ('ARCHIVED', 'string', True), 
                                               ('PRIMARY_STATUS', 'string', True), ('PRINCIPAL_BALANCE', 'string', True), 
                                               ('CHARGEOFF_BALANCE', 'string', True), ('DIVISION', 'string', True), 
                                               ('ENTITY', 'string', True), ('PROGRAM', 'string', True), 
                                               ('POOL', 'string', True), ('REPOSESSION_STATUS', 'string', True), 
                                               ('REMARKETING_STATUS', 'string', True), ('BANKRUPTCY_STATUS', 'string', True), 
                                               ('TITLE_STATUS', 'string', True), ('DUE_DATE', 'string', True), 
                                               ('DAYS_LATE', 'string', True), ('REMAINING_PAYMENTS', 'string', True), 
                                               ('MATURITY_DATE', 'string', True), ('NUMBER_OF_PAYMENTS', 'string', True), 
                                               ('NUMBER_OF_EXTENSIONS', 'string', True), ('CURRENT_APR', 'string', True), 
                                               ('CURRENT_PAYMENT', 'string', True), ('30', 'string', True), 
                                               ('60', 'string', True), ('90+', 'string', True), 
                                               ('LOSS_DATE', 'string', True), ('CONTRACT_DATE', 'string', True), 
                                               ('FUNDING_DATE', 'string', True), ('FUNDING_MONTH', 'string', True), 
                                               ('ORIGINATOR_SOURCE', 'string', True), ('RISK_LEVEL', 'string', True), 
                                               ('UNDERWRITING_TYPE_NAME', 'string', True), ('IS_OPEN_LENDING', 'string', True), 
                                               ('ORIGINAL_PRINCIPAL_BALANCE', 'string', True), ('ORIGINAL_DOC_FEE_DATE', 'string', True), 
                                               ('EOM_DOC_FEE_DATE', 'string', True), ('DOC_FEE', 'string', True), 
                                               ('TERM_OF_LOAN', 'string', True), ('ORIGINAL_APR', 'string', True), 
                                               ('ORIGINAL_PAYMENT', 'string', True), ('PAYMENT', 'string', True), 
                                               ('ORIGINAL_CONTRACT_MATURITY', 'string', True), ('MERCHANT_STATE_KEY', 'string', True), 
                                               ('VEHICLE_TYPE', 'string', True), ('GARAGE_STATE', 'string', True), 
                                               ('PRI_ZIP', 'string', True), ('DECISION_CREDIT_SCORE', 'string', True), 
                                               ('TOTAL_MONTHS_EXTENDED', 'string', True), ('DEALER_TYPE', 'string', True), 
                                               ('LTV', 'string', True), ('FRONT_END_LTV', 'string', True), 
                                               ('DTI', 'string', True), ('PTI', 'string', True), ('CASH_DOWN', 'string', True), 
                                               ('AVERAGE_FICO', 'string', True), ('EMPLOYMENT_YEARS', 'string', True), 
                                               ('RESIDENCE_YEARS', 'string', True), ('BORROWER_COUNT', 'string', True), 
                                               ('TRADE_ALLOWANCE', 'string', True), ('TRADE_ALLOWANCE_PERCENT', 'string', True), 
                                               ('CASH_DOWN_CONTRACT_PERCENT', 'string', True), ('TOTAL_DOWN_CONTRACT_PERCENT', 'string', True), 
                                               ('DISCOUNT_PCT', 'string', True), ('TOTAL_INCOME', 'string', True), 
                                               ('LAST_PAID_DATE', 'string', True), ('FIRST_PAYMENT_DATE', 'string', True), 
                                               ('RECOVERED_AMOUNT', 'string', True), ('SEASONING', 'string', True), 
                                               ('REPO_STATUS_CREATED_DATE', 'string', True), ('TITLE_STATUS_CREATED_DATE', 'string', True), 
                                               ('NEW_USED', 'string', True), ('VEHICLE_MAKE', 'string', True), 
                                               ('VEHICLE_MODEL', 'string', True), ('VEHICLE_YEAR', 'string', True), 
                                               ('VEHICLE_VALUE_AT_ORIGINATION', 'string', True), ('VEHICLE_MILEAGE', 'string', True), 
                                               ('VEHICLE_VIN', 'string', True), ('DEAERLSHIP_NAME', 'string', True), 
                                               ('DEALERSHIP_STATE', 'string', True), ('DEALERSHIP_GROUP', 'string', True), 
                                               ('DEALERSHIP_CATEGORY', 'string', True), ('DEALERSHIP_CHANNEL', 'string', True), 
                                               ('COLLATERAL_TYPE', 'string', True),('BOR_SIGNING_COMPLETED_BY', 'string', True)]
            
            expected_columns = [col[0] for col in existing_columns_and_data_types]
            print("expected_columns:", expected_columns)
            expected_data_types = [col[1] for col in existing_columns_and_data_types]
            nullable_columns = [col[2] for col in existing_columns_and_data_types]
            
            normalized_expected_columns = normalize_colibra_col_names(expected_columns)
            print("normalized_expected_columns:",normalized_expected_columns)
            
            # column count
            validate_column_count_mis_col=column_count_missing_col(normalized_expected_columns, normalized_actual_columns,variable, src_path)
            print("validate_column_count_mis_col:",validate_column_count_mis_col)
            
            # Get comapre logic for column names, datatype
            expected_schema = {'columns': expected_columns, 'types': expected_data_types}
            schema_col_datatype_val = validate_schema(flattened_df, expected_schema,variable,src_path)
            print("schema_col_datatype_val:",schema_col_datatype_val)
            
            null_valuesval_col = compare_schemas(existing_columns_and_data_types,flattened_df.schema, variable,src_path)
            print("null_valuesval_col:",null_valuesval_col)
            
            dataframe_columns = df.columns
            print("dataframe_columns", dataframe_columns)
            
            # Validate if the dataset has no data
            check_data_df = check_no_data(df)
            print("151_check_data_df:", check_data_df)
            
            # Validate column count
            #reject_base_path_soft = "communication_consent_history_phone/Soft_reject"
            #hard_base_path = "communication_consent_history_phone/Hard_reject"
            #reject_bucket_name = "oedsd-dev-s3-dl-reject-con-ue1-all"
            #col_count = validate_column_count(df, expected_columns, reject_base_path, reject_bucket_name)
            #print("147_col_count:", col_count)
    
            # Convert to pandas if the dataframe is a PySpark DataFrame
            #if isinstance(df, pyspark.sql.DataFrame):
            df_panda = df.toPandas()
            print(f"Type of df after conversion: {type(df)}")
    
            # Hard Reject condition: If any column has all missing values across all rows
            #col_data_check = check_hard_reject_and_store(df_panda, reject_bucket_name, reject_base_path)
            #print("col_data_check:", col_data_check)
            ####################################
            #df = df.toPandas()
            
            #handle_missing_columns = handle_missing_columns_and_store(df,reject_bucket_name, reject_base_path)
            #print("handle_missing_columns:",handle_missing_columns)
            
            #handle_null_columns = handle_null_values_and_store(df,reject_bucket_name, reject_base_path)
            #print("handle_null_columns:",handle_null_columns)
            
            #missing_records = find_missing_records(df)
            #print("missing_records:",missing_records)
            validate_col_order = validate_column_order(expected_columns, dataframe_columns,reject_bucket_name,hard_base_path,variable,src_path)
            print("validate_col_order:",validate_col_order)
            #df = df.toPandas()
            #id_null_mismatch = identify_null_mismatch(df,reject_bucket_name, reject_base_path_soft)
            #print("id_null_mismatch:",id_null_mismatch)
            #df = df.toPandas()
            #id_missing_values=identify_missing_values(df,id_null_mismatch,reject_bucket_name, reject_base_path_soft)
            #print("id_missing_values:",id_missing_values)
            #####################################################
            #column_mismatch_rows = handle_missing_values_and_store_in_s3(df,reject_bucket_name, reject_base_path)
            #print("column_mismatch_rows:",column_mismatch_rows)
            #null_miss_rows=filter_rows_with_null_or_missing_values(df,reject_bucket_name, reject_base_path)
            #print("null_miss_rows:",null_miss_rows)

            # Validate dataset (you can add more validation checks as needed)
            mismatch_rows, missing_value_rows,clean_df = identify_issues(df)
            print("mismatch_rows:", mismatch_rows)
            print("missing_value_rows:", missing_value_rows)
            
            # Write combined results to Parquet in S3
            write_combined_parquet(df, mismatch_rows, missing_value_rows, reject_bucket_name, reject_base_path_soft, file_name, src_path, pipeline)
            
            # Calculate the total number of rejected records
            total_records = df.count()
            threshold_percentage = 0.20
            threshold_count = total_records * threshold_percentage
            print("1044_threshold_count:", threshold_count)
    
            rejected_count = mismatch_rows.union(missing_value_rows).count()
            print("1048_rejected_count:", rejected_count)
    
            if rejected_count > threshold_count:
                print(f"Warning: Number of rejected records ({rejected_count}) exceeds threshold ({threshold_count}) for file {file_name}.")
                raise Exception(f"File {file_name} exceeded threshold with {rejected_count} rejected records.")
            else:
                # Save the clean DataFrame to S3 in Parquet format
                output_location = build_uri_from_bucket_and_key(target_bucket, target_prefix).replace("s3://", "s3a://") + "/"
                print("1555_output_location:", output_location)
                
                clean_df.coalesce(1).write.format("parquet") \
                    .option("header", "true") \
                    .option("quote", "\"") \
                    .option("escape", "\"") \
                    .mode("overwrite") \
                    .save(output_location)
            
                print(f"Clean data successfully written to the target S3 location: {output_location}")
                return True, f"File {file_name} processed successfully. Clean data saved to {output_location}."

        except Exception as e:
            print(f"Error processing file {file_name}: {str(e)}")
            failed_files.append(file_name)  # Mark file as failed
    
    # After all files are processed, check if any file failed
    if failed_files:
        print(f"Job failed. The following files failed validation: {failed_files}")
        sys.exit(1)  # Exit the program with an error code
    else:
        print("Job completed successfully.")

# Entry point to call main() function when the script is executed
if __name__ == "__main__":
    result = main()
    print(result)  # This will print the result (success or error message)
