import psycopg2
import boto3
import os
import json


s3 = boto3.client('s3', aws_access_key_id=os.getenv('aws_id'), aws_secret_access_key=os.getenv('aws_key'))

'''
def list_csv_files(bucket, prefix):
    csv_files = []
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    for page in page_iterator:
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.csv'):
                csv_files.append(obj['Key'])
    return csv_files
'''
    
def lambda_handler(event, context):
    aws_id = os.getenv('aws_id')
    aws_key = os.getenv('aws_key')
    dbname = os.getenv('dbname')
    host = os.getenv('endpoint')  
    user = os.getenv('user')
    password = os.getenv('password')
    port = os.getenv('port')
    bucket_name = os.getenv('bucket_name')
    prefix = os.getenv('prefix')
    region = os.getenv('region')
    iam_role = os.getenv('iam_role')
 

    connection = psycopg2.connect(
        dbname=os.getenv('dbname'), 
        host=os.getenv('endpoint'), 
        port=os.getenv('port'), 
        user=os.getenv('user'), 
        password=os.getenv('password')
    )
    cursor = connection.cursor()

    table_mapping = {
        'account.csv': 'account',
        'brand.csv': 'brand',
        'post_attributes.csv': 'post',
        'partnership_account.csv': 'partnership_account',
        'master.csv': 'master'
    }
   

    for file_name, table_name in table_mapping.items():
        # Construct the full S3 path to the CSV file
        csv_file_path = f's3://{bucket_name}/{prefix}{file_name}'

        # Formulate the COPY command to load the data
        copy_query = f"""
            COPY {table_name}
            FROM '{csv_file_path}'
            IAM_ROLE '{iam_role}'
            DELIMITER ','
            IGNOREHEADER 1
            REGION '{region}';
        """

        try:
            # Execute the COPY command
            cursor.execute(copy_query)
            connection.commit()
            print(f"COPY command for table {table_name} completed successfully.")
        except Exception as e:
            connection.rollback()  # Rollback in case of error
            print(f"Error loading data into table {table_name}: {str(e)}")

    # Close the database connection
    cursor.close()
    connection.close()

    return {
        'statusCode': 200,
        'body': json.dumps('CSV files processed successfully.')
    }
